<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Exception\InvalidLocalActivityReport;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Exception\LocalActivityTimedOut;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\SagaCompensationFailed;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CapabilityManifest;
use DurableWorkflow\Worker\DiscoveredHandlers;
use DurableWorkflow\Worker\HandlerDiscovery;
use DurableWorkflow\Worker\HandlerDefinition;
use DurableWorkflow\Worker\HandlerResolver;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\StickyWorkflowCache;
use DurableWorkflow\Worker\WorkerSession;
use DurableWorkflow\Worker\WorkerSessionOptions;
use DurableWorkflow\Worker\WorkflowCommand;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;

/** Managed synchronous remote worker for workflow, activity, query, and update tasks. */
final class Worker
{
    private const DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;
    private const INITIAL_TRANSIENT_RETRY_DELAY_SECONDS = 0.1;
    private const MAX_HEARTBEAT_INTERVAL_SECONDS = 3600;
    private const MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES = 255;
    private const MAX_LOCAL_ACTIVITY_HEARTBEATS = 1000;
    private const MAX_LOCAL_ACTIVITY_HEARTBEATS_PER_ATTEMPT = 1000;
    private const MAX_TRANSIENT_RETRY_DELAY_SECONDS = 5.0;
    private const TRANSIENT_RETRY_SLEEP_SLICE_SECONDS = 0.1;

    /** @var array<string, HandlerDefinition> */
    private array $workflows = [];
    /** @var array<string, HandlerDefinition> */
    private array $activities = [];
    /** @var array<string, array<string, HandlerDefinition>> */
    private array $queries = [];
    /** @var array<string, array<string, callable(mixed ...$arguments): mixed>> */
    private array $signals = [];
    /** @var array<string, array<string, HandlerDefinition>> */
    private array $updates = [];
    private bool $shutdownRequested = false;
    private bool $registered = false;
    private float $lastHeartbeatAt = 0.0;
    private int $heartbeatIntervalSeconds;
    /** @var \Closure(): float */
    private readonly \Closure $clock;
    /** @var \Closure(int): void */
    private readonly \Closure $sleeper;
    private readonly string $workerId;
    private readonly Replayer $replayer;
    private readonly HandlerDiscovery $handlerDiscovery;
    private readonly LoggerInterface $logger;
    private readonly StickyWorkflowCache $stickyCache;
    /** @var array<string, WorkerSessionOptions> */
    private array $activeWorkerSessions = [];
    /** @var array<string, mixed>|null */
    private ?array $workflowMemoCapability = null;
    /** @var (\Closure(string, array<string, mixed>): void)|null */
    private readonly ?\Closure $diagnosticListener;

    public function __construct(
        private readonly Client $client,
        public readonly string $taskQueue,
        ?string $workerId = null,
        int $heartbeatIntervalSeconds = self::DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        private readonly ?string $buildId = null,
        ?\Closure $clock = null,
        ?\Closure $sleeper = null,
        /** @var (\Closure(string, int, float, ServerException): void)|null */
        private readonly ?\Closure $transientPollRetryObserver = null,
        ?ContainerInterface $container = null,
        ?LoggerInterface $logger = null,
        /** @var (callable(string, array<string, mixed>): void)|null $diagnosticListener */
        ?callable $diagnosticListener = null,
        int $stickyCacheCapacity = 100,
        int $stickyCacheTtlSeconds = 300,
    ) {
        $this->workerId = $workerId ?? 'php-worker-'.bin2hex(random_bytes(8));
        $this->heartbeatIntervalSeconds = $this->validHeartbeatInterval($heartbeatIntervalSeconds)
            ?? self::DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
        $this->clock = $clock ?? static fn (): float => microtime(true);
        $this->sleeper = $sleeper ?? static function (int $microseconds): void {
            usleep($microseconds);
        };
        $this->replayer = new Replayer($client->payloadCodec());
        $this->handlerDiscovery = new HandlerDiscovery(new HandlerResolver($container));
        $this->logger = $logger ?? new NullLogger();
        $this->diagnosticListener = $diagnosticListener === null
            ? null
            : \Closure::fromCallable($diagnosticListener);
        $this->stickyCache = new StickyWorkflowCache($stickyCacheCapacity, $stickyCacheTtlSeconds, $this->clock);
    }

    /**
     * Construct the preferred class-oriented worker surface.
     *
     * @param (callable(string, array<string, mixed>): void)|null $diagnosticListener
     */
    public static function create(
        Client $client,
        string $taskQueue,
        ?ContainerInterface $container = null,
        ?LoggerInterface $logger = null,
        ?callable $diagnosticListener = null,
    ): self {
        return new self(
            $client,
            $taskQueue,
            container: $container,
            logger: $logger,
            diagnosticListener: $diagnosticListener,
        );
    }

    /**
     * Discover and register one or more attribute-based handler services.
     *
     * @param class-string|object ...$services
     */
    public function register(string|object ...$services): self
    {
        $discovered = array_map($this->handlerDiscovery->discover(...), $services);
        $this->assertDiscoveriesCanRegister($discovered);

        foreach ($discovered as $handlers) {
            foreach ($handlers->workflows as $name => $handler) {
                $this->registerWorkflowDefinition($name, $handler);
            }
            foreach ($handlers->activities as $name => $handler) {
                $this->registerActivityDefinition($name, $handler);
            }
            foreach ($handlers->queries as $workflowType => $queries) {
                foreach ($queries as $name => $handler) {
                    $this->registerQueryDefinition($workflowType, $name, $handler);
                }
            }
            foreach ($handlers->signals as $workflowType => $signals) {
                foreach ($signals as $name => $handler) {
                    $this->declareSignal($workflowType, $name, $handler);
                }
            }
            foreach ($handlers->updates as $workflowType => $updates) {
                foreach ($updates as $name => $handler) {
                    $this->registerUpdateDefinition($workflowType, $name, $handler);
                }
            }
        }

        return $this;
    }

    /** @param callable(WorkflowContext, mixed ...$arguments): mixed $handler */
    public function registerWorkflow(string $workflowType, callable $handler): self
    {
        return $this->registerWorkflowDefinition($workflowType, HandlerDefinition::shared($handler));
    }

    private function registerWorkflowDefinition(string $workflowType, HandlerDefinition $handler): self
    {
        $this->assertValidDeclarationName($workflowType, 'workflow');
        $this->assertHandlerContext($handler->contract(), WorkflowContext::class, "workflow {$workflowType}");
        $this->assertUnique($this->workflows, $workflowType, 'workflow');
        $this->workflows[$workflowType] = $handler;

        return $this;
    }

    /** @param callable(ActivityContext, mixed ...$arguments): mixed $handler */
    public function registerActivity(string $activityType, callable $handler): self
    {
        return $this->registerActivityDefinition($activityType, HandlerDefinition::shared($handler));
    }

    private function registerActivityDefinition(string $activityType, HandlerDefinition $handler): self
    {
        $this->assertValidDeclarationName($activityType, 'activity');
        $this->assertHandlerContext($handler->contract(), ActivityContext::class, "activity {$activityType}");
        $this->assertUnique($this->activities, $activityType, 'activity');
        $this->activities[$activityType] = $handler;

        return $this;
    }

    /** @param callable(QueryContext, mixed ...$arguments): mixed $handler */
    public function registerQuery(string $workflowType, string $queryName, callable $handler): self
    {
        return $this->registerQueryDefinition($workflowType, $queryName, HandlerDefinition::shared($handler));
    }

    private function registerQueryDefinition(
        string $workflowType,
        string $queryName,
        HandlerDefinition $handler,
    ): self {
        $this->assertValidDeclarationName($workflowType, 'workflow');
        $this->assertValidDeclarationName($queryName, 'query');
        $this->assertHandlerContext($handler->contract(), QueryContext::class, "query {$workflowType}.{$queryName}");
        $this->queries[$workflowType] ??= [];
        $this->assertUnique($this->queries[$workflowType], $queryName, 'query');
        $this->queries[$workflowType][$queryName] = $handler;

        return $this;
    }

    /**
     * Declare a replay-consumed signal and its argument signature.
     *
     * The optional signature is reflected for registration metadata only and
     * is never invoked. Workflows continue to consume signals deterministically
     * through WorkflowContext::signals().
     *
     * @param callable(mixed ...$arguments): mixed|null $signature
     */
    public function declareSignal(string $workflowType, string $signalName, ?callable $signature = null): self
    {
        $this->assertValidDeclarationName($workflowType, 'workflow type', true);
        $this->assertValidDeclarationName($signalName, 'signal', true);
        $this->assertSignalNameIsNotRuntimeReserved($signalName);
        $this->signals[$workflowType] ??= [];
        $this->assertUnique($this->signals[$workflowType], $signalName, 'signal');
        $this->signals[$workflowType][$signalName] = $signature ?? static fn (): mixed => null;

        return $this;
    }

    /** @param callable(QueryContext, mixed ...$arguments): mixed $handler */
    public function registerUpdate(string $workflowType, string $updateName, callable $handler): self
    {
        return $this->registerUpdateDefinition($workflowType, $updateName, HandlerDefinition::shared($handler));
    }

    private function registerUpdateDefinition(
        string $workflowType,
        string $updateName,
        HandlerDefinition $handler,
    ): self {
        $this->assertValidDeclarationName($workflowType, 'workflow');
        $this->assertValidDeclarationName($updateName, 'update');
        $this->assertHandlerContext($handler->contract(), QueryContext::class, "update {$workflowType}.{$updateName}");
        $this->updates[$workflowType] ??= [];
        $this->assertUnique($this->updates[$workflowType], $updateName, 'update');
        $this->updates[$workflowType][$updateName] = $handler;

        return $this;
    }

    public function requestShutdown(): void
    {
        if ($this->shutdownRequested) {
            return;
        }
        $this->shutdownRequested = true;
        $this->diagnostic('worker.shutdown_requested', ['worker_id' => $this->workerId]);
    }

    /** Create an explicit typed worker-session lifecycle handle. */
    public function workerSession(WorkerSessionOptions $options): WorkerSession
    {
        $this->activeWorkerSessions[$options->sessionId] = $options;

        return new WorkerSession($this->client, $this->workerId, $options);
    }

    /** @return array{hit: int, miss: int, eviction: int, forced_cold_replay: int} */
    public function stickyCacheMetrics(): array
    {
        return $this->stickyCache->metrics();
    }

    public function run(int $pollTimeoutSeconds = 5): void
    {
        $this->validate();
        $this->diagnostic('worker.starting', [
            'worker_id' => $this->workerId,
            'task_queue' => $this->taskQueue,
            'contracts' => $this->contracts(),
        ]);
        $this->installSignalHandlers();
        $runFailure = null;
        try {
            $registration = $this->registerWithRetry();
            if ($registration === null) {
                return;
            }
            $this->applyHeartbeatInterval($registration);
            $this->registered = true;
            $this->lastHeartbeatAt = $this->now();
            $this->diagnostic('worker.registered', [
                'worker_id' => $this->workerId,
                'task_queue' => $this->taskQueue,
                'heartbeat_interval_seconds' => $this->heartbeatIntervalSeconds,
            ]);

            while (!$this->shutdownRequested) {
                $this->tick($pollTimeoutSeconds);
                $this->heartbeatIfDue();
            }
        } catch (Throwable $exception) {
            $runFailure = $exception;
            $this->diagnostic('worker.failed', [
                'worker_id' => $this->workerId,
                'task_queue' => $this->taskQueue,
                'exception' => $exception,
            ], 'error');
            throw $exception;
        } finally {
            $this->closeWorkerSessions();
            $this->stickyCache->clear();
            if ($this->registered) {
                try {
                    $this->client->deregisterWorkerRegistration($this->workerId);
                    $this->registered = false;
                    $this->diagnostic('worker.deregistered', ['worker_id' => $this->workerId]);
                } catch (Throwable $exception) {
                    $this->diagnostic('worker.shutdown_failed', [
                        'worker_id' => $this->workerId,
                        'exception' => $exception,
                    ], 'error');
                    if ($runFailure === null) {
                        throw $exception;
                    }
                }
            }
            $this->diagnostic('worker.stopped', [
                'worker_id' => $this->workerId,
                'task_queue' => $this->taskQueue,
            ]);
        }
    }

    /** Validate every registered command contract without contacting the server. */
    public function validate(): void
    {
        foreach ($this->workflows as $name => $handler) {
            $this->assertValidDeclarationName($name, 'workflow');
            $this->assertHandlerContext($handler->contract(), WorkflowContext::class, "workflow {$name}");
        }
        foreach ($this->activities as $name => $handler) {
            $this->assertValidDeclarationName($name, 'activity');
            $this->assertHandlerContext($handler->contract(), ActivityContext::class, "activity {$name}");
        }
        foreach ($this->queries as $workflowType => $handlers) {
            foreach ($handlers as $name => $handler) {
                $this->assertValidDeclarationName($name, 'query');
                $this->assertHandlerContext($handler->contract(), QueryContext::class, "query {$workflowType}.{$name}");
            }
        }
        foreach ($this->signals as $workflowType => $handlers) {
            foreach (array_keys($handlers) as $name) {
                $this->assertValidDeclarationName($name, 'signal');
            }
        }
        foreach ($this->updates as $workflowType => $handlers) {
            foreach ($handlers as $name => $handler) {
                $this->assertValidDeclarationName($name, 'update');
                $this->assertHandlerContext(
                    $handler->contract(),
                    QueryContext::class,
                    "update {$workflowType}.{$name}",
                );
            }
        }
    }

    /**
     * Return the complete local definition sent during worker registration.
     *
     * @return array{
     *     workflows: list<string>,
     *     activities: list<string>,
     *     workflow_commands: array<string, mixed>
     * }
     */
    public function contracts(): array
    {
        return [
            'workflows' => array_keys($this->workflows),
            'activities' => array_keys($this->activities),
            'workflow_commands' => $this->workflowCommandContracts(),
        ];
    }

    /**
     * Resolve a registered callable for the public worker test harness.
     *
     * @internal Applications should use register() or the explicit low-level registration methods.
     */
    public function registeredHandler(string $kind, string $name, ?string $workflowType = null): callable
    {
        $handler = match ($kind) {
            'workflow' => $this->workflows[$name] ?? null,
            'activity' => $this->activities[$name] ?? null,
            'query' => $workflowType === null ? null : ($this->queries[$workflowType][$name] ?? null),
            'signal' => $workflowType === null ? null : ($this->signals[$workflowType][$name] ?? null),
            'update' => $workflowType === null ? null : ($this->updates[$workflowType][$name] ?? null),
            default => null,
        };
        if ($handler === null) {
            $identity = $workflowType === null ? $name : "{$workflowType}.{$name}";
            throw new \InvalidArgumentException("No {$kind} handler is registered for {$identity}.");
        }

        return $handler;
    }

    /** @return array<string, mixed>|null */
    private function registerWithRetry(): ?array
    {
        $attempt = 0;
        while (!$this->shutdownRequested) {
            try {
                return $this->client->registerWorker(
                    $this->workerId,
                    $this->taskQueue,
                    array_keys($this->workflows),
                    array_keys($this->activities),
                    [
                        'query_tasks',
                        'workflow_updates',
                        'durable_history_replay',
                        'graceful_shutdown',
                        'message_streams',
                        'memo_upserts',
                        'typed_search_attributes',
                        'durable_selection',
                        'local_activities',
                        'worker_sessions',
                        'sticky_execution',
                    ],
                    buildId: $this->buildId,
                    workflowCommandContracts: $this->workflowCommandContracts(),
                    capabilityManifest: CapabilityManifest::portableWorkerAffinity(),
                );
            } catch (ServerException $exception) {
                if (!$this->isTransientRegistrationFailure($exception)) {
                    throw $exception;
                }

                ++$attempt;
                $delaySeconds = $this->transientRetryDelay(
                    $attempt,
                    $exception->details['retry_after_seconds'] ?? null,
                );
                if ($this->transientPollRetryObserver !== null) {
                    ($this->transientPollRetryObserver)('registration', $attempt, $delaySeconds, $exception);
                }
                $this->diagnostic('worker.retrying', [
                    'worker_id' => $this->workerId,
                    'operation' => 'registration',
                    'attempt' => $attempt,
                    'delay_seconds' => $delaySeconds,
                    'exception' => $exception,
                ], 'warning');
                $this->waitForTransientRetry($delaySeconds);
            }
        }

        return null;
    }

    private function isTransientRegistrationFailure(ServerException $exception): bool
    {
        if ($exception->status !== 503 || $exception->reason !== 'backend_lock_pressure') {
            return false;
        }

        $response = $exception->details;
        if ($response === null || array_is_list($response)) {
            return false;
        }

        $backend = $response['backend'] ?? null;

        return ($response['reason'] ?? null) === 'backend_lock_pressure'
            && ($response['operation'] ?? null) === 'register_worker'
            && ($response['worker_id'] ?? null) === $this->workerId
            && ($response['task_queue'] ?? null) === $this->taskQueue
            && ($response['registered'] ?? null) === false
            && ($response['retryable'] ?? null) === true
            && is_int($response['retry_after_seconds'] ?? null)
            && $response['retry_after_seconds'] > 0
            && is_array($backend)
            && ($backend['lock_pressure'] ?? null) === true;
    }

    /** Execute at most one task of each kind; useful for custom supervisors and tests. */
    public function tick(int $pollTimeoutSeconds = 1): bool
    {
        if ($this->shutdownRequested) {
            return false;
        }

        $handled = false;
        $workflowPoll = $this->pollWithRetry(
            'workflow',
            fn (): array => $this->client->pollWorkflowTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $this->preparePoll($pollTimeoutSeconds),
            ),
        );
        if ($workflowPoll === null) {
            return false;
        }
        if ($this->stopForTerminalPoll($workflowPoll)) {
            return false;
        }
        $this->rememberWorkflowMemoCapability($workflowPoll);
        $this->heartbeatIfDue();
        $workflowTask = $this->taskFromPoll($workflowPoll);
        if ($workflowTask !== null) {
            $this->executePolledTask('workflow', $workflowTask);
            $handled = true;
        }
        if ($this->shutdownRequested) {
            return $handled;
        }

        $activityPoll = $this->pollWithRetry(
            'activity',
            fn (): array => $this->client->pollActivityTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $this->preparePoll($handled ? 0 : $pollTimeoutSeconds),
            ),
        );
        if ($activityPoll === null) {
            return $handled;
        }
        if ($this->stopForTerminalPoll($activityPoll)) {
            return $handled;
        }
        $this->heartbeatIfDue();
        $activityTask = $this->taskFromPoll($activityPoll);
        if ($activityTask !== null) {
            $this->executePolledTask('activity', $activityTask);
            $handled = true;
        }
        if ($this->shutdownRequested) {
            return $handled;
        }

        $queryPoll = $this->pollWithRetry(
            'query',
            fn (): array => $this->client->pollQueryTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $this->preparePoll($handled ? 0 : $pollTimeoutSeconds),
            ),
        );
        if ($queryPoll === null) {
            return $handled;
        }
        if ($this->stopForTerminalPoll($queryPoll)) {
            return $handled;
        }
        $this->heartbeatIfDue();
        $queryTask = $this->taskFromPoll($queryPoll);
        if ($queryTask !== null) {
            $this->executePolledTask('query', $queryTask);
            $handled = true;
        }

        return $handled;
    }

    /**
     * @param \Closure(): array<string, mixed> $poll
     * @return array<string, mixed>|null
     */
    private function pollWithRetry(string $taskKind, \Closure $poll): ?array
    {
        $attempt = 0;
        while (!$this->shutdownRequested) {
            try {
                return $poll();
            } catch (ServerException $exception) {
                if (!PollResponse::isTransientFailure($exception)) {
                    throw $exception;
                }

                ++$attempt;
                $delaySeconds = $this->transientRetryDelay(
                    $attempt,
                    $exception->details['retry_after_seconds'] ?? null,
                );
                if ($this->transientPollRetryObserver !== null) {
                    ($this->transientPollRetryObserver)($taskKind, $attempt, $delaySeconds, $exception);
                }
                $this->diagnostic('worker.retrying', [
                    'worker_id' => $this->workerId,
                    'operation' => "{$taskKind}_poll",
                    'attempt' => $attempt,
                    'delay_seconds' => $delaySeconds,
                    'exception' => $exception,
                ], 'warning');
                $this->waitForTransientRetry($delaySeconds);
            }
        }

        return null;
    }

    private function transientRetryDelay(int $attempt, mixed $retryAfterSeconds): float
    {
        $exponent = min(max(0, $attempt - 1), 6);
        $delaySeconds = self::INITIAL_TRANSIENT_RETRY_DELAY_SECONDS * (2 ** $exponent);
        if (is_int($retryAfterSeconds)) {
            $delaySeconds = max($delaySeconds, (float) $retryAfterSeconds);
        }

        return min(self::MAX_TRANSIENT_RETRY_DELAY_SECONDS, $delaySeconds);
    }

    private function waitForTransientRetry(float $delaySeconds): void
    {
        $deadline = $this->now() + $delaySeconds;
        while (!$this->shutdownRequested) {
            $this->heartbeatIfDue();
            $remainingSeconds = $deadline - $this->now();
            if ($remainingSeconds <= 0) {
                return;
            }

            $sleepSeconds = min(self::TRANSIENT_RETRY_SLEEP_SLICE_SECONDS, $remainingSeconds);
            if ($this->registered) {
                $untilHeartbeatSeconds = $this->heartbeatIntervalSeconds - $this->elapsedSinceHeartbeat();
                if ($untilHeartbeatSeconds > 0) {
                    $sleepSeconds = min($sleepSeconds, $untilHeartbeatSeconds);
                }
            }

            ($this->sleeper)((int) max(1, ceil($sleepSeconds * 1_000_000)));
        }
    }

    /** @param array<string, mixed> $response */
    private function stopForTerminalPoll(array $response): bool
    {
        if (!PollResponse::isTerminal($response)) {
            return false;
        }

        $this->shutdownRequested = true;
        $this->registered = false;
        $this->diagnostic('worker.stopped_by_server', [
            'worker_id' => $this->workerId,
            'poll_status' => $response['poll_status'] ?? null,
            'reason' => $response['reason'] ?? null,
        ], 'warning');

        return true;
    }

    /**
     * @param array<string, mixed> $response
     * @return array<string, mixed>|null
     */
    private function taskFromPoll(array $response): ?array
    {
        $task = $response['task'] ?? null;
        if (!is_array($task)) {
            return null;
        }

        /** @var array<string, mixed> $task */
        return $task;
    }

    /** @param array<string, mixed> $task */
    private function executePolledTask(string $taskKind, array $task): void
    {
        try {
            $this->assertSupportedTaskPayloadCodec($task);
        } catch (CodecException $exception) {
            $this->rejectPolledTask($taskKind, $task, $exception);

            return;
        }

        match ($taskKind) {
            'workflow' => $this->executeWorkflowTask($task),
            'activity' => $this->executeActivityTask($task),
            'query' => $this->executeQueryTask($task),
            default => throw new \LogicException("Unsupported polled task kind {$taskKind}."),
        };
    }

    /** @param array<string, mixed> $task */
    private function rejectPolledTask(string $taskKind, array $task, CodecException $exception): void
    {
        $taskId = (string) ($task[$taskKind === 'query' ? 'query_task_id' : 'task_id'] ?? '');
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        $this->acknowledgeTaskFailure(
            $taskKind,
            $taskId,
            $exception,
            match ($taskKind) {
                'workflow' => function (Throwable $failure) use ($taskId, $leaseOwner, $task): void {
                    $this->client->failWorkflowTask(
                        $taskId,
                        $leaseOwner,
                        (int) ($task['workflow_task_attempt'] ?? 1),
                        'PHP workflow task execution failed: '.$failure->getMessage(),
                        $failure::class,
                    );
                },
                'activity' => function (Throwable $failure) use ($taskId, $leaseOwner, $task): void {
                    $this->client->failActivityTask(
                        $taskId,
                        (string) ($task['activity_attempt_id'] ?? $task['attempt_id'] ?? ''),
                        $leaseOwner,
                        $failure->getMessage(),
                        $failure::class,
                        nonRetryable: true,
                    );
                },
                'query' => function (Throwable $failure) use ($taskId, $leaseOwner, $task): void {
                    $this->client->failQueryTask(
                        $taskId,
                        $leaseOwner,
                        (int) ($task['query_task_attempt'] ?? 1),
                        $failure->getMessage(),
                    );
                },
                default => throw new \LogicException("Unsupported polled task kind {$taskKind}."),
            },
        );
    }

    /** @param array<string, mixed> $task */
    private function executeWorkflowTask(array $task): void
    {
        $taskId = (string) ($task['task_id'] ?? '');
        $attempt = (int) ($task['workflow_task_attempt'] ?? 1);
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        $messageStreamCursors = [];
        $messageStreamWaits = [];
        try {
            $history = $this->completeHistory($task, $leaseOwner, $attempt);
            if (!$this->renewWorkflowTaskLease($taskId, $leaseOwner, $attempt)) {
                return;
            }
            $workflowType = (string) ($task['workflow_type'] ?? '');
            $updateId = isset($task['workflow_update_id']) ? (string) $task['workflow_update_id'] : null;
            if ($updateId !== null && $updateId !== '') {
                $commands = [$this->executeUpdate($workflowType, $updateId, $history, $task)];
            } else {
                $handler = $this->workflows[$workflowType] ?? null;
                if ($handler === null) {
                    throw new \RuntimeException("No workflow handler is registered for {$workflowType}.");
                }
                $input = $this->decodeArguments($task['arguments'] ?? $task['input'] ?? null);
                try {
                    $replay = $this->replayer->replay(
                        $handler,
                        $history,
                        $input,
                        $this->taskQueue,
                        $task,
                        fn (string $activityType, array $arguments, array $options): array => $this->executeLocalActivity(
                            $task,
                            $activityType,
                            $arguments,
                            $options,
                        ),
                    );
                    $commands = $replay->commands;
                    $messageStreamCursors = $replay->messageStreamCursors;
                    $messageStreamWaits = $replay->messageStreamWaits;
                    if ($replay->terminalFailure instanceof Throwable) {
                        $this->handlerFailure('workflow', $workflowType, $replay->terminalFailure);
                        $commands[] = $this->workflowFailureCommand($replay->terminalFailure);
                    } else {
                        $this->diagnoseWorkflowWait($task, $commands);
                    }
                } catch (NonDeterministicWorkflow $exception) {
                    throw $exception;
                } catch (Throwable $exception) {
                    $this->handlerFailure('workflow', $workflowType, $exception);
                    $commands = [$this->workflowFailureCommand($exception)];
                }
            }
            $this->assertWorkflowMemoUpdatesAvailable($commands);
            $this->client->completeWorkflowTask(
                $taskId,
                $leaseOwner,
                $attempt,
                $commands,
                $messageStreamCursors,
                $messageStreamWaits,
                $this->stickyCacheClaim($task),
            );
        } catch (Throwable $exception) {
            $this->acknowledgeTaskFailure(
                'workflow',
                $taskId,
                $exception,
                function (Throwable $failure) use ($taskId, $leaseOwner, $attempt): void {
                    $this->client->failWorkflowTask(
                        $taskId,
                        $leaseOwner,
                        $attempt,
                        'PHP workflow task execution failed: '.$failure->getMessage(),
                        $failure::class,
                        $failure instanceof NonDeterministicWorkflow ? $failure->reason : null,
                        $failure instanceof NonDeterministicWorkflow ? $failure->sequence : null,
                    );
                },
            );
        }
    }

    private function renewWorkflowTaskLease(string $taskId, string $leaseOwner, int $taskAttempt): bool
    {
        $retryAttempt = 0;
        while (!$this->shutdownRequested) {
            $response = $this->client->heartbeatWorkflowTask($taskId, $leaseOwner, $taskAttempt);
            if (!$this->matchesWorkflowTaskLeaseFence($response, $taskId, $leaseOwner, $taskAttempt)) {
                throw $this->workflowTaskLeaseResponseFailure(
                    'Workflow task lease renewal returned mismatched fencing fields.',
                    $response,
                );
            }

            if (($response['renewed'] ?? null) === true) {
                return true;
            }

            if (!$this->isTransientWorkflowTaskLeaseRefusal($response)) {
                throw $this->workflowTaskLeaseResponseFailure(
                    'Workflow task lease renewal was not acknowledged.',
                    $response,
                );
            }

            ++$retryAttempt;
            $this->waitForTransientRetry($this->transientRetryDelay(
                $retryAttempt,
                $response['retry_after_seconds'] ?? null,
            ));
        }

        return false;
    }

    /** @param array<string, mixed> $poll */
    private function rememberWorkflowMemoCapability(array $poll): void
    {
        $capabilities = $poll['server_capabilities'] ?? null;
        $memo = is_array($capabilities) ? ($capabilities['workflow_memo_updates'] ?? null) : null;
        $supportedCommands = is_array($capabilities)
            ? ($capabilities['supported_workflow_task_commands'] ?? null)
            : null;
        $this->workflowMemoCapability = [
            'supported' => is_array($memo)
                && ($memo['supported'] ?? null) === true
                && is_array($supportedCommands)
                && in_array('upsert_memo', $supportedCommands, true),
        ];
    }

    /** @param list<array<string, mixed>> $commands */
    private function assertWorkflowMemoUpdatesAvailable(array $commands): void
    {
        $usesMemo = false;
        foreach ($commands as $command) {
            if (($command['type'] ?? null) === 'upsert_memo') {
                $usesMemo = true;
                break;
            }
        }
        if (!$usesMemo || ($this->workflowMemoCapability['supported'] ?? null) === true) {
            return;
        }

        throw new \RuntimeException(
            'workflow_memo_updates_unavailable: the connected runtime did not advertise workflow memo update support.',
        );
    }

    /** @param array<string, mixed> $response */
    private function matchesWorkflowTaskLeaseFence(
        array $response,
        string $taskId,
        string $leaseOwner,
        int $taskAttempt,
    ): bool {
        return ($response['task_id'] ?? null) === $taskId
            && ($response['lease_owner'] ?? null) === $leaseOwner
            && ($response['workflow_task_attempt'] ?? null) === $taskAttempt;
    }

    /** @param array<string, mixed> $response */
    private function isTransientWorkflowTaskLeaseRefusal(array $response): bool
    {
        if (($response['renewed'] ?? null) !== false || ($response['retryable'] ?? null) !== true) {
            return false;
        }

        $reason = $response['reason'] ?? null;
        if (!is_string($reason) || $reason === '') {
            return false;
        }

        if (array_key_exists('retry_after_seconds', $response)
            && (!is_int($response['retry_after_seconds']) || $response['retry_after_seconds'] < 0)) {
            return false;
        }

        if ($reason === 'backend_lock_pressure') {
            return isset($response['retry_after_seconds']) && $response['retry_after_seconds'] > 0;
        }

        return true;
    }

    /** @param array<string, mixed> $response */
    private function workflowTaskLeaseResponseFailure(string $fallbackMessage, array $response): ServerException
    {
        $message = $response['message'] ?? $response['error'] ?? $fallbackMessage;
        $reason = $response['reason'] ?? null;

        return new ServerException(
            is_string($message) && $message !== '' ? $message : $fallbackMessage,
            200,
            is_string($reason) && $reason !== '' ? $reason : 'invalid_workflow_task_lease_response',
            $response,
        );
    }

    /** @param array<string, mixed> $task */
    private function executeActivityTask(array $task): void
    {
        $taskId = (string) ($task['task_id'] ?? '');
        $attemptId = (string) ($task['activity_attempt_id'] ?? $task['attempt_id'] ?? '');
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        $activityType = (string) ($task['activity_type'] ?? '');
        try {
            $this->trackWorkerSessionFromTask($task);
            $handler = $this->activities[$activityType] ?? null;
            if ($handler === null) {
                throw new \RuntimeException("No activity handler is registered for {$activityType}.");
            }
            $context = new ActivityContext(
                $this->client,
                $taskId,
                $attemptId,
                $leaseOwner,
                $activityType,
                (int) ($task['attempt_number'] ?? 1),
            );
            $result = $handler($context, ...$this->decodeArguments($task['arguments'] ?? null));
            $this->client->completeActivityTask($taskId, $attemptId, $leaseOwner, $result);
        } catch (Throwable $exception) {
            $this->acknowledgeTaskFailure(
                'activity',
                $taskId,
                $exception,
                function (Throwable $failure) use ($taskId, $attemptId, $leaseOwner): void {
                    $this->client->failActivityTask(
                        $taskId,
                        $attemptId,
                        $leaseOwner,
                        $failure->getMessage(),
                        $failure::class,
                        $failure instanceof ActivityCancelled,
                    );
                },
            );
        }
    }

    /** @param array<string, mixed> $task */
    private function executeQueryTask(array $task): void
    {
        $taskId = (string) ($task['query_task_id'] ?? $task['task_id'] ?? '');
        $attempt = (int) ($task['query_task_attempt'] ?? 1);
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        try {
            $workflowType = (string) ($task['workflow_type'] ?? '');
            $queryName = (string) ($task['query_name'] ?? '');
            $handler = $this->queries[$workflowType][$queryName] ?? null;
            if ($handler === null) {
                throw new \RuntimeException("No query handler is registered for {$workflowType}.{$queryName}.");
            }
            $history = $this->historyFromTask($task);
            $context = new QueryContext(
                (string) ($task['workflow_id'] ?? ''),
                (string) ($task['run_id'] ?? ''),
                $history,
                $task,
            );
            $arguments = $this->decodeArguments($task['query_arguments'] ?? $task['arguments'] ?? null);
            $this->client->completeQueryTask($taskId, $leaseOwner, $attempt, $handler($context, ...$arguments));
        } catch (Throwable $exception) {
            $this->acknowledgeTaskFailure(
                'query',
                $taskId,
                $exception,
                function (Throwable $failure) use ($taskId, $leaseOwner, $attempt): void {
                    $this->client->failQueryTask($taskId, $leaseOwner, $attempt, $failure->getMessage());
                },
            );
        }
    }

    /** @param callable(Throwable): void $failureAcknowledgement */
    private function acknowledgeTaskFailure(
        string $taskKind,
        string $taskId,
        Throwable $taskFailure,
        callable $failureAcknowledgement,
    ): void {
        if ($this->isTerminalTaskConflict($taskKind, $taskId, $taskFailure)) {
            return;
        }
        $this->handlerFailure($taskKind, $taskId, $taskFailure);
        if ($taskFailure instanceof ServerException) {
            throw $taskFailure;
        }

        try {
            $failureAcknowledgement($taskFailure);
        } catch (Throwable $acknowledgementFailure) {
            if (!$this->isTerminalTaskConflict($taskKind, $taskId, $acknowledgementFailure)) {
                throw $acknowledgementFailure;
            }
        }
    }

    private function isTerminalTaskConflict(string $taskKind, string $taskId, Throwable $exception): bool
    {
        if (!$exception instanceof ServerException || $exception->status !== 409) {
            return false;
        }

        $details = $exception->details;
        if ($details === null || array_is_list($details)) {
            return false;
        }

        $taskIdField = $taskKind === 'query' ? 'query_task_id' : 'task_id';
        if (($details[$taskIdField] ?? null) !== $taskId) {
            return false;
        }

        $reason = $exception->reason;

        return match ($taskKind) {
            'workflow' => ($reason === 'run_closed'
                    && ($details['can_continue'] ?? null) === false
                    && ($details['task_status'] ?? null) === 'cancelled')
                || ($reason === 'task_not_leased'
                    && ($details['task_status'] ?? null) === 'cancelled'),
            'activity' => in_array($reason, ['run_cancelled', 'run_terminated'], true)
                && ($details['can_continue'] ?? null) === false
                && ($details['task_status'] ?? null) === 'cancelled',
            'query' => $reason === 'query_task_timed_out'
                && ($details['outcome'] ?? null) === 'rejected',
            default => false,
        };
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     * @return array<string, mixed>
     */
    private function executeUpdate(string $workflowType, string $updateId, array $history, array $task): array
    {
        $accepted = [];
        foreach (array_reverse($history) as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'UpdateAccepted') {
                continue;
            }
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            if (($payload['update_id'] ?? null) === $updateId) {
                $accepted = $payload;
                break;
            }
        }
        $updateName = (string) ($accepted['update_name'] ?? $task['update_name'] ?? '');
        $handler = $this->updates[$workflowType][$updateName] ?? null;
        if ($handler === null) {
            return [
                'type' => 'fail_update',
                'update_id' => $updateId,
                'message' => "No update handler is registered for {$workflowType}.{$updateName}.",
                'exception_type' => 'UnknownUpdate',
                'non_retryable' => true,
            ];
        }
        $context = new QueryContext(
            (string) ($task['workflow_id'] ?? ''),
            (string) ($task['run_id'] ?? ''),
            $history,
            $task,
        );
        $arguments = $this->decodeArguments($accepted['arguments'] ?? $task['arguments'] ?? null);
        try {
            return [
                'type' => 'complete_update',
                'update_id' => $updateId,
                'result' => $this->client->payloadCodec()->envelope($handler($context, ...$arguments)),
            ];
        } catch (Throwable $exception) {
            $this->handlerFailure('update', "{$workflowType}.{$updateName}", $exception);
            return [
                'type' => 'fail_update',
                'update_id' => $updateId,
                'message' => $exception->getMessage(),
                'exception_type' => $exception::class,
                'non_retryable' => true,
            ];
        }
    }

    /**
     * @param array<string, mixed> $task
     * @return list<array<string, mixed>>
     */
    private function completeHistory(array $task, string $leaseOwner, int $attempt): array
    {
        $history = $this->historyFromTask($task);
        $next = isset($task['next_history_page_token']) ? (string) $task['next_history_page_token'] : '';
        while ($next !== '') {
            $page = $this->client->workflowTaskHistory((string) $task['task_id'], $leaseOwner, $attempt, $next);
            foreach (($page['history_events'] ?? []) as $event) {
                if (is_array($event)) {
                    $history[] = $event;
                }
            }
            $newNext = isset($page['next_history_page_token']) ? (string) $page['next_history_page_token'] : '';
            if ($newNext === $next) {
                throw new \RuntimeException('Workflow history pagination returned the same page token twice.');
            }
            $next = $newNext;
        }

        $workflowId = (string) ($task['workflow_id'] ?? '');
        $runId = (string) ($task['run_id'] ?? '');
        if ($workflowId === '' || $runId === '') {
            return $history;
        }

        $history = $this->stickyCache->history(
            $workflowId,
            $runId,
            $this->effectiveBuildId(),
            $history,
            is_string($task['sticky_replay_mode'] ?? null) ? $task['sticky_replay_mode'] : null,
        );
        if ($history === null) {
            $history = $this->authoritativeWorkflowTaskHistory($task, $leaseOwner, $attempt);
        }
        $this->stickyCache->remember($workflowId, $runId, $this->effectiveBuildId(), $history);

        return $history;
    }

    /**
     * Fetch complete durable history after a sticky cache miss.
     *
     * @param array<string, mixed> $task
     * @return list<array<string, mixed>>
     */
    private function authoritativeWorkflowTaskHistory(array $task, string $leaseOwner, int $attempt): array
    {
        $history = [];
        $next = base64_encode('0');
        $seenTokens = [];
        do {
            if (isset($seenTokens[$next])) {
                throw new \RuntimeException('Authoritative workflow history pagination repeated a page token.');
            }
            $seenTokens[$next] = true;
            $page = $this->client->workflowTaskHistory((string) $task['task_id'], $leaseOwner, $attempt, $next);
            foreach (($page['history_events'] ?? []) as $event) {
                if (is_array($event)) {
                    $history[] = $event;
                }
            }
            $next = isset($page['next_history_page_token'])
                ? (string) $page['next_history_page_token']
                : '';
        } while ($next !== '');

        $first = $history[0] ?? null;
        if (! is_array($first)
            || ($first['event_type'] ?? $first['type'] ?? null) !== 'WorkflowStarted') {
            throw new \RuntimeException(
                'Authoritative workflow history must begin with WorkflowStarted after a sticky cache miss.',
            );
        }

        return $history;
    }

    /**
     * @param array<string, mixed> $task
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    private function executeLocalActivity(
        array $task,
        string $activityType,
        array $arguments,
        array $options,
    ): array {
        $options = WorkflowCommand::canonicalLocalActivityOptions($options);
        $handler = $this->activities[$activityType] ?? null;
        if ($handler === null) {
            $message = "No local activity handler is registered for {$activityType}.";

            return [
                'outcome' => 'failed',
                'message' => $message,
                'exception_type' => \RuntimeException::class,
                'non_retryable' => true,
                'attempts' => [[
                    'attempt_id' => $this->localActivityAttemptId($task, $activityType, $arguments, 1),
                    'attempt_number' => 1,
                    'outcome' => 'failed',
                    'duration_ms' => 0,
                    'message' => $message,
                    'exception_type' => \RuntimeException::class,
                    'non_retryable' => true,
                    'heartbeats' => [],
                ]],
            ];
        }

        $retryPolicy = $options['retry_policy'] ?? [];
        $maxAttempts = $retryPolicy['max_attempts'] ?? 1;
        $backoff = $retryPolicy['backoff_seconds'] ?? [];
        $nonRetryable = $retryPolicy['non_retryable_error_types'] ?? [];
        $startToClose = $options['start_to_close_timeout'] ?? null;
        $scheduleToClose = $options['schedule_to_close_timeout'] ?? null;
        $heartbeatTimeout = $options['heartbeat_timeout'] ?? null;
        $executionStartedAt = $this->now();
        $attempts = [];
        $totalHeartbeatCount = 0;

        for ($attemptNumber = 1; $attemptNumber <= $maxAttempts; ++$attemptNumber) {
            $attemptId = $this->localActivityAttemptId($task, $activityType, $arguments, $attemptNumber);
            if ($scheduleToClose !== null && $this->now() - $executionStartedAt >= $scheduleToClose) {
                $message = 'Local activity schedule-to-close timeout elapsed.';
                $attempts[] = [
                    'attempt_id' => $attemptId,
                    'attempt_number' => $attemptNumber,
                    'outcome' => 'timed_out',
                    'duration_ms' => 0,
                    'message' => $message,
                    'exception_type' => LocalActivityTimedOut::class,
                    'non_retryable' => false,
                    'timeout_kind' => 'schedule_to_close',
                    'heartbeats' => [],
                ];

                return [
                    'outcome' => 'timed_out',
                    'message' => $message,
                    'exception_type' => LocalActivityTimedOut::class,
                    'non_retryable' => false,
                    'timeout_kind' => 'schedule_to_close',
                    'attempts' => $attempts,
                ];
            }
            $attemptStartedAt = $this->now();
            $lastHeartbeatAt = $attemptStartedAt;
            $heartbeats = [];
            $heartbeatCount = 0;
            try {
                if ($this->shutdownRequested || (bool) ($task['cancel_requested'] ?? false)) {
                    throw new ActivityCancelled('The workflow requested local activity cancellation.');
                }
                $context = new ActivityContext(
                    $this->client,
                    (string) ($task['task_id'] ?? ''),
                    $attemptId,
                    (string) ($task['lease_owner'] ?? $this->workerId),
                    $activityType,
                    $attemptNumber,
                    function (array $details) use (
                        $task,
                        $attemptStartedAt,
                        $executionStartedAt,
                        $startToClose,
                        $scheduleToClose,
                        $heartbeatTimeout,
                        &$lastHeartbeatAt,
                        &$heartbeats,
                        &$heartbeatCount,
                        &$totalHeartbeatCount,
                    ): void {
                        $now = $this->now();
                        if ($this->shutdownRequested) {
                            throw new ActivityCancelled('Worker shutdown cancelled the local activity.');
                        }
                        if ($heartbeatTimeout !== null && $now - $lastHeartbeatAt > $heartbeatTimeout) {
                            throw new LocalActivityTimedOut(
                                'heartbeat',
                                'Local activity heartbeat timeout elapsed.',
                            );
                        }
                        if ($startToClose !== null && $now - $attemptStartedAt > $startToClose) {
                            throw new LocalActivityTimedOut(
                                'start_to_close',
                                'Local activity start-to-close timeout elapsed.',
                            );
                        }
                        if ($scheduleToClose !== null && $now - $executionStartedAt > $scheduleToClose) {
                            throw new LocalActivityTimedOut(
                                'schedule_to_close',
                                'Local activity schedule-to-close timeout elapsed.',
                            );
                        }
                        if ($heartbeatCount >= self::MAX_LOCAL_ACTIVITY_HEARTBEATS_PER_ATTEMPT) {
                            throw new InvalidLocalActivityReport(sprintf(
                                'Local activity attempts may contain at most %d heartbeats.',
                                self::MAX_LOCAL_ACTIVITY_HEARTBEATS_PER_ATTEMPT,
                            ));
                        }
                        if ($totalHeartbeatCount >= self::MAX_LOCAL_ACTIVITY_HEARTBEATS) {
                            throw new InvalidLocalActivityReport(sprintf(
                                'Local activity reports may contain at most %d heartbeats.',
                                self::MAX_LOCAL_ACTIVITY_HEARTBEATS,
                            ));
                        }
                        try {
                            $this->client->payloadCodec()->encode($details);
                        } catch (Throwable $exception) {
                            throw new InvalidLocalActivityReport(sprintf(
                                'Local activity heartbeat details could not be encoded with the %s payload codec.',
                                $this->client->payloadCodec()->name(),
                            ), previous: $exception);
                        }
                        try {
                            json_encode($details, JSON_THROW_ON_ERROR);
                        } catch (Throwable $exception) {
                            throw new InvalidLocalActivityReport(
                                'Local activity heartbeat details could not be encoded for the HTTP JSON wire boundary.',
                                previous: $exception,
                            );
                        }
                        $this->renewWorkflowTaskLease(
                            (string) ($task['task_id'] ?? ''),
                            (string) ($task['lease_owner'] ?? $this->workerId),
                            (int) ($task['workflow_task_attempt'] ?? 1),
                        );
                        $lastHeartbeatAt = $now;
                        ++$heartbeatCount;
                        ++$totalHeartbeatCount;
                        $previousElapsed = $heartbeats === []
                            ? 0
                            : (int) $heartbeats[array_key_last($heartbeats)]['elapsed_ms'];
                        $heartbeats[] = [
                            'details' => $details,
                            'elapsed_ms' => max(
                                $previousElapsed,
                                max(0, (int) round(($now - $attemptStartedAt) * 1000)),
                            ),
                        ];
                    },
                );
                $result = $handler($context, ...$arguments);
                $elapsed = $this->now() - $attemptStartedAt;
                if ($heartbeatTimeout !== null && $this->now() - $lastHeartbeatAt > $heartbeatTimeout) {
                    throw new LocalActivityTimedOut('heartbeat', 'Local activity heartbeat timeout elapsed.');
                }
                if ($startToClose !== null && $elapsed > $startToClose) {
                    throw new LocalActivityTimedOut(
                        'start_to_close',
                        'Local activity start-to-close timeout elapsed.',
                    );
                }
                if ($scheduleToClose !== null && $this->now() - $executionStartedAt > $scheduleToClose) {
                    throw new LocalActivityTimedOut(
                        'schedule_to_close',
                        'Local activity schedule-to-close timeout elapsed.',
                    );
                }
                $attempts[] = [
                    'attempt_id' => $attemptId,
                    'attempt_number' => $attemptNumber,
                    'outcome' => 'completed',
                    'duration_ms' => max(0, (int) round($elapsed * 1000)),
                    'heartbeats' => $heartbeats,
                ];

                return ['outcome' => 'completed', 'result' => $result, 'attempts' => $attempts];
            } catch (Throwable $exception) {
                $timedOut = $exception instanceof LocalActivityTimedOut;
                $cancelled = $exception instanceof ActivityCancelled;
                $timeoutKind = $timedOut ? $exception->timeoutKind : null;
                $type = $exception::class;
                $message = trim($exception->getMessage());
                $invalidFailureMetadata = false;
                try {
                    json_encode([
                        'message' => $message,
                        'exception_type' => $type,
                    ], JSON_THROW_ON_ERROR);
                } catch (Throwable) {
                    $invalidFailureMetadata = true;
                    $message = 'Local activity failure metadata could not be encoded for the HTTP JSON wire boundary.';
                    $type = InvalidLocalActivityReport::class;
                    $timedOut = false;
                    $cancelled = false;
                    $timeoutKind = null;
                }
                if (strlen($type) > self::MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES) {
                    $invalidFailureMetadata = true;
                    $message = 'Local activity failure metadata exceeded the published exception type limit.';
                    $type = InvalidLocalActivityReport::class;
                    $timedOut = false;
                    $cancelled = false;
                    $timeoutKind = null;
                }
                $invalidReport = $exception instanceof InvalidLocalActivityReport || $invalidFailureMetadata;
                $isNonRetryable = $cancelled || $invalidReport || in_array($type, $nonRetryable, true)
                    || in_array((new \ReflectionClass($exception))->getShortName(), $nonRetryable, true);
                $retry = ! $cancelled && ! $isNonRetryable && $attemptNumber < $maxAttempts;
                $backoffSeconds = $retry ? max(0, (int) ($backoff[$attemptNumber - 1] ?? 0)) : 0;
                if ($retry
                    && $scheduleToClose !== null
                    && $this->now() - $executionStartedAt + $backoffSeconds >= $scheduleToClose
                ) {
                    $retry = false;
                    $backoffSeconds = 0;
                }
                if ($message === '') {
                    $message = "Local activity failed with {$type}.";
                }
                $attempt = [
                    'attempt_id' => $attemptId,
                    'attempt_number' => $attemptNumber,
                    'outcome' => $cancelled ? 'cancelled' : ($timedOut ? 'timed_out' : 'failed'),
                    'duration_ms' => max(0, (int) round(($this->now() - $attemptStartedAt) * 1000)),
                    'message' => $message,
                    'exception_type' => $type,
                    'non_retryable' => $isNonRetryable,
                    'heartbeats' => $heartbeats,
                ];
                if ($timeoutKind !== null) {
                    $attempt['timeout_kind'] = $timeoutKind;
                }
                if ($retry) {
                    $attempt['retry_reason'] = $timedOut ? 'timeout' : 'failure';
                    $attempt['backoff_seconds'] = $backoffSeconds;
                }
                $attempts[] = $attempt;
                if ($retry) {
                    if ($backoffSeconds > 0) {
                        ($this->sleeper)($backoffSeconds * 1_000_000);
                    }
                    continue;
                }

                return [
                    'outcome' => $cancelled ? 'cancelled' : ($timedOut ? 'timed_out' : 'failed'),
                    'message' => $message,
                    'exception_type' => $type,
                    'non_retryable' => $isNonRetryable,
                    ...($timeoutKind === null ? [] : ['timeout_kind' => $timeoutKind]),
                    'attempts' => $attempts,
                ];
            }
        }

        throw new \LogicException('Local activity retry loop exhausted without a terminal outcome.');
    }

    /**
     * @param array<string, mixed> $task
     * @param list<mixed> $arguments
     */
    private function localActivityAttemptId(
        array $task,
        string $activityType,
        array $arguments,
        int $attemptNumber,
    ): string {
        return hash('sha256', implode("\0", [
            (string) ($task['task_id'] ?? ''),
            $activityType,
            (string) $attemptNumber,
            $this->client->payloadCodec()->encode($arguments),
        ]));
    }

    /**
     * @param array<string, mixed> $task
     * @return array{
     *     worker_id: string,
     *     workflow_id: string,
     *     run_id: string,
     *     build_id: string,
     *     ttl_seconds: int,
     *     metrics: array{hit: int, miss: int, eviction: int, forced_cold_replay: int}
     * }|null
     */
    private function stickyCacheClaim(array $task): ?array
    {
        if ((string) ($task['workflow_id'] ?? '') === '' || (string) ($task['run_id'] ?? '') === '') {
            return null;
        }

        return [
            'worker_id' => $this->workerId,
            'workflow_id' => (string) $task['workflow_id'],
            'run_id' => (string) $task['run_id'],
            'build_id' => $this->effectiveBuildId(),
            'ttl_seconds' => $this->stickyCache->ttlSeconds(),
            'metrics' => $this->stickyCache->metrics(),
        ];
    }

    private function effectiveBuildId(): string
    {
        return $this->buildId !== null && trim($this->buildId) !== ''
            ? trim($this->buildId)
            : SdkIdentity::registration();
    }

    /** @param array<string, mixed> $task */
    private function trackWorkerSessionFromTask(array $task): void
    {
        $session = $task['worker_session'] ?? null;
        if (! is_array($session) || ! is_string($session['session_id'] ?? null)) {
            return;
        }
        try {
            $options = new WorkerSessionOptions(
                sessionId: $session['session_id'],
                queue: is_string($session['queue'] ?? null) ? $session['queue'] : null,
                requirements: is_array($session['requirements'] ?? null) ? $session['requirements'] : [],
                leaseSeconds: is_int($session['lease_seconds'] ?? null) ? $session['lease_seconds'] : 120,
                ttlSeconds: is_int($session['ttl_seconds'] ?? null) ? $session['ttl_seconds'] : 1800,
                maxConcurrentActivities: is_int($session['max_concurrent_activities'] ?? null)
                    ? $session['max_concurrent_activities']
                    : 1,
            );
            $this->activeWorkerSessions[$options->sessionId] = $options;
        } catch (\InvalidArgumentException $exception) {
            $this->diagnostic('worker.session_invalid', ['exception' => $exception], 'warning');
        }
    }

    private function closeWorkerSessions(): void
    {
        foreach ($this->activeWorkerSessions as $options) {
            try {
                $this->client->closeWorkerSession($this->workerId, $options->sessionId, 'worker_shutdown');
            } catch (Throwable $exception) {
                $this->diagnostic('worker.session_close_failed', [
                    'session_id' => $options->sessionId,
                    'exception' => $exception,
                ], 'warning');
            }
        }
        $this->activeWorkerSessions = [];
    }

    /**
     * @param array<string, mixed> $task
     * @return list<array<string, mixed>>
     */
    private function historyFromTask(array $task): array
    {
        $raw = $task['history_events'] ?? $task['history'] ?? [];
        $history = [];
        if (is_array($raw)) {
            foreach ($raw as $event) {
                if (is_array($event)) {
                    $history[] = $event;
                }
            }
        }

        return $history;
    }

    /** @return list<mixed> */
    private function decodeArguments(mixed $raw): array
    {
        if ($raw === null) {
            return [];
        }
        $decoded = (is_array($raw) || is_string($raw))
            ? $this->client->payloadCodec()->decodeEnvelope($raw)
            : $raw;

        return is_array($decoded) && array_is_list($decoded) ? $decoded : [$decoded];
    }

    /** @param array<string, mixed> $task */
    private function assertSupportedTaskPayloadCodec(array $task): void
    {
        $codec = $task['payload_codec'] ?? null;
        if ($codec === 'avro') {
            return;
        }

        $rendered = !array_key_exists('payload_codec', $task)
            ? 'missing'
            : (is_string($codec) ? sprintf('"%s"', $codec) : get_debug_type($codec));

        throw new CodecException(sprintf(
            'unsupported_payload_codec: worker task payload_codec %s is not supported by Durable Workflow 2.0; use payload_codec="avro" with the fixed Avro Value schema and single-object framing. JSON remains the HTTP document transport, not a workflow payload codec.',
            $rendered,
        ));
    }

    private function preparePoll(int $requestedTimeoutSeconds): int
    {
        $timeoutSeconds = max(0, min(60, $requestedTimeoutSeconds));
        if (!$this->registered) {
            return $timeoutSeconds;
        }

        // A synchronous worker cannot heartbeat while a long poll is blocked.
        // Leave a one-second reserve when possible, then refresh early when
        // the next request would otherwise carry the worker to its cadence.
        $maxPollTimeoutSeconds = max(1, $this->heartbeatIntervalSeconds - 1);
        $timeoutSeconds = min($timeoutSeconds, $maxPollTimeoutSeconds);
        $elapsed = $this->elapsedSinceHeartbeat();
        if ($this->heartbeatIntervalSeconds > 1
            && $timeoutSeconds > 0
            && $elapsed + $timeoutSeconds >= $this->heartbeatIntervalSeconds) {
            $this->heartbeat();
        } else {
            $this->heartbeatIfDue();
        }

        return min($timeoutSeconds, max(1, $this->heartbeatIntervalSeconds - 1));
    }

    private function heartbeatIfDue(): void
    {
        if (!$this->registered || $this->shutdownRequested) {
            return;
        }

        if ($this->elapsedSinceHeartbeat() < $this->heartbeatIntervalSeconds) {
            return;
        }

        $this->heartbeat();
    }

    private function heartbeat(): void
    {
        $acknowledgement = $this->client->heartbeatWorker($this->workerId, [
            'workflow_available' => 1,
            'activity_available' => 1,
        ]);
        $this->applyHeartbeatInterval($acknowledgement);
        $this->lastHeartbeatAt = $this->now();
    }

    /** @param array<string, mixed> $response */
    private function applyHeartbeatInterval(array $response): void
    {
        $interval = $this->validHeartbeatInterval($response['heartbeat_interval_seconds'] ?? null);
        if ($interval !== null) {
            $this->heartbeatIntervalSeconds = $interval;
        }
    }

    private function validHeartbeatInterval(mixed $interval): ?int
    {
        if (!is_int($interval) || $interval < 1 || $interval > self::MAX_HEARTBEAT_INTERVAL_SECONDS) {
            return null;
        }

        return $interval;
    }

    private function elapsedSinceHeartbeat(): float
    {
        return max(0.0, $this->now() - $this->lastHeartbeatAt);
    }

    private function now(): float
    {
        return ($this->clock)();
    }

    private function installSignalHandlers(): void
    {
        if (!function_exists('pcntl_async_signals') || !function_exists('pcntl_signal')) {
            return;
        }
        pcntl_async_signals(true);
        foreach ([SIGINT, SIGTERM] as $signal) {
            pcntl_signal($signal, fn (): bool => $this->shutdownRequested = true);
        }
    }

    /**
     * @return array<string, array{
     *     queries: list<string>,
     *     query_contracts: list<array{name: string, parameters: list<array{
     *         name: string,
     *         position: int,
     *         required: bool,
     *         variadic: bool,
     *         default_available: bool,
     *         default: mixed,
     *         type: string|null,
     *         allows_null: bool
     *     }>}>,
     *     signals: list<string>,
     *     signal_contracts: list<array{name: string, parameters: list<array{
     *         name: string,
     *         position: int,
     *         required: bool,
     *         variadic: bool,
     *         default_available: bool,
     *         default: mixed,
     *         type: string|null,
     *         allows_null: bool
     *     }>}>,
     *     updates: list<string>,
     *     update_validators: list<string>,
     *     update_contracts: list<array{name: string, parameters: list<array{
     *         name: string,
     *         position: int,
     *         required: bool,
     *         variadic: bool,
     *         default_available: bool,
     *         default: mixed,
     *         type: string|null,
     *         allows_null: bool
     *     }>}>
     * }>
     */
    private function workflowCommandContracts(): array
    {
        $contracts = [];
        foreach (array_keys($this->workflows) as $workflowType) {
            $queries = $this->queries[$workflowType] ?? [];
            $signals = $this->signals[$workflowType] ?? [];
            $updates = $this->updates[$workflowType] ?? [];

            $contracts[$workflowType] = [
                'queries' => array_keys($queries),
                'query_contracts' => $this->commandHandlerContracts($queries, QueryContext::class),
                'signals' => array_keys($signals),
                'signal_contracts' => $this->commandHandlerContracts($signals),
                'updates' => array_keys($updates),
                'update_contracts' => $this->commandHandlerContracts($updates, QueryContext::class),
                'update_validators' => [],
            ];
        }

        return $contracts;
    }

    /**
     * @param array<string, HandlerDefinition|callable> $handlers
     * @param class-string|null $contextClass
     * @return list<array{name: string, parameters: list<array{
     *     name: string,
     *     position: int,
     *     required: bool,
     *     variadic: bool,
     *     default_available: bool,
     *     default: mixed,
     *     type: string|null,
     *     allows_null: bool
     * }>}>
     */
    private function commandHandlerContracts(array $handlers, ?string $contextClass = null): array
    {
        $contracts = [];

        foreach ($handlers as $name => $handler) {
            $parameters = [];
            $position = 0;
            $contract = $handler instanceof HandlerDefinition ? $handler->contract() : $handler;
            $reflection = new \ReflectionFunction(\Closure::fromCallable($contract));

            foreach ($reflection->getParameters() as $parameter) {
                $type = $parameter->getType();

                if ($type instanceof \ReflectionNamedType
                    && !$type->isBuiltin()
                    && $type->getName() === $contextClass
                ) {
                    continue;
                }

                $defaultAvailable = $parameter->isDefaultValueAvailable();
                $parameters[] = [
                    'name' => $parameter->getName(),
                    'position' => $position,
                    'required' => !$defaultAvailable && !$parameter->isVariadic(),
                    'variadic' => $parameter->isVariadic(),
                    'default_available' => $defaultAvailable,
                    'default' => $defaultAvailable ? $parameter->getDefaultValue() : null,
                    'type' => $type === null ? null : (string) $type,
                    'allows_null' => $type?->allowsNull() ?? true,
                ];
                $position++;
            }

            $contracts[] = [
                'name' => $name,
                'parameters' => $parameters,
            ];
        }

        return $contracts;
    }

    /** @param list<DiscoveredHandlers> $discoveries */
    private function assertDiscoveriesCanRegister(array $discoveries): void
    {
        $workflows = $this->workflows;
        $activities = $this->activities;
        $queries = $this->queries;
        $signals = $this->signals;
        $updates = $this->updates;

        foreach ($discoveries as $discovery) {
            try {
                foreach ($discovery->workflows as $name => $handler) {
                    $this->assertUnique($workflows, $name, 'workflow');
                    $workflows[$name] = $handler;
                }
                foreach ($discovery->activities as $name => $handler) {
                    $this->assertUnique($activities, $name, 'activity');
                    $activities[$name] = $handler;
                }
                foreach ($discovery->queries as $workflowType => $handlers) {
                    $queries[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertUnique($queries[$workflowType], $name, 'query');
                        $queries[$workflowType][$name] = $handler;
                    }
                }
                foreach ($discovery->signals as $workflowType => $handlers) {
                    $signals[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertSignalNameIsNotRuntimeReserved($name);
                        $this->assertUnique($signals[$workflowType], $name, 'signal');
                        $signals[$workflowType][$name] = $handler;
                    }
                }
                foreach ($discovery->updates as $workflowType => $handlers) {
                    $updates[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertUnique($updates[$workflowType], $name, 'update');
                        $updates[$workflowType][$name] = $handler;
                    }
                }
            } catch (\InvalidArgumentException $exception) {
                throw new InvalidWorkerDefinition(
                    $discovery->class,
                    $exception->getMessage().' Rename the attributed contract or remove the duplicate service.',
                );
            }
        }
    }

    private function assertSignalNameIsNotRuntimeReserved(string $signalName): void
    {
        if ($signalName === WorkflowContext::MESSAGE_STREAM_SIGNAL) {
            throw new \InvalidArgumentException(
                "Signal name {$signalName} is reserved by the workflow runtime.",
            );
        }
    }

    /** @param callable $handler */
    private function assertHandlerContext(callable $handler, string $contextClass, string $contract): void
    {
        $reflection = new \ReflectionFunction(\Closure::fromCallable($handler));
        $parameter = $reflection->getParameters()[0] ?? null;
        $type = $parameter?->getType();
        if ($type instanceof \ReflectionNamedType
            && !$type->isBuiltin()
            && $type->getName() === $contextClass
        ) {
            return;
        }

        throw new InvalidWorkerDefinition(
            $contract,
            "Make the first handler parameter {$contextClass}.",
        );
    }

    private function handlerFailure(string $kind, string $identity, Throwable $exception): void
    {
        $context = [
            'worker_id' => $this->workerId,
            'handler_kind' => $kind,
            'handler' => $identity,
            'exception' => $exception,
        ];
        if ($exception instanceof SagaCompensationFailed) {
            $context['saga_failure'] = $exception->diagnosticContext();
        }

        $this->diagnostic('worker.handler_failed', $context, 'error');
    }

    /** @return array{type: string, message: string, exception_type: class-string<Throwable>} */
    private function workflowFailureCommand(Throwable $exception): array
    {
        $command = [
            'type' => 'fail_workflow',
            'message' => $exception->getMessage(),
            'exception_type' => $exception::class,
        ];

        try {
            json_encode($command, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            $command = [
                'type' => 'fail_workflow',
                'message' => 'Workflow failure metadata could not be encoded for the HTTP JSON wire boundary.',
                'exception_type' => \RuntimeException::class,
            ];
        }

        return $command;
    }

    /**
     * @param array<string, mixed> $task
     * @param list<array<string, mixed>> $commands
     */
    private function diagnoseWorkflowWait(array $task, array $commands): void
    {
        foreach ($commands as $command) {
            if (($command['type'] ?? null) !== 'open_condition_wait') {
                continue;
            }

            $this->diagnostic('worker.workflow_waiting', [
                'worker_id' => $this->workerId,
                'workflow_id' => $task['workflow_id'] ?? null,
                'run_id' => $task['run_id'] ?? null,
                'task_id' => $task['task_id'] ?? null,
                'wait_kind' => 'condition',
                'condition_key' => $command['condition_key'] ?? null,
                'condition_definition_fingerprint' => $command['condition_definition_fingerprint'] ?? null,
                'timeout_seconds' => $command['timeout_seconds'] ?? null,
            ]);

            return;
        }
    }

    /** @param array<string, mixed> $context */
    private function diagnostic(string $event, array $context = [], string $level = 'info'): void
    {
        $this->logger->log($level, $event, $context);
        if ($this->diagnosticListener !== null) {
            ($this->diagnosticListener)($event, $context);
        }
    }

    /** @param array<string, mixed> $registry */
    private function assertUnique(array $registry, string $name, string $kind): void
    {
        if (isset($registry[$name])) {
            throw new \InvalidArgumentException("Duplicate {$kind} registration: {$name}.");
        }
    }

    private function assertValidDeclarationName(string $name, string $kind, bool $signalDeclaration = false): void
    {
        if ($name === '' || trim($name) !== $name) {
            if ($signalDeclaration) {
                throw new \InvalidArgumentException(
                    "Signal declaration {$kind} must be non-empty without surrounding whitespace.",
                );
            }
            throw new InvalidWorkerDefinition(
                $kind,
                "Give the {$kind} contract a non-empty name without surrounding whitespace.",
            );
        }
    }
}
