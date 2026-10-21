<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Exception\InvalidLocalActivityReport;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Exception\LocalActivityTimedOut;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowEntryArgumentsMismatch;
use DurableWorkflow\Exception\SagaCompensationFailed;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\ActivityLeaseProcess;
use DurableWorkflow\Worker\ActivityExecutionFailure;
use DurableWorkflow\Worker\CapabilityManifest;
use DurableWorkflow\Worker\CancellationHistory;
use DurableWorkflow\Worker\CancellationDelivery;
use DurableWorkflow\Worker\CancellationRequest;
use DurableWorkflow\Worker\CancellationScopeDeliveryClaim;
use DurableWorkflow\Worker\CooperativeCancellationObserved;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\DiscoveredHandlers;
use DurableWorkflow\Worker\HandlerDiscovery;
use DurableWorkflow\Worker\HandlerDefinition;
use DurableWorkflow\Worker\WorkflowDefinitionFingerprint;
use DurableWorkflow\Worker\HandlerResolver;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\PreparedLocalActivityAttempt;
use DurableWorkflow\Worker\PreparedLocalActivityCall;
use DurableWorkflow\Worker\PreparedLocalActivityRunner;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\QueryTaskAuthority;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\ScopedActivityCancellationObserved;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowClaimDeferred;
use DurableWorkflow\Worker\WorkflowClaimRevoked;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\WorkflowEntryArguments;
use DurableWorkflow\Worker\StickyWorkflowCache;
use DurableWorkflow\Worker\WorkerSession;
use DurableWorkflow\Worker\WorkerSessionOptions;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;

/** Managed synchronous remote worker for workflow, activity, query, and update tasks. */
final class Worker
{
    private const DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;
    private const WORKFLOW_HISTORY_PAGE_SIZE = 500;
    private const INITIAL_TRANSIENT_RETRY_DELAY_SECONDS = 0.1;
    private const MAX_HEARTBEAT_INTERVAL_SECONDS = 3600;
    private const MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES = 255;
    private const MAX_LOCAL_ACTIVITY_HEARTBEATS = 1000;
    private const MAX_LOCAL_ACTIVITY_HEARTBEATS_PER_ATTEMPT = 1000;
    private const MAX_TRANSIENT_RETRY_DELAY_SECONDS = 5.0;
    private const TRANSIENT_RETRY_SLEEP_SLICE_SECONDS = 0.1;

    /** @var array<array-key, HandlerDefinition> */
    private array $workflows = [];
    /** @var array<array-key, HandlerDefinition> */
    private array $activities = [];
    /** @var array<array-key, array<array-key, HandlerDefinition>> */
    private array $queries = [];
    /** @var array<array-key, array<array-key, callable(mixed ...$arguments): mixed>> */
    private array $signals = [];
    /** @var array<array-key, array<array-key, HandlerDefinition>> */
    private array $updates = [];
    private bool $shutdownRequested = false;
    private bool $registered = false;
    private ?string $registrationToken = null;
    private bool $pollSweepRequested = false;
    private float $lastHeartbeatAt = 0.0;
    private float $heartbeatRetryAt = 0.0;
    private int $heartbeatRetryAttempt = 0;
    private int $heartbeatIntervalSeconds;
    /** @var \Closure(): float */
    private readonly \Closure $clock;
    /** @var \Closure(int): void */
    private readonly \Closure $sleeper;
    private readonly string $workerId;
    private readonly Client $client;
    private readonly Replayer $replayer;
    private readonly HandlerDiscovery $handlerDiscovery;
    private readonly LoggerInterface $logger;
    private readonly StickyWorkflowCache $stickyCache;
    /** @var array<string, WorkerSessionOptions> */
    private array $activeWorkerSessions = [];
    /** @var array<string, mixed>|null */
    private ?array $workflowMemoCapability = null;
    private bool $serviceOperationsSupported = false;
    private bool $queryTaskAuthoritySupported = false;
    private ?QueryTaskAuthority $queryTaskAuthority = null;
    private bool $activityLeaseRenewalSupported = false;
    private ?ActivityLeaseProcess $activityLeaseProcess = null;
    /** @var (\Closure(): void)|null */
    private ?\Closure $unbindActivityLease = null;
    private ?CancellationRequest $claimCancellation = null;
    private ?string $claimDeliveredCancellationId = null;
    private bool $preparedLocalActivityGroupsSupported = false;
    /** @var list<string> */
    private array $preparedLocalActivityCancellationPolicies = [];
    /** @var (\Closure(string, array<string, mixed>): void)|null */
    private readonly ?\Closure $diagnosticListener;

    public function __construct(
        Client $client,
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
        /**
         * Source opt-in. Activity callbacks run in a Unix child process. Open handler-owned
         * connections there. Captured memory changes do not update the owning worker.
         */
        private readonly bool $enableCooperativeCancellation = false,
        /** Source opt-in for durably admitted sequential local callbacks. */
        private readonly bool $enablePreparedLocalActivities = false,
        /** Unfrozen source opt-in for scalar/all-group scope delivery and committed scope replay. */
        private readonly bool $enableCancellationScopes = false,
        private readonly bool $requireQueryTaskAuthority = false,
        private readonly ?ExecutionTelemetry $telemetry = null,
    ) {
        if ($enableCancellationScopes && !$enableCooperativeCancellation) {
            throw new \InvalidArgumentException('Cancellation scopes require the cooperative worker opt-in.');
        }
        if ($enablePreparedLocalActivities && !$enableCooperativeCancellation) {
            throw new \InvalidArgumentException('Prepared local activities require the cooperative worker opt-in.');
        }
        if ($enableCooperativeCancellation && !Version::supportsCooperativeCancellation($client->workerProtocolVersion)) {
            throw new \InvalidArgumentException('Cooperative cancellation requires explicit worker protocol 1.20.');
        }
        if ($enableCooperativeCancellation && !CooperativeActivityExecutor::available()) {
            throw new \InvalidArgumentException('Cooperative workers require Unix CLI with pcntl and posix.');
        }
        $workerClient = $telemetry === null ? $client : $client->withTelemetry($telemetry);
        $workerClient = $enableCooperativeCancellation ? $workerClient->withBoundedWorkerRequests() : $workerClient;
        $this->client = $workerClient->withWorkerPayloadObserver($this->workerPayloadProgress(...));
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
        int $stickyCacheCapacity = 100,
        int $stickyCacheTtlSeconds = 300,
        bool $requireQueryTaskAuthority = false,
        ?ExecutionTelemetry $telemetry = null,
    ): self {
        return new self(
            $client,
            $taskQueue,
            container: $container,
            logger: $logger,
            diagnosticListener: $diagnosticListener,
            stickyCacheCapacity: $stickyCacheCapacity,
            stickyCacheTtlSeconds: $stickyCacheTtlSeconds,
            requireQueryTaskAuthority: $requireQueryTaskAuthority,
            telemetry: $telemetry,
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
                $this->registerWorkflowDefinition((string) $name, $handler);
            }
            foreach ($handlers->activities as $name => $handler) {
                $this->registerActivityDefinition((string) $name, $handler);
            }
            foreach ($handlers->queries as $workflowType => $queries) {
                foreach ($queries as $name => $handler) {
                    $this->registerQueryDefinition((string) $workflowType, (string) $name, $handler);
                }
            }
            foreach ($handlers->signals as $workflowType => $signals) {
                foreach ($signals as $name => $handler) {
                    $this->declareSignal((string) $workflowType, (string) $name, $handler);
                }
            }
            foreach ($handlers->updates as $workflowType => $updates) {
                foreach ($updates as $name => $handler) {
                    $this->registerUpdateDefinition((string) $workflowType, (string) $name, $handler);
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

    /** Request that run() stop polling and perform its managed cleanup on exit. */
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

    /** Register, initialize heartbeat state, poll, and deregister on exit. */
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
            $this->captureRegistrationFence($registration);
            $this->queryTaskAuthoritySupported = $this->client->workerProtocolVersion === '1.20'
                && ($registration['server_capabilities']['query_task_authority'] ?? null) === true
                && $this->client->supportsSupervisedQueryRequests();
            if (!$this->queryTaskAuthoritySupported && $this->queries !== []) {
                $this->diagnostic('worker.query_authority_unavailable', ['worker_id' => $this->workerId,
                    'mode' => 'legacy_local_timing', 'strong_authority' => false], 'warning');
            }
            $this->registered = true;
            $this->lastHeartbeatAt = $this->now();
            if ($this->activityLeaseRenewalSupported && $this->activities !== []) {
                if (!ActivityLeaseProcess::available()) {
                    throw new \RuntimeException('Automatic activity lease maintenance requires PHP CLI proc_open.');
                }
                $this->client->activityLeaseProcessConfiguration();
            }
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
            $this->stopActivityLease();
            $this->closeWorkerSessions();
            $this->stickyCache->clear();
            if ($this->registered) {
                try {
                    $this->deregisterWithRetry();
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
                'sticky_cache' => $this->stickyCacheMetrics(),
            ]);
        }
    }

    /** @param array<string, mixed> $registration */
    private function captureRegistrationFence(array $registration): void
    {
        $capabilities = $registration['server_capabilities'] ?? [];
        $token = $registration['registration_token'] ?? null;
        if (!is_array($capabilities)) {
            throw new ServerException('The worker registration fencing contract is invalid.', 201,
                'invalid_worker_registration_fence');
        }
        $this->activityLeaseRenewalSupported = ($capabilities['activity_lease_renewal'] ?? null) === true;
        $capability = $capabilities['worker_deregistration_fencing'] ?? null;
        if (($capability === null || (is_array($capability) && ($capability['supported'] ?? null) === false))
            && $token === null) {
            $this->registrationToken = null;
            return;
        }
        if (!is_array($capability) || array_is_list($capability)
            || ($capability['schema'] ?? null) !== 'durable-workflow.v2.worker-deregistration.v1'
            || ($capability['supported'] ?? null) !== true
            || ($capability['endpoint'] ?? null) !== '/worker/registrations/{workerId}/deregister'
            || ($capability['receipt_retention_seconds'] ?? null) !== 600
            || !is_string($token) || preg_match('/^[a-f0-9]{32}$/D', $token) !== 1) {
            throw new ServerException('The worker registration fencing contract is invalid.', 201,
                'invalid_worker_registration_fence');
        }
        $this->registrationToken = $token;
    }

    private function deregisterWithRetry(): void
    {
        if ($this->registrationToken === null) {
            $this->client->deregisterWorkerRegistration($this->workerId);
            return;
        }
        if (!$this->client->supportsBoundedWorkerRequests()) {
            $this->diagnostic('worker.shutdown_retry_unavailable', [
                'worker_id' => $this->workerId, 'reason' => 'unbounded_transport',
            ], 'warning');
            $this->client->deregisterWorkerRegistration($this->workerId, $this->registrationToken);
            return;
        }

        $budget = new RequestBudget(10, hrtime(true) / 1e9 + 10);
        $deadline = $this->now() + 10;
        if ($this->claimCancellation !== null) {
            $authority = new \DateTimeImmutable($this->claimCancellation->cleanupDeadlineAt);
            $budget->restrictWallAuthorityDeadline($authority);
            $deadline = min($deadline, (float) $authority->format('U.u'));
        }
        for ($attempt = 1; ; ++$attempt) {
            $budget->remainingSeconds();
            try {
                $this->client->deregisterWorkerRegistration($this->workerId, $this->registrationToken, $budget);
                return;
            } catch (ServerException $error) {
                if (!$this->isTransientDeregistrationFailure($error) || $this->now() >= $deadline) {
                    throw $error;
                }
                $delay = max(min(5.0, 0.1 * (2 ** min($attempt - 1, 6))),
                    (float) ($error->details['retry_after_seconds'] ?? 0));
                if ($this->now() + $delay >= $deadline) {
                    $this->diagnostic('worker.shutdown_budget_expired', ['worker_id' => $this->workerId,
                        'attempt' => $attempt, 'exception' => $error], 'error');
                    throw $error;
                }
                $this->diagnostic('worker.retrying', ['worker_id' => $this->workerId,
                    'operation' => 'deregister_worker', 'attempt' => $attempt,
                    'delay_seconds' => $delay, 'exception' => $error], 'warning');
                $wake = $this->now() + $delay;
                while ($this->now() < $wake) {
                    $budget->remainingSeconds();
                    ($this->sleeper)((int) ceil(min(0.1, $wake - $this->now()) * 1_000_000));
                }
            }
        }
    }

    private function isTransientDeregistrationFailure(ServerException $error): bool
    {
        if ($error->isTransientConnectionFailure() || $error->isTransientUpstreamFailure()) {
            return true;
        }
        $reply = $error->details;
        return $error->status === 503
            && in_array($error->reason, ['backend_lock_pressure', 'backend_unavailable'], true)
            && $reply !== null && !array_is_list($reply)
            && ($reply['reason'] ?? null) === $error->reason
            && ($reply['operation'] ?? null) === 'deregister_worker'
            && ($reply['worker_id'] ?? null) === $this->workerId
            && ($reply['registration_token'] ?? null) === $this->registrationToken
            && ($reply['outcome'] ?? null) === 'unknown'
            && ($reply['retryable'] ?? null) === true
            && is_int($reply['retry_after_seconds'] ?? null) && $reply['retry_after_seconds'] > 0;
    }

    /** Validate every registered command contract without contacting the server. */
    public function validate(): void
    {
        foreach ($this->workflows as $name => $handler) {
            $this->assertValidDeclarationName((string) $name, 'workflow');
            $this->assertHandlerContext($handler->contract(), WorkflowContext::class, "workflow {$name}");
        }
        foreach ($this->activities as $name => $handler) {
            $this->assertValidDeclarationName((string) $name, 'activity');
            $this->assertHandlerContext($handler->contract(), ActivityContext::class, "activity {$name}");
        }
        foreach ($this->queries as $workflowType => $handlers) {
            foreach ($handlers as $name => $handler) {
                $this->assertValidDeclarationName((string) $name, 'query');
                $this->assertHandlerContext($handler->contract(), QueryContext::class, "query {$workflowType}.{$name}");
            }
        }
        foreach ($this->signals as $workflowType => $handlers) {
            foreach (array_keys($handlers) as $name) {
                $this->assertValidDeclarationName((string) $name, 'signal');
            }
        }
        foreach ($this->updates as $workflowType => $handlers) {
            foreach ($handlers as $name => $handler) {
                $this->assertValidDeclarationName((string) $name, 'update');
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
     *     workflow_commands: array<array-key, mixed>
     * }
     */
    public function contracts(): array
    {
        return [
            'workflows' => $this->declarationNames($this->workflows),
            'activities' => $this->declarationNames($this->activities),
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
        if ($this->requireQueryTaskAuthority) {
            if ($this->client->workerProtocolVersion !== '1.20' || !$this->client->supportsSupervisedQueryRequests()) {
                throw new WorkflowClaimAborted('Query authority requires protocol 1.20 and bounded Worker transport.');
            }
            $protocol = $this->client->withBoundedWorkerRequests()->workerClusterInfo()->raw['worker_protocol'] ?? null;
            if (!is_array($protocol) || ($protocol['server_capabilities']['query_task_authority'] ?? null) !== true) {
                throw new WorkflowClaimAborted('The runtime does not advertise query task authority.');
            }
        }
        if ($this->enableCooperativeCancellation) {
            $protocol = $this->client->workerClusterInfo()->raw['worker_protocol'] ?? null;
            if (!is_array($protocol) || !is_string($protocol['version'] ?? null)
                || !Version::supportsCooperativeCancellation($protocol['version'])
                || ($protocol['server_capabilities']['cooperative_cancellation'] ?? null) !== true) {
                throw new WorkflowClaimAborted('Cooperative cancellation requires explicit compatible runtime discovery.');
            }
            if ($this->enablePreparedLocalActivities
                && ($protocol['server_capabilities']['prepared_local_activities'] ?? null) !== true) {
                throw new WorkflowClaimAborted('prepared_local_activity_not_supported: the Server must advertise the installed prepared-local bridge.');
            }
            $this->preparedLocalActivityGroupsSupported = $this->enablePreparedLocalActivities
                && ($protocol['server_capabilities']['prepared_local_activity_groups'] ?? null) === true;
            $policies = $protocol['server_capabilities']['prepared_local_activity_cancellation_policies'] ?? null;
            $this->preparedLocalActivityCancellationPolicies = $this->enablePreparedLocalActivities && is_array($policies)
                ? array_values(array_filter(['try_cancel', 'wait_cancellation_completed'], static fn (string $policy): bool => in_array($policy, $policies, true)))
                : [];
        }
        $attempt = 0;
        while (!$this->shutdownRequested) {
            try {
                return $this->client->registerWorker(
                    $this->workerId,
                    $this->taskQueue,
                    $this->declarationNames($this->workflows),
                    $this->declarationNames($this->activities),
                    [
                        'query_tasks',
                        'workflow_updates',
                        'durable_history_replay',
                        'workflow_entry_arguments_validation',
                        'graceful_shutdown',
                        'message_streams',
                        'memo_upserts',
                        'typed_search_attributes',
                        'durable_selection',
                        'local_activities',
                        'worker_sessions',
                        'sticky_execution',
                        'cross_kind_poll_wake',
                        ...($this->enableCooperativeCancellation ? ['cooperative_cancellation', 'nexus_cooperative_cancellation'] : []),
                        ...($this->enablePreparedLocalActivities ? ['prepared_local_activities'] : []),
                        ...($this->preparedLocalActivityGroupsSupported ? ['prepared_local_activity_groups'] : []),
                        ...($this->preparedLocalActivityCancellationPolicies !== [] ? ['prepared_local_activity_cancellation_policies'] : []),
                    ],
                    buildId: $this->buildId,
                    workflowCommandContracts: $this->workflowCommandContracts(),
                    capabilityManifest: [
                        ...CapabilityManifest::portableWorkerAffinity(),
                        ...($this->preparedLocalActivityCancellationPolicies !== [] ? [
                            'prepared_local_activity_cancellation_policies' => [
                                'supported' => true,
                                'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                'implementation' => 'prepared_local_policy_admission_and_replay',
                            ],
                        ] : []),
                        ...($this->enablePreparedLocalActivities ? [
                            'prepared_local_activities' => [
                                'supported' => true,
                                'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                'implementation' => 'durable_sequential_admission',
                            ],
                        ] : []),
                        ...($this->preparedLocalActivityGroupsSupported ? [
                            'prepared_local_activity_groups' => [
                                'supported' => true,
                                'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                'implementation' => 'durable_atomic_all_admission',
                            ],
                        ] : []),
                        ...($this->enableCooperativeCancellation ? [
                            'nexus_cooperative_cancellation' => [
                                'supported' => true,
                                'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                'implementation' => 'owned_workflow_run_canonical_delivery',
                                'binding_kinds' => ['workflow_run'],
                            ],
                            'cooperative_cancellation' => [
                                'supported' => true,
                                'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                'implementation' => 'authored_call_canonical_delivery',
                            ],
                        ] : []),
                    ],
                    workflowDefinitionFingerprints: $this->workflowDefinitionFingerprints(),
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
        if ($this->isTransientDatabaseFailure($exception, 'register_worker')
            || $exception->isStorageAdmissionFailure()) {
            return true;
        }

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

    /**
     * Execute at most one task of each kind without establishing or closing registration.
     * Use run() for the managed worker lifecycle.
     */
    public function tick(int $pollTimeoutSeconds = 1): bool
    {
        if ($this->shutdownRequested) {
            return false;
        }

        if ($this->pollSweepRequested) {
            $pollTimeoutSeconds = 0;
            $this->pollSweepRequested = false;
        }

        $handled = false;
        $workflowPoll = $this->pollWithRetry(
            'workflow',
            fn (string $requestId, int $timeoutSeconds): array => $this->client->pollWorkflowTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $timeoutSeconds,
                $requestId,
                self::WORKFLOW_HISTORY_PAGE_SIZE,
            ),
            $pollTimeoutSeconds,
        );
        if ($workflowPoll === null) {
            return false;
        }
        if ($this->stopForTerminalPoll($workflowPoll)) {
            return false;
        }
        $this->rememberWorkflowMemoCapability($workflowPoll);
        $capabilities = $workflowPoll['server_capabilities'] ?? null;
        $supportedCommands = is_array($capabilities) ? ($capabilities['supported_workflow_task_commands'] ?? null) : null;
        $this->serviceOperationsSupported = is_array($supportedCommands)
            && in_array('start_service_operation', $supportedCommands, true);
        $this->heartbeatIfDue();
        if ($this->crossKindPollWakeReceived($workflowPoll)) {
            $pollTimeoutSeconds = 0;
        }
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
            fn (string $requestId, int $timeoutSeconds): array => $this->client->pollActivityTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $timeoutSeconds,
                $requestId,
            ),
            $handled ? 0 : $pollTimeoutSeconds,
        );
        if ($activityPoll === null) {
            return $handled;
        }
        if ($this->stopForTerminalPoll($activityPoll)) {
            return $handled;
        }
        $this->heartbeatIfDue();
        if ($this->crossKindPollWakeReceived($activityPoll)) {
            $pollTimeoutSeconds = 0;
        }
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
            fn (string $requestId, int $timeoutSeconds): array => $this->client->pollQueryTaskResponse(
                $this->workerId,
                $this->taskQueue,
                $timeoutSeconds,
                $requestId,
            ),
            $handled ? 0 : $pollTimeoutSeconds,
        );
        if ($queryPoll === null) {
            return $handled;
        }
        if ($this->stopForTerminalPoll($queryPoll)) {
            return $handled;
        }
        $this->heartbeatIfDue();
        $this->crossKindPollWakeReceived($queryPoll);
        $queryTask = $this->taskFromPoll($queryPoll);
        if ($queryTask !== null) {
            $this->executePolledTask('query', $queryTask);
            $handled = true;
        }

        return $handled;
    }

    /** @param array<string, mixed> $response */
    private function crossKindPollWakeReceived(array $response): bool
    {
        if (($response['poll_status'] ?? null) !== 'task_queue_changed') {
            return false;
        }

        $this->pollSweepRequested = true;

        return true;
    }

    /**
     * @param \Closure(string, int): array<string, mixed> $poll
     * @return array<string, mixed>|null
     */
    private function pollWithRetry(string $taskKind, \Closure $poll, int $requestedTimeoutSeconds): ?array
    {
        $attempt = 0;
        $requestId = 'php-'.$taskKind.'-poll-'.bin2hex(random_bytes(16));
        while (!$this->shutdownRequested) {
            try {
                $timeoutSeconds = $this->preparePoll($requestedTimeoutSeconds);
                if ($this->shutdownRequested) {
                    return null;
                }

                return $poll($requestId, $timeoutSeconds);
            } catch (WorkflowClaimRevoked $exception) {
                if ($taskKind === 'activity') {
                    $this->stopActivityLease();
                }
                if ($taskKind === 'query') {
                    $this->queryTaskAuthority = null;
                }
                $cause = $exception->getPrevious();
                $this->diagnostic('worker.claim_aborted', ['task_kind' => $taskKind, 'task_id' => $exception->taskId,
                    'workflow_task_attempt' => $exception->taskAttempt, 'reason' => $exception->reason,
                    'cause_reason' => $cause instanceof ServerException ? $cause->reason : null,
                    'cause_type' => $cause === null ? null : $cause::class,
                    'message' => $exception->getMessage()], 'warning');

                return ['task' => null, 'poll_status' => 'empty'];
            } catch (ServerException $exception) {
                if ($taskKind === 'activity') {
                    $this->stopActivityLease();
                }
                $databaseUnavailable = $this->isTransientDatabaseFailure(
                    $exception,
                    "poll_{$taskKind}_task",
                    $requestId,
                );
                $storageAdmission = $exception->isStorageAdmissionFailure($requestId);
                $retryable = match ($exception->reason) {
                    'backend_unavailable' => $databaseUnavailable,
                    'storage_pressure', 'storage_admission_unavailable' => $storageAdmission,
                    default => PollResponse::isTransientFailure($exception),
                };
                if (!$retryable) {
                    throw $exception;
                }

                if (!$exception->isTransientConnectionFailure() && !$exception->isTransientUpstreamFailure()
                    && !PollResponse::isTransientPayloadFailure($exception)
                    && !$databaseUnavailable && !$storageAdmission) {
                    $requestId = 'php-'.$taskKind.'-poll-'.bin2hex(random_bytes(16));
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
                if ($exception->status === 429
                    && $exception->reason === 'long_poll_capacity_exhausted'
                    && ($exception->details['poll_status'] ?? null) === 'long_poll_capacity_exhausted') {
                    // This explicit empty-task refusal acquired no lease. Give
                    // other task kinds a turn instead of retrying only this one.
                    return $this->shutdownRequested ? null : $exception->details;
                }
            } catch (Throwable $exception) {
                if ($taskKind === 'activity') {
                    $this->stopActivityLease();
                }
                throw $exception;
            }
        }

        return null;
    }

    /**
     * @template T
     * @param \Closure(): T $request
     * @param array{task_id: string, lease_owner: string, attempt: int}|null $workflowTaskLease
     * @param array{task_id: string, activity_attempt_id: string, lease_owner: string}|null $activityTaskLease
     * @return T
     */
    private function retryStorageAdmission(
        string $operation,
        \Closure $request,
        ?array $workflowTaskLease = null,
        ?array $activityTaskLease = null,
    ): mixed
    {
        $attempt = 0;
        $uncertainActivityCompletion = false;
        while (true) {
            if (str_starts_with($operation, 'query_')) {
                $this->queryTaskAuthority?->confirm();
            }
            if ($operation !== 'workflow_fail') {
                $this->assertCancellationDeadline();
            }
            try {
                return $request();
            } catch (ServerException $exception) {
                $workflowCompletionDeferred = $operation === 'workflow_complete' && $workflowTaskLease !== null
                    && ($exception->isWorkflowTaskCompletionDeferred(
                        $workflowTaskLease['task_id'], $workflowTaskLease['lease_owner'], $workflowTaskLease['attempt'],
                    ) || $exception->isWorkflowTaskCompletionPublicationDeferred(
                        $workflowTaskLease['task_id'], $workflowTaskLease['lease_owner'], $workflowTaskLease['attempt'],
                    ));
                $workflowBackendUnavailable = $workflowTaskLease !== null
                    && $exception->isWorkflowTaskBackendUnavailable(
                        $workflowTaskLease['task_id'],
                        $workflowTaskLease['lease_owner'],
                        $workflowTaskLease['attempt'],
                        $operation === 'workflow_complete' ? 'complete_workflow_task' : 'heartbeat_workflow_task',
                    );
                $activityBackendUnavailable = $operation === 'activity_complete'
                    && $activityTaskLease !== null
                    && $exception->isActivityTaskBackendUnavailable(
                        $activityTaskLease['task_id'],
                        $activityTaskLease['activity_attempt_id'],
                        $activityTaskLease['lease_owner'],
                    );
                $activityDiscoveryUnavailable = $operation === 'activity_complete'
                    && $activityTaskLease !== null
                    && $exception->isActivityCompletionPayloadDiscoveryUnavailable(
                        $activityTaskLease['task_id'],
                        $activityTaskLease['activity_attempt_id'],
                        $activityTaskLease['lease_owner'],
                    );
                if ($uncertainActivityCompletion && $activityTaskLease !== null
                    && $this->isCommittedActivityCompletion($exception, $activityTaskLease)) {
                    return null;
                }
                $backendUnavailable = $workflowBackendUnavailable || $activityBackendUnavailable;
                if ((!$exception->isStorageAdmissionFailure() && !$backendUnavailable && !$activityDiscoveryUnavailable
                    && !$workflowCompletionDeferred)
                    || $this->shutdownRequested) {
                    if ($workflowCompletionDeferred && $this->shutdownRequested) {
                        throw new WorkflowClaimDeferred('Shutdown stopped the fenced workflow completion retry.');
                    }
                    throw $exception;
                }
                $uncertainActivityCompletion = $uncertainActivityCompletion || $activityBackendUnavailable;

                ++$attempt;
                $delaySeconds = $this->transientRetryDelay($attempt, $exception->details['retry_after_seconds'] ?? null);
                if ($this->transientPollRetryObserver !== null) {
                    ($this->transientPollRetryObserver)($operation, $attempt, $delaySeconds, $exception);
                }
                $this->diagnostic('worker.retrying', [
                    'worker_id' => $this->workerId,
                    'operation' => $operation,
                    'attempt' => $attempt,
                    'delay_seconds' => $delaySeconds,
                    'exception' => $exception,
                ], 'warning');
                $this->waitForTransientRetry($delaySeconds);
                if ($this->shutdownRequested) {
                    if ($workflowCompletionDeferred) {
                        throw new WorkflowClaimDeferred('Shutdown stopped the fenced workflow completion retry.');
                    }
                    // Leave the task unacknowledged; shutdown is not completion.
                    throw $exception;
                }
            }
        }
    }

    /** @param array{task_id: string, activity_attempt_id: string, lease_owner: string} $lease */
    private function isCommittedActivityCompletion(ServerException $exception, array $lease): bool
    {
        $details = $exception->details;

        return $exception->status === 409
            && $exception->reason === 'stale_attempt'
            && $details !== null && !array_is_list($details)
            && ($details['reason'] ?? null) === 'stale_attempt'
            && ($details['recorded'] ?? null) === false
            && ($details['task_id'] ?? null) === $lease['task_id']
            && ($details['activity_attempt_id'] ?? null) === $lease['activity_attempt_id']
            && ($details['lease_owner'] ?? null) === $lease['lease_owner']
            && ($details['activity_status'] ?? null) === 'completed'
            && ($details['attempt_status'] ?? null) === 'completed'
            && ($details['task_status'] ?? null) === 'completed';
    }

    private function isTransientDatabaseFailure(
        ServerException $exception,
        string $operation,
        ?string $pollRequestId = null,
    ): bool {
        $response = $exception->details;
        if ($exception->status !== 503 || $exception->reason !== 'backend_unavailable'
            || $response === null || array_is_list($response)
            || ($response['reason'] ?? null) !== 'backend_unavailable'
            || ($response['operation'] ?? null) !== $operation
            || ($response['worker_id'] ?? null) !== $this->workerId
            || ($response['outcome'] ?? null) !== 'unknown'
            || ($response['retryable'] ?? null) !== true
            || !is_int($response['retry_after_seconds'] ?? null)
            || $response['retry_after_seconds'] <= 0) {
            return false;
        }
        if ($operation !== 'heartbeat_worker' && ($response['task_queue'] ?? null) !== $this->taskQueue) {
            return false;
        }

        // Database loss can follow a committed claim. Keep the same identity
        // so the Server can return its existing lease instead of claiming again.
        return $pollRequestId === null || (
            array_key_exists('task', $response) && $response['task'] === null
            && ($response['poll_status'] ?? null) === 'backend_unavailable'
            && ($response['poll_request_id'] ?? null) === $pollRequestId
            && ($response['retry_same_poll_request_id'] ?? null) === true
        );
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
            $this->assertCancellationDeadline();
            $this->queryTaskAuthority?->remaining();
            $this->heartbeatIfDue($this->queryRequestBudget($this->queryTaskAuthority));
            $remainingSeconds = $deadline - $this->now();
            if ($remainingSeconds <= 0) {
                return;
            }

            $sleepSeconds = min(self::TRANSIENT_RETRY_SLEEP_SLICE_SECONDS, $remainingSeconds);
            $sleepSeconds = min($sleepSeconds, $this->queryTaskAuthority?->remaining() ?? INF);
            if ($this->registered) {
                $untilHeartbeatSeconds = max(
                    $this->lastHeartbeatAt + $this->heartbeatIntervalSeconds,
                    $this->heartbeatRetryAt,
                ) - $this->now();
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
            if ($this->telemetry === null || $this->client->observabilityContextAvailable()) {
                $this->executeTask($taskKind, $task);

                return;
            }
            $this->telemetry->task($taskKind, $task, $this->client->namespace, $this->taskQueue,
                fn () => $this->executeTask($taskKind, $task));
        } finally {
            if ($taskKind === 'activity') {
                $this->stopActivityLease();
            }
        }
    }

    /** @param array<string, mixed> $task */
    private function executeTask(string $taskKind, array $task): void
    {
        try {
            $this->assertSupportedTaskPayloadCodec($task);
        } catch (CodecException $exception) {
            $this->rejectPolledTask($taskKind, $task, $exception);

            return;
        }

        try {
            match ($taskKind) {
                'workflow' => $this->executeWorkflowTask($task),
                'activity' => $this->executeActivityTask($task),
                'query' => $this->executeQueryTask($task),
                default => throw new \LogicException("Unsupported polled task kind {$taskKind}."),
            };
        } catch (ServerException $exception) {
            if ($taskKind === 'activity' && $this->discardClosedActivityAttempt($task, $exception)) {
                return;
            }
            if ($taskKind !== 'workflow' || !$this->isTimedOutWorkflowCompletion($task, $exception)) {
                throw $exception;
            }
        }
    }

    /** @param array<string, mixed> $task */
    private function discardClosedActivityAttempt(array $task, ServerException $exception): bool
    {
        $taskId = (string) ($task['task_id'] ?? '');
        $attemptId = (string) ($task['activity_attempt_id'] ?? $task['attempt_id'] ?? '');
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $details = $exception->details;
        if ($taskId === '' || $attemptId === '' || $owner === ''
            || $exception->status !== 409 || $exception->reason !== 'stale_attempt'
            || $details === null || array_is_list($details)
            || ($details['reason'] ?? null) !== 'stale_attempt'
            || ($details['recorded'] ?? null) !== false
            || ($details['task_id'] ?? null) !== $taskId
            || ($details['activity_attempt_id'] ?? null) !== $attemptId
            || (array_key_exists('lease_owner', $details) && $details['lease_owner'] !== $owner)) {
            return false;
        }

        // Server failure acknowledgements prove the refusal with the submitted
        // task/attempt IDs. Completion replies also include the original owner
        // and closed state, which must agree before discarding that result.
        $failureRefusal = ($details['outcome'] ?? null) === 'failed'
            && !array_key_exists('attempt_status', $details)
            && !array_key_exists('activity_status', $details)
            && !array_key_exists('task_status', $details);
        $closedCompletion = ($details['outcome'] ?? null) === 'completed'
            && ($details['lease_owner'] ?? null) === $owner
            && in_array($details['attempt_status'] ?? null, ['failed', 'expired', 'cancelled'], true)
            && in_array($details['activity_status'] ?? null, ['pending', 'running', 'failed', 'cancelled'], true)
            && in_array($details['task_status'] ?? null, ['completed', 'cancelled', 'failed'], true);
        if ((!$failureRefusal && !$closedCompletion)
            || (array_key_exists('can_continue', $details) && $details['can_continue'] !== false)
            || (array_key_exists('heartbeat_recorded', $details) && $details['heartbeat_recorded'] !== false)) {
            return false;
        }

        $this->diagnostic('worker.claim_aborted', [
            'task_id' => $taskId, 'activity_attempt_id' => $attemptId,
            'task_kind' => 'activity', 'reason' => 'stale_attempt',
            'message' => 'Discarded a late activity outcome after the Server fenced its original attempt.',
        ], 'warning');

        return true;
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
        $traceScope = null;
        $this->claimCancellation = null;
        $this->claimDeliveredCancellationId = null;
        $taskId = (string) ($task['task_id'] ?? '');
        $attempt = (int) ($task['workflow_task_attempt'] ?? 1);
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        $messageStreamCursors = [];
        $messageStreamWaits = [];
        try {
            if (array_key_exists('cancellation_request', $task) && $task['cancellation_request'] !== null) {
                $this->observeClaimCancellation($task['cancellation_request']);
            }
            $history = $this->completeHistory($task, $leaseOwner, $attempt);
            if (!$this->renewWorkflowTaskLease($taskId, $leaseOwner, $attempt)) {
                return;
            }
            if ($this->client->observabilityContextAvailable()) {
                $traceScope = $this->telemetry?->enterTask('workflow', $task, $this->client->namespace, $this->taskQueue);
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
                // Keep pre-entry refusal outside the application-error boundary.
                // A valid-bound body throwing the same exception is still a body failure.
                WorkflowEntryArguments::validate($handler, $input, $workflowType);
                try {
                    $replay = $this->replayWorkflowClaim(
                        $handler,
                        $history,
                        $input,
                        $task,
                    );
                    $commands = $replay->commands;
                    $messageStreamCursors = $replay->messageStreamCursors;
                    $messageStreamWaits = $replay->messageStreamWaits;
                    if ($replay->terminalFailure instanceof ServerException
                        && $replay->terminalFailure->isStorageAdmissionFailure()) {
                        throw $replay->terminalFailure;
                    }
                    if ($replay->terminalFailure instanceof Throwable) {
                        $this->handlerFailure('workflow', $workflowType, $replay->terminalFailure);
                        $commands[] = $this->workflowFailureCommand(
                            $replay->terminalFailure,
                            $replay->failedActivitySequence,
                            $replay->failedActivityExecutionId,
                        );
                    } else {
                        $this->diagnoseWorkflowWait($task, $commands);
                    }
                } catch (NonDeterministicWorkflow|WorkflowClaimAborted $exception) {
                    throw $exception;
                } catch (Throwable $exception) {
                    if ($exception instanceof ServerException && $exception->isStorageAdmissionFailure()) {
                        throw $exception;
                    }
                    $this->handlerFailure('workflow', $workflowType, $exception);
                    $commands = [$this->workflowFailureCommand($exception)];
                }
            }
            $this->assertWorkflowMemoUpdatesAvailable($commands);
            $this->assertServiceOperationsAvailable($commands);
            $this->assertCancellationPoliciesAvailable($commands);
            $stickyClaim = $this->stickyCacheClaim($task);
            $this->retryStorageAdmission('workflow_complete', fn (): array => $this->client->completeWorkflowTask(
                $taskId,
                $leaseOwner,
                $attempt,
                $commands,
                $messageStreamCursors,
                $messageStreamWaits,
                $stickyClaim,
            ), [
                'task_id' => $taskId,
                'lease_owner' => $leaseOwner,
                'attempt' => $attempt,
            ]);
        } catch (Throwable $exception) {
            if ($exception instanceof WorkflowClaimDeferred) {
                $this->diagnostic('worker.claim_deferred', [
                    'task_id' => $taskId, 'task_kind' => 'workflow',
                    'reason' => $this->shutdownRequested ? 'shutdown_requested' : 'claim_released',
                    'message' => $exception->getMessage(),
                ], 'info');

                return;
            }
            if ($exception instanceof WorkflowClaimRevoked) {
                $this->diagnostic('worker.claim_aborted', [
                    'task_id' => $taskId, 'task_kind' => 'workflow',
                    'reason' => $exception->reason, 'message' => $exception->getMessage(),
                ], 'warning');

                return;
            }
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
                        $failure instanceof NonDeterministicWorkflow || $failure instanceof WorkflowEntryArgumentsMismatch ? $failure->reason : null,
                        $failure instanceof NonDeterministicWorkflow ? $failure->sequence : null,
                    );
                },
                $attempt,
            );
        } finally {
            $traceScope?->__invoke();
            $this->claimCancellation = null;
            $this->claimDeliveredCancellationId = null;
        }
    }

    /** @param array<string, mixed> $task */
    private function isTimedOutWorkflowCompletion(array $task, ServerException $exception): bool
    {
        $details = $exception->details;
        $runId = $task['run_id'] ?? null;

        return $exception->status === 409
            && $exception->reason === 'run_timed_out'
            && is_string($runId) && $runId !== ''
            && ($details['outcome'] ?? null) === 'completed'
            && ($details['recorded'] ?? null) === false
            && ($details['run_status'] ?? null) === 'failed'
            && ($details['run_id'] ?? null) === $runId
            && ($details['task_id'] ?? null) === ($task['task_id'] ?? '')
            && ($details['workflow_task_attempt'] ?? null) === (int) ($task['workflow_task_attempt'] ?? 1);
    }

    private function renewWorkflowTaskLease(string $taskId, string $leaseOwner, int $taskAttempt): bool
    {
        $retryAttempt = 0;
        while (!$this->shutdownRequested) {
            $response = $this->retryStorageAdmission('workflow_heartbeat', fn (): array =>
                $this->client->heartbeatWorkflowTask($taskId, $leaseOwner, $taskAttempt), [
                    'task_id' => $taskId,
                    'lease_owner' => $leaseOwner,
                    'attempt' => $taskAttempt,
                ]);
            if (!$this->matchesWorkflowTaskLeaseFence($response, $taskId, $leaseOwner, $taskAttempt)) {
                throw $this->workflowTaskLeaseResponseFailure(
                    'Workflow task lease renewal returned mismatched fencing fields.',
                    $response,
                );
            }

            if (($response['renewed'] ?? null) === true) {
                if (array_key_exists('cancellation_request', $response) && $response['cancellation_request'] !== null) {
                    $this->observeClaimCancellation($response['cancellation_request']);
                }
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

    /**
     * @param array<string, mixed>|null $body
     * @param array<string, mixed> $response
     * @return (\Closure(): void)|null
     */
    private function workerPayloadProgress(string $path, ?array $body, array $response, ?RequestBudget $budget): ?\Closure
    {
        if (in_array($path, ['/worker/query-tasks/poll', '/worker/activity-tasks/poll'], true)) {
            if (!$this->registered || !is_array($response['task'] ?? null)) {
                return null;
            }

            // Presence keeps this registration alive. It is not application
            // progress and cannot renew an activity or query execution budget.
            $task = $response['task'];
            $activity = $path === '/worker/activity-tasks/poll';
            if ($activity && $this->activityLeaseRenewalSupported) {
                if ($this->activityLeaseProcess !== null) {
                    throw new WorkflowClaimRevoked('activity_supervisor_conflict', 'A previous activity supervisor is still active.');
                }
                $this->activityLeaseProcess = new ActivityLeaseProcess($this->client, $task, $this->workerId,
                    $this->taskQueue, $response['server_time'] ?? null, hrtime(true) / 1e9,
                    $this->registered, $this->heartbeatIntervalSeconds);
                $this->unbindActivityLease = $this->client->bindActivityLease($this->activityLeaseProcess);

                $process = $this->activityLeaseProcess;

                return static fn () => $process->check();
            }
            $queryAuthority = null;
            if (!$activity) {
                $this->queryTaskAuthority = $queryAuthority = $this->queryAuthorityForTask($task);
                $queryAuthority?->confirm();
            }
            $observedAt = null;
            $queryObservedAt = hrtime(true) / 1e9;
            return function () use ($budget, $task, $activity, $queryAuthority, &$observedAt, &$queryObservedAt): void {
                if ($this->shutdownRequested) {
                    throw new WorkflowClaimRevoked('worker_stopped', 'Payload transfer stopped with its Worker.');
                }
                $queryAuthority?->remaining();
                if ($queryAuthority !== null && hrtime(true) / 1e9 - $queryObservedAt >= 1) {
                    $queryAuthority->confirm();
                    $queryObservedAt = hrtime(true) / 1e9;
                }
                $heartbeatBudget = $this->queryRequestBudget($queryAuthority) ?? $budget
                    ?? ($this->client->supportsBoundedWorkerRequests() ? new RequestBudget(5) : null);
                if ($activity && $this->enableCooperativeCancellation && isset($task['lease_expires_at'])) {
                    $this->assertPayloadDeadline($task['lease_expires_at']);
                    $heartbeatBudget?->restrictWallAuthorityDeadline(new \DateTimeImmutable($task['lease_expires_at']));
                }
                if ($queryAuthority !== null && $heartbeatBudget !== null) {
                    $queryAuthority->assertRequestBudget($heartbeatBudget);
                } else {
                    $heartbeatBudget?->remainingSeconds();
                }
                $this->heartbeatIfDue($heartbeatBudget);
                if ($queryAuthority !== null && $heartbeatBudget !== null) {
                    $queryAuthority->assertRequestBudget($heartbeatBudget);
                } else {
                    $heartbeatBudget?->remainingSeconds();
                }
                if ($this->heartbeatRetryAttempt > 0) {
                    // Drop the partial body and let the existing heartbeat-aware
                    // poll retry preserve the original request and task identity.
                    throw new TransportException('Payload loading waits for confirmed Worker presence.',
                        transientConnectionFailure: true);
                }
                if ($this->enableCooperativeCancellation) {
                    if ($activity && ($observedAt === null || $this->now() - $observedAt >= 1)) {
                        try {
                            $this->assertRemoteActivityClaimActive($task, $heartbeatBudget);
                        } catch (Throwable $error) {
                            throw new WorkflowClaimRevoked('payload_activity_claim_unconfirmed',
                                'Activity payload loading stopped before callback dispatch.', previous: $error);
                        }
                        $observedAt = $this->now();
                    }
                }
                $queryAuthority?->remaining();
            };
        }
        if ($path === '/worker/workflow-tasks/poll') {
            $task = $response['task'] ?? null;
            if (!is_array($task)) {
                return null;
            }
            $taskId = $task['task_id'] ?? null;
            $owner = $task['lease_owner'] ?? $this->workerId;
            $attempt = $task['workflow_task_attempt'] ?? 1;
        } elseif (preg_match('~\A/worker/workflow-tasks/([^/]+)/history\z~', $path, $matches) === 1) {
            $taskId = rawurldecode($matches[1]);
            $owner = $body['lease_owner'] ?? null;
            $attempt = $body['workflow_task_attempt'] ?? null;
        } else {
            return null;
        }
        if (!is_string($taskId) || $taskId === '' || $owner !== $this->workerId || !is_int($attempt) || $attempt < 1) {
            return null;
        }
        $renewedAt = null;
        $cancellation = $path === '/worker/workflow-tasks/poll' ? ($task['cancellation_request'] ?? null)
            : ($this->claimCancellation === null ? null : [
                'request_id' => $this->claimCancellation->requestId,
                'requested_at' => $this->claimCancellation->requestedAt,
                'cleanup_deadline_at' => $this->claimCancellation->cleanupDeadlineAt,
                'history_refresh_page_token' => $this->claimCancellation->historyRefreshPageToken,
            ]);
        $originalCancellation = null;

        return function () use ($taskId, $owner, $attempt, $budget, &$renewedAt, &$cancellation, &$originalCancellation): void {
            if ($this->shutdownRequested) {
                throw new WorkflowClaimRevoked('worker_stopped', 'Payload transfer stopped with its Worker.');
            }
            $this->assertPayloadCancellation($cancellation, $originalCancellation);
            if ($renewedAt !== null && $this->now() - $renewedAt < 10) {
                return;
            }
            $heartbeatBudget = $budget ?? ($this->client->supportsBoundedWorkerRequests() ? new RequestBudget(5) : null);
            if ($originalCancellation !== null) {
                $heartbeatBudget?->restrictWallAuthorityDeadline(new \DateTimeImmutable($originalCancellation->cleanupDeadlineAt));
            }
            try {
                $heartbeatBudget?->remainingSeconds();
                $receipt = $this->client->heartbeatWorkflowTask($taskId, $owner, $attempt, $heartbeatBudget);
                if (!$this->matchesWorkflowTaskLeaseFence($receipt, $taskId, $owner, $attempt)
                    || ($receipt['renewed'] ?? null) !== true) {
                    throw $this->workflowTaskLeaseResponseFailure('Payload transfer lease renewal was not acknowledged.', $receipt);
                }
                if (isset($receipt['cancellation_request'])) {
                    $cancellation = $receipt['cancellation_request'];
                    $this->assertPayloadCancellation($cancellation, $originalCancellation);
                    $heartbeatBudget?->restrictWallAuthorityDeadline(new \DateTimeImmutable($originalCancellation->cleanupDeadlineAt));
                }
                $this->heartbeatIfDue($heartbeatBudget);
                $heartbeatBudget?->remainingSeconds();
            } catch (Throwable $error) {
                if ($error instanceof ServerException && in_array($error->status, [401, 403], true)) {
                    throw $error;
                }
                throw new WorkflowClaimRevoked('payload_lease_unconfirmed',
                    'Payload transfer stopped because its original workflow lease could not be confirmed.', previous: $error,
                    taskId: $taskId, taskAttempt: $attempt);
            }
            $renewedAt = $this->now();
        };
    }

    private function assertPayloadDeadline(mixed $value): void
    {
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new WorkflowClaimRevoked('payload_deadline_invalid', 'Payload loading requires its original valid deadline.');
        }
        $deadline = new \DateTimeImmutable($value);
        $errors = \DateTimeImmutable::getLastErrors();
        if (($errors !== false && ($errors['warning_count'] > 0 || $errors['error_count'] > 0))
            || $this->now() >= (float) $deadline->format('U.u')) {
            throw new WorkflowClaimRevoked('payload_deadline_expired', 'Payload loading exceeded its original authority deadline.');
        }
    }

    private function assertPayloadCancellation(mixed $value, ?CancellationRequest &$original): void
    {
        if ($value === null) {
            return;
        }
        try {
            if (!$this->enableCooperativeCancellation || !is_array($value) || array_is_list($value)) {
                throw new \InvalidArgumentException('Payload cancellation was not negotiated or is malformed.');
            }
            $request = CancellationRequest::fromObservation($value);
            if ($original !== null && ($original->requestId !== $request->requestId
                || $original->requestedAt !== $request->requestedAt
                || $original->cleanupDeadlineAt !== $request->cleanupDeadlineAt)) {
                throw new \InvalidArgumentException('Payload cancellation changed its original identity or deadline.');
            }
            $original ??= $request;
            $this->assertPayloadDeadline($original->cleanupDeadlineAt);
        } catch (WorkflowClaimRevoked $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('payload_cancellation_invalid', 'Payload loading lost cancellation authority.', previous: $error);
        }
    }

    private function observeClaimCancellation(mixed $observation): CancellationRequest
    {
        if (!$this->enableCooperativeCancellation || !is_array($observation) || array_is_list($observation)) {
            throw new WorkflowClaimAborted('Workflow cancellation observation was not negotiated or is malformed.');
        }
        try {
            $current = CancellationRequest::fromObservation($observation);
        } catch (\InvalidArgumentException $error) {
            throw new WorkflowClaimAborted('Workflow cancellation observation is malformed.', previous: $error);
        }
        $original = $this->claimCancellation;
        if ($original !== null && ($original->requestId !== $current->requestId
            || $original->requestedAt !== $current->requestedAt
            || $original->cleanupDeadlineAt !== $current->cleanupDeadlineAt)) {
            throw new WorkflowClaimAborted('Workflow cancellation observation changed its original identity or deadline.');
        }
        $this->claimCancellation = $current;

        return $current;
    }

    /** @return array<string, string> */
    private function claimCancellationObservation(): array
    {
        $request = $this->claimCancellation;
        if ($request === null || $request->historyRefreshPageToken === null) {
            throw new WorkflowClaimAborted('Cancellation needs the Server-issued history refresh token on this claim.');
        }

        return ['request_id' => $request->requestId, 'requested_at' => $request->requestedAt,
            'cleanup_deadline_at' => $request->cleanupDeadlineAt,
            'history_refresh_page_token' => $request->historyRefreshPageToken];
    }

    /** @param array<string, mixed> $task
     *  @return list<array<string, mixed>>
     */
    private function refreshCancellationHistory(array $task): array
    {
        return $this->refreshWorkflowClaimHistory($task, $this->claimCancellationObservation()['history_refresh_page_token'], true);
    }

    /** @param array<string, mixed> $task
     * @return list<array<string, mixed>>
     */
    private function refreshWorkflowClaimHistory(array $task, string $token, bool $requireCancellation = false, ?RequestBudget $budget = null): array
    {
        if (trim($token) === '') {
            throw new WorkflowClaimAborted('Claim history refresh needs a nonempty Server-issued cursor.');
        }
        $seen = [];
        $history = [];
        $hasCanonicalRequest = false;
        do {
            if (isset($seen[$token])) {
                throw new WorkflowClaimAborted('Cancellation history paging repeated its opaque token.');
            }
            $seen[$token] = true;
            try {
                $page = $this->client->workflowTaskHistory((string) $task['task_id'],
                    (string) ($task['lease_owner'] ?? $this->workerId), (int) ($task['workflow_task_attempt'] ?? 1), $token, $budget);
            } catch (Throwable $error) {
                throw new WorkflowClaimAborted('Canonical cancellation history could not be loaded on this claim.', previous: $error);
            }
            $events = $page['history_events'] ?? null;
            if (!is_array($events) || !array_is_list($events)) {
                throw new WorkflowClaimAborted('Cancellation history page is not a list of canonical events.');
            }
            foreach ($events as $event) {
                if (!is_array($event) || array_is_list($event)) {
                    throw new WorkflowClaimAborted('Cancellation history page contains a malformed event.');
                }
                $history[] = $event;
                $hasCanonicalRequest = $hasCanonicalRequest
                    || ($event['event_type'] ?? $event['type'] ?? null) === CancellationHistory::REQUEST_EVENT;
            }
            $next = $page['next_history_page_token'] ?? null;
            if ($next !== null && !is_string($next)) {
                throw new WorkflowClaimAborted('Cancellation history page has a malformed next token.');
            }
            $token = $next ?? '';
        } while ($token !== '');
        if (!StickyWorkflowCache::startsWithWorkflowStart($history) || ($requireCancellation && !$hasCanonicalRequest)) {
            throw new WorkflowClaimAborted('Claim refresh must contain the original start and any required canonical cancellation.');
        }
        $workflowId = (string) ($task['workflow_id'] ?? '');
        $runId = (string) ($task['run_id'] ?? '');
        if ($workflowId !== '' && $runId !== '') {
            $this->stickyCache->remember($workflowId, $runId, $this->effectiveBuildId(), $history);
        }

        return $history;
    }

    /** @param callable(WorkflowContext, mixed ...$input): mixed $handler
     *  @param list<array<string, mixed>> $history
     *  @param list<mixed> $input
     *  @param array<string, mixed> $task
     */
    private function replayWorkflowClaim(callable $handler, array $history, array $input, array $task): ReplayResult
    {
        if ($this->claimCancellation !== null) {
            $history = $this->refreshCancellationHistory($task);
        }
        $cancellationPasses = 0;
        $lastScopeOpening = 0;
        /** @var array<string, array{claim: CancellationScopeDeliveryClaim, budget: RequestBudget, phase: string, checkpointed: bool}> $scopeClaims */
        $scopeClaims = [];
        while (true) {
            $observation = $this->claimCancellation === null ? null : $this->claimCancellationObservation();
            $state = CancellationHistory::fromEvents($history, (string) ($task['run_id'] ?? ''),
                $observation === null ? null : CancellationRequest::fromObservation($observation));
            if ($state->request !== null && $state->delivery === null && !$this->enableCooperativeCancellation) {
                throw new WorkflowClaimAborted('Pending cancellation requires a negotiated capable worker.');
            }
            $this->claimDeliveredCancellationId = $state->delivery?->requestId;
            $task['cancellation_request'] = $observation;
            $replay = null;
            try {
                $replay = $this->replayer->replay($handler, $history, $input, $this->taskQueue, $task,
                    fn (string $activityType, array $arguments, array $options): array => $this->executeLocalActivity(
                        $task, $activityType, $arguments, $options,
                    ), $this->enablePreparedLocalActivities, $this->preparedLocalActivityGroupsSupported,
                    $this->preparedLocalActivityCancellationPolicies, allowCancellationScopeAuthoring: $this->enableCooperativeCancellation,
                    replayCommittedCancellationScopes: $this->enableCancellationScopes,
                    prepareCancellationScopeDelivery: $this->enableCancellationScopes,
                    allowScopedPreparedLocalActivities: $this->enableCancellationScopes && $this->enablePreparedLocalActivities);
            } catch (CooperativeCancellationObserved) {
                if (++$cancellationPasses > 3) {
                    throw new WorkflowClaimAborted('Cancellation replay did not converge on its canonical delivery.');
                }
                $history = $this->refreshCancellationHistory($task);
                continue;
            }
            if ($replay->terminalFailure instanceof CooperativeCancellationObserved) {
                if ($replay->commands !== []) {
                    // Earlier local reports must commit before this claim is released.
                    return new ReplayResult($replay->commands, $replay->messageStreamCursors, $replay->messageStreamWaits);
                }
                $history = $this->refreshCancellationHistory($task);
                continue;
            }
            if ($replay->terminalFailure instanceof WorkflowClaimAborted) {
                throw $replay->terminalFailure;
            }
            if ($replay->cancellationScopeDelivery !== null) {
                $intent = $replay->cancellationScopeDelivery;
                $key = $intent->context->scopeId;
                try {
                    if (!isset($scopeClaims[$key])) {
                        $budget = new RequestBudget(5);
                        $scopeClaims[$key] = [
                            'claim' => new CancellationScopeDeliveryClaim($this->client, (string) $task['task_id'],
                                (string) ($task['lease_owner'] ?? $this->workerId), (int) ($task['workflow_task_attempt'] ?? 1), $intent, $budget),
                            'budget' => $budget, 'phase' => 'selected', 'checkpointed' => false,
                        ];
                    }
                    $claim = $scopeClaims[$key];
                    $claim['budget']->remainingSeconds();
                    if ($claim['phase'] === 'delivered') {
                        throw new WorkflowClaimAborted('Scope replay did not consume its original canonical delivery.');
                    }
                    if ($replay->commands !== []) {
                        if ($claim['checkpointed']) {
                            throw new WorkflowClaimAborted('Scope prefix did not advance past its original canonical checkpoint.');
                        }
                        $history = $this->checkpointPreparedLocalPrefix($task, $replay->commands, $intent->boundary->sequence,
                            scopePrefix: true, budget: $claim['budget']);
                        $scopeClaims[$key]['checkpointed'] = true;
                    } elseif ($claim['phase'] === 'selected') {
                        $receipt = $claim['claim']->prepare();
                        $history = $receipt->history;
                        $scopeClaims[$key]['phase'] = 'prepared';
                    } else {
                        $receipt = $claim['claim']->deliver($intent);
                        $history = $receipt->history;
                        $scopeClaims[$key]['phase'] = 'delivered';
                    }
                } catch (WorkflowClaimAborted|NonDeterministicWorkflow $error) {
                    throw $error;
                } catch (Throwable $error) {
                    throw new WorkflowClaimAborted('Scope delivery authority could not be proved on the original claim.', previous: $error);
                }
                continue;
            }
            if ($replay->cancellationScopeOpening !== null) {
                $opening = $replay->cancellationScopeOpening;
                if ($opening->sequence <= $lastScopeOpening) {
                    throw new WorkflowClaimAborted('Scope authoring did not advance past its original canonical opening.');
                }
                try {
                    if ($replay->commands !== []) {
                        $history = $this->checkpointPreparedLocalPrefix($task, $replay->commands, $opening->sequence, scopePrefix: true);
                    } else {
                        $receipt = $this->client->openCancellationScopeOnClaim(
                            (string) $task['task_id'], (string) $task['run_id'],
                            (string) ($task['lease_owner'] ?? $this->workerId), (int) ($task['workflow_task_attempt'] ?? 1),
                            $opening->sequence, $opening->parentScopeId, $opening->shieldParent,
                        );
                        $history = $receipt->history;
                        $lastScopeOpening = $opening->sequence;
                    }
                } catch (WorkflowClaimAborted|NonDeterministicWorkflow $error) {
                    throw $error;
                } catch (Throwable $error) {
                    throw new WorkflowClaimAborted('Scope authoring authority could not be proved on the original claim.', previous: $error);
                }
                continue;
            }
            if ($replay->preparedLocalActivityGroup !== null) {
                try {
                    $history = $this->executePreparedLocalActivityGroup($task, $history, $replay);
                } catch (CooperativeCancellationObserved) {
                    $history = $this->refreshCancellationHistory($task);
                }
                $cancellationPasses = 0;
                continue;
            }
            if ($replay->preparedLocalActivity !== null) {
                try {
                    $history = $this->executePreparedLocalActivity($task, $history, $replay);
                } catch (ScopedActivityCancellationObserved $error) {
                    if (!$this->enableCancellationScopes || $error->historyRefreshPageToken === null
                        || trim($error->historyRefreshPageToken) === '') {
                        throw new WorkflowClaimAborted('Scoped local stop lacks canonical scope replay authority.', previous: $error);
                    }
                    // The runner has already joined and acknowledged this attempt.
                    // Retain the hosting claim and replay the canonical scope boundary.
                    $history = $this->refreshWorkflowClaimHistory($task, $error->historyRefreshPageToken);
                    $canonicalRunRequest = CancellationHistory::fromEvents(
                        $history, (string) $task['run_id'], $this->claimCancellation
                    )->request;
                    if ($canonicalRunRequest !== null) {
                        // A canonical run request supplies root replay with the
                        // Server-issued cursor from this same original claim.
                        $this->claimCancellation = $canonicalRunRequest
                            ->withHistoryRefreshPageToken($error->historyRefreshPageToken);
                    }
                } catch (CooperativeCancellationObserved) {
                    $history = $this->refreshCancellationHistory($task);
                }
                $cancellationPasses = 0;
                continue;
            }
            $intent = $replay->cancellationDelivery;
            if ($intent === null || $replay->commands !== []) {
                return $replay;
            }
            $this->claimCancellationObservation();
            if (++$cancellationPasses > 3) {
                throw new WorkflowClaimAborted('Cancellation replay did not converge on its canonical delivery.');
            }
            $deliveryError = null;
            try {
                $this->deliverCancellationOrDeferClaim($task, $intent);
            } catch (WorkflowClaimRevoked|WorkflowClaimDeferred $error) {
                throw $error;
            } catch (Throwable $error) {
                $deliveryError = $error;
            }
            $history = $this->refreshCancellationHistory($task);
            $committed = CancellationHistory::fromEvents($history, (string) ($task['run_id'] ?? ''),
                CancellationRequest::fromObservation($this->claimCancellationObservation()));
            if ($committed->delivery != $intent) {
                throw new WorkflowClaimAborted('Cancellation delivery was not proved by matching canonical history.', previous: $deliveryError);
            }
        }

    }

    /** @param array<string, mixed> $task
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private function executePreparedLocalActivityGroup(array $task, array $history, ReplayResult $replay): array
    {
        $group = $replay->preparedLocalActivityGroup ?? throw new \LogicException('Missing prepared local group.');
        if (!$this->preparedLocalActivityGroupsSupported) {
            throw new WorkflowClaimAborted('Prepared local group execution lacks its negotiated capability.');
        }
        $taskId = (string) $task['task_id'];
        $runId = (string) $task['run_id'];
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $epoch = (int) ($task['workflow_task_attempt'] ?? 1);
        $members = [];
        $executionStarted = false;
        try {
            if ($replay->commands !== []) {
                return $this->checkpointPreparedLocalPrefix($task, $replay->commands, $group->baseSequence);
            }
            if (!$group->committed) {
                $checkpointId = hash('sha256', json_encode([$taskId, $owner, $epoch, $group->baseSequence, $group->commands], JSON_THROW_ON_ERROR));
                $receipt = $this->client->preparedLocalActivityOperation($taskId, $owner, $epoch, 'checkpoint-group', [
                    'checkpoint_id' => $checkpointId, 'start_sequence' => $group->baseSequence, 'commands' => $group->commands,
                ]);
                if (($receipt['checkpointed'] ?? null) !== true || !is_bool($receipt['duplicate'] ?? null)
                    || ($receipt['checkpoint_id'] ?? null) !== $checkpointId
                    || ($receipt['task_id'] ?? null) !== $taskId || ($receipt['workflow_run_id'] ?? null) !== $runId
                    || ($receipt['workflow_task_attempt'] ?? null) !== $epoch || ($receipt['lease_owner'] ?? null) !== $owner
                    || ($receipt['start_sequence'] ?? null) !== $group->baseSequence
                    || ($receipt['next_sequence'] ?? null) !== $group->baseSequence + $group->size
                    || !array_key_exists('reason', $receipt) || $receipt['reason'] !== null
                    || !is_array($receipt['local_activities'] ?? null) || !array_is_list($receipt['local_activities'])
                    || count($receipt['local_activities']) !== count($group->calls)) {
                    throw new WorkflowClaimAborted('Prepared local group lacks its complete original retained-claim receipt.');
                }
                $identities = [];
                foreach ($group->calls as $index => $call) {
                    $member = $receipt['local_activities'][$index];
                    if (!is_array($member) || ($member['sequence'] ?? null) !== $call->sequence
                        || !is_string($member['activity_execution_id'] ?? null) || trim($member['activity_execution_id']) === ''
                        || isset($identities[$member['activity_execution_id']])) {
                        throw new WorkflowClaimAborted('Prepared local group receipt changes its authored local members.');
                    }
                    $identities[$member['activity_execution_id']] = true;
                }
                // Validate complete canonical history on the next replay before
                // preparing even the first application callback.
                return $this->refreshPreparedLocalHistory($task, $receipt);
            }
            foreach ($group->calls as $call) {
                if ($call->recover) {
                    return $this->executePreparedLocalActivity($task, $history, new ReplayResult([], preparedLocalActivity: $call));
                }
            }
            foreach ($group->calls as $call) {
                $members[] = $this->prepareLocalCallback($task, $call);
            }
            $executionStarted = true;
            $receipts = PreparedLocalActivityRunner::executeGroup($members);

            return $this->refreshPreparedLocalHistory($task, end($receipts));
        } catch (\DurableWorkflow\Exception\ServerException $error) {
            if ($error->reason === 'cancellation_requested') {
                if (!$this->renewWorkflowTaskLease($taskId, $owner, $epoch) || $this->claimCancellation === null) {
                    throw new WorkflowClaimRevoked('local_admission_cancelled', 'Group admission was cancelled without a current workflow observation.');
                }
                if (!$executionStarted) {
                    foreach ($members as $member) {
                        $member['runner']->acknowledgeUnstartedCancellation();
                    }
                }
                throw new CooperativeCancellationObserved('Cancellation was accepted before the whole group could start.');
            }
            throw new WorkflowClaimRevoked($error->reason ?? 'prepared_local_group_unknown',
                'Prepared group authority or outcome is unknown. Leave the claim for durable recovery.', previous: $error);
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('invalid_prepared_local_group_receipt',
                'Prepared group authority could not be validated. Leave the claim for durable recovery.', previous: $error);
        }
    }

    /** @param array<string, mixed> $task
     * @param list<array<string, mixed>> $commands
     * @return list<array<string, mixed>>
     */
    private function checkpointPreparedLocalPrefix(array $task, array $commands, int $nextSequence, bool $scopePrefix = false, ?RequestBudget $budget = null): array
    {
        $this->assertWorkflowMemoUpdatesAvailable($commands);
        foreach ($commands as $command) {
            if (!in_array($command['type'] ?? null, ['record_side_effect', 'record_version_marker', 'upsert_memo', 'upsert_search_attributes'], true)) {
                throw new WorkflowClaimAborted('Prepared local prefix contains an unsupported retained-claim command.');
            }
        }
        $taskId = (string) $task['task_id'];
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $epoch = (int) ($task['workflow_task_attempt'] ?? 1);
        $start = $nextSequence - count($commands);
        $checkpointId = hash('sha256', json_encode([$taskId, $owner, $epoch, $start, $commands], JSON_THROW_ON_ERROR));
        $body = ['checkpoint_id' => $checkpointId, 'start_sequence' => $start, 'commands' => $commands];
        $budget = $scopePrefix ? ($budget ?? new RequestBudget(5)) : null;
        try {
            $receipt = $scopePrefix
                ? $this->client->cancellationScopeOperation($taskId, $owner, $epoch, 'checkpoint', $body, $budget)
                : $this->client->preparedLocalActivityOperation($taskId, $owner, $epoch, 'checkpoint', $body);
        } catch (ServerException $error) {
            if (!$scopePrefix || (!$error->isTransientConnectionFailure() && !$error->isTransientUpstreamFailure())) {
                throw $error;
            }
            $budget?->remainingSeconds();
            $receipt = $this->client->cancellationScopeOperation($taskId, $owner, $epoch, 'checkpoint', $body, $budget);
        }
        if (($receipt['checkpointed'] ?? null) !== true || !is_bool($receipt['duplicate'] ?? null)
            || ($receipt['checkpoint_id'] ?? null) !== $checkpointId
            || ($receipt['task_id'] ?? null) !== $taskId || ($receipt['workflow_run_id'] ?? null) !== (string) $task['run_id']
            || ($receipt['workflow_task_attempt'] ?? null) !== $epoch || ($receipt['lease_owner'] ?? null) !== $owner
            || ($receipt['start_sequence'] ?? null) !== $start || ($receipt['next_sequence'] ?? null) !== $nextSequence
            || !array_key_exists('reason', $receipt) || $receipt['reason'] !== null) {
            throw new WorkflowClaimAborted('Prepared local prefix lacks its original retained-claim receipt.');
        }

        if (!$scopePrefix) {
            return $this->refreshPreparedLocalHistory($task, $receipt);
        }
        $token = $receipt['history_refresh_page_token'] ?? null;
        if (!is_string($token) || trim($token) === '') {
            throw new WorkflowClaimAborted('Scope prefix lacks its original canonical history cursor.');
        }
        $history = $this->refreshWorkflowClaimHistory($task, $token, budget: $budget);
        $expected = [];
        foreach ($commands as $offset => $command) {
            $expected[$start + $offset] = match ($command['type']) {
                'record_side_effect' => 'SideEffectRecorded', 'record_version_marker' => 'VersionMarkerRecorded',
                'upsert_memo' => 'MemoUpserted', 'upsert_search_attributes' => 'SearchAttributesUpserted',
            };
        }
        foreach ($history as $event) {
            $sequence = $event['payload']['sequence'] ?? null;
            if (is_int($sequence) && ($expected[$sequence] ?? null) === ($event['event_type'] ?? $event['type'] ?? null)) {
                unset($expected[$sequence]);
            }
        }
        if ($expected !== []) {
            throw new WorkflowClaimAborted('Scope prefix commands are absent from the original canonical history.');
        }
        $budget?->remainingSeconds();

        return $history;
    }

    /** @param array<string, mixed> $task
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private function executePreparedLocalActivity(array $task, array $history, ReplayResult $replay): array
    {
        $call = $replay->preparedLocalActivity ?? throw new \LogicException('Missing prepared local call.');
        $taskId = (string) $task['task_id'];
        $runId = (string) $task['run_id'];
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $epoch = (int) ($task['workflow_task_attempt'] ?? 1);
        try {
            if ($replay->commands !== []) {
                return $this->checkpointPreparedLocalPrefix($task, $replay->commands, $call->sequence);
            }
            $descriptor = $call->descriptor($this->client->payloadCodec());
            if ($call->recover) {
                $original = null;
                foreach ($history as $event) {
                    if (($event['event_type'] ?? $event['type'] ?? null) === 'ActivityStarted'
                        && ($event['payload']['sequence'] ?? null) === $call->sequence) {
                        $original = $event['payload'];
                    }
                }
                $receipt = $this->client->preparedLocalActivityOperation($taskId, $owner, $epoch, 'recover', [
                    'sequence' => $call->sequence, 'descriptor' => $descriptor,
                ]);
                $kind = $receipt['event_type'] ?? null;
                if (($receipt['recovered'] ?? null) !== true || !is_bool($receipt['duplicate'] ?? null)
                    || !array_key_exists('reason', $receipt) || $receipt['reason'] !== null
                    || ($receipt['workflow_task_id'] ?? null) !== $taskId
                    || $original === null
                    || !is_string($receipt['activity_execution_id'] ?? null) || trim($receipt['activity_execution_id']) === ''
                    || !is_string($receipt['activity_attempt_id'] ?? null) || trim($receipt['activity_attempt_id']) === ''
                    || $receipt['activity_execution_id'] !== ($original['activity_execution_id'] ?? null)
                    || $receipt['activity_attempt_id'] !== ($original['activity_attempt_id'] ?? null)
                    || ($receipt['callback_stop_state'] ?? null) !== 'unknown'
                    || !in_array($kind, ['ActivityRetryScheduled', 'ActivityFailed', 'ActivityTimedOut'], true)
                    || ($receipt['claim_released'] ?? null) !== ($kind === 'ActivityRetryScheduled')
                    || !is_array($receipt['created_task_ids'] ?? null) || !array_is_list($receipt['created_task_ids'])
                    || count($receipt['created_task_ids']) !== ($kind === 'ActivityRetryScheduled' ? 1 : 0)
                    || !is_string($receipt['event_id'] ?? null) || trim($receipt['event_id']) === '') {
                    throw new WorkflowClaimAborted('Prepared local recovery lacks a canonical unknown-stop receipt.');
                }
                foreach ($receipt['created_task_ids'] as $createdTask) {
                    if (!is_string($createdTask) || trim($createdTask) === '') {
                        throw new WorkflowClaimAborted('Prepared local recovery has an invalid durable retry task identity.');
                    }
                }
                if ($receipt['claim_released']) {
                    throw new WorkflowClaimDeferred('Native scheduled the prepared local retry and released this workflow claim.');
                }
                $refreshed = $this->refreshPreparedLocalHistory($task, $receipt);
                $event = null;
                foreach ($refreshed as $candidate) {
                    if (($candidate['id'] ?? null) === $receipt['event_id']) {
                        $event = $candidate;
                        break;
                    }
                }
                if ($event === null || ($event['event_type'] ?? $event['type'] ?? null) !== $kind
                    || ($event['payload']['sequence'] ?? null) !== $call->sequence
                    || ($event['payload']['activity_execution_id'] ?? null) !== $receipt['activity_execution_id']
                    || ($event['payload']['activity_attempt_id'] ?? null) !== $receipt['activity_attempt_id']
                    || ($event['payload']['local_recovery']['workflow_task_id'] ?? null) !== $taskId
                    || ($event['payload']['local_recovery']['workflow_task_attempt'] ?? null) !== $epoch
                    || ($event['payload']['local_recovery']['lease_owner'] ?? null) !== $owner
                    || ($event['payload']['local_recovery']['callback_stop_state'] ?? null) !== 'unknown') {
                    throw new WorkflowClaimAborted('Prepared recovery is absent from this claim\'s canonical history.');
                }

                return $refreshed;
            }
            $member = $this->prepareLocalCallback($task, $call);
            $outcome = $member['runner']->execute($member['callback']);
            if ($outcome['claim_released']) {
                throw new WorkflowClaimDeferred('Native scheduled the prepared local retry and released this workflow claim.');
            }

            return $this->refreshPreparedLocalHistory($task, $outcome);
        } catch (\DurableWorkflow\Exception\ServerException $error) {
            if ($error->reason === 'cancellation_requested') {
                if (!$this->renewWorkflowTaskLease($taskId, $owner, $epoch) || $this->claimCancellation === null) {
                    throw new WorkflowClaimRevoked('local_admission_cancelled', 'Local admission was cancelled without a current workflow observation.');
                }
                throw new CooperativeCancellationObserved('Cancellation was accepted before the local callback was admitted.');
            }
            throw new WorkflowClaimRevoked($error->reason ?? 'prepared_local_outcome_unknown',
                'Prepared local operation has an unknown or refused outcome. Leave its original claim for durable recovery.', previous: $error);
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('invalid_prepared_local_receipt',
                'Prepared local authority could not be validated. Leave its original claim for durable recovery.', previous: $error);
        }
    }

    /** @param array<string, mixed> $task
     * @param array<string, mixed> $receipt
     * @return list<array<string, mixed>>
     */
    private function refreshPreparedLocalHistory(array $task, array $receipt): array
    {
        $token = $receipt['history_refresh_page_token'] ?? null;
        if (!is_string($token) || trim($token) === '') {
            throw new WorkflowClaimAborted('Prepared local operation lacks a Server-issued canonical history cursor.');
        }

        return $this->refreshWorkflowClaimHistory($task, $token);
    }

    /** @param array<string, mixed> $task
     * @return array{runner: PreparedLocalActivityRunner, callback: \Closure}
     */
    private function prepareLocalCallback(array $task, PreparedLocalActivityCall $call): array
    {
        $taskId = (string) $task['task_id'];
        $runId = (string) $task['run_id'];
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $epoch = (int) ($task['workflow_task_attempt'] ?? 1);
        $descriptor = $call->descriptor($this->client->payloadCodec());
        $nonce = hash('sha256', json_encode([$taskId, $runId, $owner, $epoch, $call->sequence], JSON_THROW_ON_ERROR));
        $started = hrtime(true) / 1e9;
        $receipt = $this->client->preparedLocalActivityOperation($taskId, $owner, $epoch, 'prepare', [
            'sequence' => $call->sequence, 'worker_attempt_id' => $nonce, 'descriptor' => $descriptor,
        ]);
        $attempt = PreparedLocalActivityAttempt::fromPreparation($receipt, $taskId, $runId, $owner, $epoch, $nonce,
            $call->command->attributes['heartbeat_timeout'] ?? null, $call->cleanupSnapshot(),
            $call->command->attributes['cancellation_scope_id'] ?? 'root');
        $activityType = (string) $call->command->attributes['activity_type'];
        $handler = $this->activities[$activityType] ?? null;
        $arguments = $call->command->attributes['arguments_value'];
        $runner = new PreparedLocalActivityRunner($this->client, $attempt, $receipt, $started,
            fn (): bool => $this->shutdownRequested,
            function (array $observation): void { $this->observeClaimCancellation($observation); },
            function (int $relay, int $callback) use ($taskId, $attempt, $activityType): void {
                $this->diagnostic('worker.activity_process_started', [
                    'task_id' => $taskId, 'activity_execution_id' => $attempt->executionId,
                    'activity_attempt_id' => $attempt->attemptId, 'activity_type' => $activityType,
                    'relay_pid' => $relay, 'callback_pid' => $callback, 'local' => true, 'prepared' => true,
                ]);
            },
            function (RequestBudget $budget): void { $this->heartbeatIfDue($budget); },
        );
        $callback = function (\Closure $heartbeat) use ($handler, $attempt, $activityType, $arguments, $task): mixed {
            if ($handler === null) {
                throw new InvalidLocalActivityReport('No local activity handler is registered for '.$activityType.'.');
            }
            return $this->invokeActivity($handler, new ActivityContext($this->client, $attempt->taskId, $attempt->attemptId,
                $attempt->leaseOwner, $activityType, $attempt->attemptNumber, localHeartbeat: $heartbeat), $arguments,
                ['activity_id' => $attempt->executionId] + ExecutionTelemetry::taskFields($task, $this->client->namespace, $this->taskQueue), true);
        };

        return ['runner' => $runner, 'callback' => $callback];
    }

    /** @param array<string, mixed> $task */
    private function deliverCancellationOrDeferClaim(array $task, CancellationDelivery $intent): void
    {
        $taskId = (string) $task['task_id'];
        $owner = (string) ($task['lease_owner'] ?? $this->workerId);
        $attempt = (int) ($task['workflow_task_attempt'] ?? 1);
        if ($this->shutdownRequested) {
            throw new WorkflowClaimRevoked('worker_shutdown', 'Worker stopped before cancellation delivery.');
        }
        $this->assertCancellationDeadline();
        try {
            $response = $this->client->deliverWorkflowCancellation($taskId, $owner, $attempt, $intent);
        } catch (Throwable $error) {
            if ($this->isTaskAuthorityLost('workflow', $taskId, $error, $attempt)) {
                throw new WorkflowClaimRevoked('terminal_task_fence', 'Workflow claim has closed.', previous: $error);
            }
            throw $error;
        }
        $this->assertCancellationDeadline();
        if (($response['delivered'] ?? null) === false) {
            // Client validation requires an explicit acknowledgement of claim release.
            throw new WorkflowClaimDeferred('Server released this claim until cancellation acknowledgments resolve.');
        }
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

    /** @param list<array<string, mixed>> $commands */
    private function assertServiceOperationsAvailable(array $commands): void
    {
        foreach ($commands as $command) {
            if (($command['type'] ?? null) === 'start_service_operation' && !$this->serviceOperationsSupported) {
                throw new WorkflowClaimAborted(
                    'service_operations_unavailable: the connected runtime did not advertise start_service_operation support.',
                );
            }
        }
    }

    /** @param list<array<string, mixed>> $commands */
    private function assertCancellationPoliciesAvailable(array $commands): void
    {
        if ($this->enableCooperativeCancellation) {
            return;
        }
        foreach ($commands as $command) {
            if (($command['type'] ?? null) === 'schedule_activity' && array_key_exists('cancellation_policy', $command)) {
                throw new WorkflowClaimAborted(
                    "activity_cancellation_policy_not_supported: PHP worker {$this->workerId} must enable cooperative cancellation with worker protocol "
                    .Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL.' and a compatible Server/Native backend.',
                );
            }
            if (($command['type'] ?? null) === 'start_child_workflow'
                && (($command['parent_close_policy'] ?? null) === 'request_cancellation'
                    || in_array($command['cancellation_policy'] ?? null, ['try_cancel', 'wait_cancellation_completed'], true))) {
                throw new WorkflowClaimAborted(
                    "child_cancellation_policy_not_supported: PHP worker {$this->workerId} must enable cooperative cancellation with worker protocol "
                    .Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL.' and a compatible Server/Native backend.',
                );
            }
        }
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
        $callbackStopped = false;
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
                heartbeatRequest: fn (array $details): array => $this->retryStorageAdmission(
                    'activity_heartbeat',
                    fn (): array => $this->client->heartbeatActivityTask($taskId, $attemptId, $leaseOwner, $details),
                ),
            );
            $arguments = $this->decodeArguments($task['arguments'] ?? null);
            $this->activityLeaseProcess?->check();
            if ($this->enableCooperativeCancellation) {
                $nextObservation = 0.0;
                $check = function (bool $force) use ($task, &$nextObservation): void {
                    if ($this->shutdownRequested) {
                        throw new WorkflowClaimAborted('Worker shutdown abandoned its remote activity claim.');
                    }
                    if ($force || hrtime(true) / 1e9 >= $nextObservation) {
                        $this->assertRemoteActivityClaimActive($task);
                        $nextObservation = hrtime(true) / 1e9 + 1;
                    }
                };
                $result = (new CooperativeActivityExecutor($this->client->payloadCodec()))->execute(
                    function (\Closure $heartbeat) use ($handler, $taskId, $attemptId, $leaseOwner, $activityType, $task, $arguments): mixed {
                        $callbackContext = new ActivityContext($this->client, $taskId, $attemptId, $leaseOwner,
                            $activityType, (int) ($task['attempt_number'] ?? 1), localHeartbeat: $heartbeat);

                        return $this->invokeActivity($handler, $callbackContext, $arguments,
                            ExecutionTelemetry::taskFields($task, $this->client->namespace, $this->taskQueue), authorizedTask: $task);
                    },
                    function (array $details) use ($taskId, $attemptId, $leaseOwner): array {
                        try {
                            $reply = $this->client->heartbeatActivityTask($taskId, $attemptId, $leaseOwner, $details);
                            if (($reply['task_id'] ?? null) !== $taskId
                                || ($reply['activity_attempt_id'] ?? null) !== $attemptId
                                || ($reply['lease_owner'] ?? null) !== $leaseOwner
                                || ($reply['can_continue'] ?? null) !== true
                                || ($reply['cancel_requested'] ?? null) !== false) {
                                throw new WorkflowClaimAborted('Remote activity heartbeat lost its ownership fence.');
                            }

                            return $reply;
                        } catch (Throwable $error) {
                            throw new WorkflowClaimAborted('Remote activity user heartbeat failed.', previous: $error);
                        }
                    },
                    $check,
                    function (int $relay, int $callback) use ($taskId, $attemptId, $activityType): void {
                        $this->diagnostic('worker.activity_process_started', [
                            'task_id' => $taskId, 'activity_attempt_id' => $attemptId, 'activity_type' => $activityType,
                            'relay_pid' => $relay, 'callback_pid' => $callback, 'local' => false,
                        ]);
                    },
                    static function () use (&$callbackStopped): void { $callbackStopped = true; },
                );
                $check(true);
            } else {
                $result = $this->invokeActivity($handler, $context, $arguments,
                    ExecutionTelemetry::taskFields($task, $this->client->namespace, $this->taskQueue), authorizedTask: $task);
            }
            $this->retryStorageAdmission(
                'activity_complete',
                fn (): array => $this->client->completeActivityTask($taskId, $attemptId, $leaseOwner, $result),
                activityTaskLease: [
                    'task_id' => $taskId,
                    'activity_attempt_id' => $attemptId,
                    'lease_owner' => $leaseOwner,
                ],
            );
        } catch (Throwable $exception) {
            if ($exception instanceof ServerException && $this->discardClosedActivityAttempt($task, $exception)) {
                return;
            }
            if ($this->activityLeaseProcess !== null && !$exception instanceof WorkflowClaimRevoked) {
                try {
                    $this->activityLeaseProcess->check();
                } catch (WorkflowClaimRevoked $revoked) {
                    $exception = $revoked;
                }
            }
            if ($exception instanceof WorkflowClaimRevoked
                || ($this->enableCooperativeCancellation && $exception instanceof WorkflowClaimAborted)) {
                if ($callbackStopped) {
                    $this->acknowledgeStoppedRemoteActivity($taskId, $attemptId, $leaseOwner);
                }
                $this->diagnostic('worker.claim_aborted', [
                    'task_id' => $taskId, 'activity_attempt_id' => $attemptId,
                    'reason' => $exception instanceof WorkflowClaimRevoked ? $exception->reason : null,
                    'task_kind' => 'activity', 'message' => $exception->getMessage(),
                ], 'warning');

                return;
            }
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
                        $failure instanceof ActivityExecutionFailure ? $failure->originalType : $failure::class,
                        $failure instanceof ActivityCancelled || ($failure instanceof ActivityExecutionFailure && $failure->cancelled),
                    );
                },
            );
        }
    }

    /** This is reached only after the isolated callback stopped and its relay joined. */
    private function acknowledgeStoppedRemoteActivity(string $taskId, string $attemptId, string $leaseOwner): void
    {
        try {
            // Discovery and every retry share one bounded report budget. This
            // observation grants no callback or workflow execution authority.
            $reportDeadline = hrtime(true) / 1e9 + 5;
            $budget = new RequestBudget(5, $reportDeadline);
            $status = $this->retryStoppedRemoteActivityReceiptRequest(
                fn (): array => $this->client->activityTaskStatus($taskId, $attemptId, $leaseOwner, $budget), $budget,
            );
            $receipt = $status['cancellation_acknowledgement'] ?? null;
            if (($status['task_id'] ?? null) !== $taskId
                || ($status['activity_attempt_id'] ?? null) !== $attemptId
                || ($status['lease_owner'] ?? null) !== $leaseOwner) {
                throw new WorkflowClaimAborted('Remote callback-stop observation changed its original claim identity.');
            }
            if (($status['cancel_requested'] ?? null) !== true) {
                $this->diagnostic('worker.activity_cancellation_acknowledgement_skipped', [
                    'task_id' => $taskId, 'activity_attempt_id' => $attemptId,
                    'callback_stopped' => true, 'reason' => 'cancellation_not_observed',
                ]);

                return;
            }
            if (($status['can_continue'] ?? null) !== false || ($status['heartbeat_recorded'] ?? null) !== false
                || !is_array($receipt)
                || !in_array($receipt['callback_state'] ?? null, ['unknown', 'stopped'], true)) {
                throw new WorkflowClaimAborted('Remote callback stopped without canonical cancellation acknowledgement proof.');
            }
            foreach (['request_id', 'root_request_id', 'cleanup_deadline_at', 'cancellation_history_event_id'] as $field) {
                if (!is_string($receipt[$field] ?? null) || trim($receipt[$field]) === '') {
                    throw new WorkflowClaimAborted('Remote callback-stop proof omitted its original '.$field.'.');
                }
            }
            if (preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $receipt['cleanup_deadline_at']) !== 1) {
                throw new WorkflowClaimAborted('Remote stop receipt has an invalid original cleanup deadline.');
            }
            $cleanupDeadline = new \DateTimeImmutable($receipt['cleanup_deadline_at']);
            $parseErrors = \DateTimeImmutable::getLastErrors();
            if ($parseErrors !== false && ($parseErrors['warning_count'] !== 0 || $parseErrors['error_count'] !== 0)) {
                throw new WorkflowClaimAborted('Remote stop receipt has an invalid original cleanup deadline.');
            }
            $reportDeadline = min($reportDeadline,
                hrtime(true) / 1e9 + (float) $cleanupDeadline->format('U.u') - microtime(true));
            $budget = new RequestBudget(5, $reportDeadline);
            // Never refresh the captured identity or deadline after an uncertain
            // response. Native's duplicate receipt retains the first history ID.
            $reply = $this->retryStoppedRemoteActivityReceiptRequest(
                fn (): array => $this->client->acknowledgeActivityCancellation($taskId, $attemptId, $leaseOwner, $receipt['request_id'], $budget),
                $budget,
            );
            if (($reply['task_id'] ?? null) !== $taskId
                || ($reply['activity_attempt_id'] ?? null) !== $attemptId
                || ($reply['lease_owner'] ?? null) !== $leaseOwner
                || ($reply['request_id'] ?? null) !== $receipt['request_id']
                || ($reply['acknowledged'] ?? null) !== true
                || !is_bool($reply['duplicate'] ?? null)
                || ($reply['reason'] ?? null) !== null
                || ($reply['heartbeat_recorded'] ?? null) !== false
                || !is_string($reply['history_event_id'] ?? null) || $reply['history_event_id'] === '') {
                throw new WorkflowClaimAborted('Remote callback-stop acknowledgment was not proved by the Server.');
            }
            $this->diagnostic('worker.activity_cancellation_acknowledged', [
                'task_id' => $taskId, 'activity_attempt_id' => $attemptId,
                'request_id' => $receipt['request_id'], 'root_request_id' => $receipt['root_request_id'],
                'cleanup_deadline_at' => $receipt['cleanup_deadline_at'],
                'callback_stopped' => true,
                'history_event_id' => $reply['history_event_id'], 'duplicate' => $reply['duplicate'],
            ]);
        } catch (Throwable $error) {
            // Stop is complete even if storage or transport cannot retain its report.
            // Never turn a receipt refusal into publication or a fresh cleanup budget.
            $this->diagnostic('worker.activity_cancellation_acknowledgement_failed', [
                'task_id' => $taskId, 'activity_attempt_id' => $attemptId,
                'callback_stopped' => true,
                'message' => $error->getMessage(),
            ], 'warning');
        }
    }

    /** @param \Closure(): array<string, mixed> $request
     * @return array<string, mixed>
     */
    private function retryStoppedRemoteActivityReceiptRequest(\Closure $request, RequestBudget $budget): array
    {
        for ($attempt = 0; ; ++$attempt) {
            $budget->remainingSeconds();
            try {
                return $request();
            } catch (ServerException $error) {
                // A protocol refusal or malformed proof is final. Only known
                // transient transport failures can retry the same stop report.
                if ($attempt >= 2 || (!$error->isTransientConnectionFailure() && !$error->isTransientUpstreamFailure())) {
                    throw $error;
                }
                usleep(100_000 * ($attempt + 1));
            }
        }
    }

    /** @param array<string, mixed> $task */
    private function assertRemoteActivityClaimActive(array $task, ?RequestBudget $budget = null): void
    {
        try {
            $reply = $this->client->activityTaskStatus((string) $task['task_id'],
                (string) ($task['activity_attempt_id'] ?? $task['attempt_id'] ?? ''),
                (string) ($task['lease_owner'] ?? $this->workerId), $budget);
            if (($reply['task_id'] ?? null) !== $task['task_id']
                || ($reply['activity_attempt_id'] ?? null) !== ($task['activity_attempt_id'] ?? $task['attempt_id'] ?? '')
                || ($reply['lease_owner'] ?? null) !== ($task['lease_owner'] ?? $this->workerId)
                || ($reply['can_continue'] ?? null) !== true
                || ($reply['cancel_requested'] ?? null) !== false
                || ($reply['heartbeat_recorded'] ?? null) !== false
                || ($reply['reason'] ?? null) !== null
                || !is_string($reply['lease_expires_at'] ?? null)) {
                throw new WorkflowClaimAborted('Remote activity observation refused its ownership fence.');
            }
            $bounds = [$reply['lease_expires_at']];
            if ((isset($reply['deadlines']) && !is_array($reply['deadlines']))
                || (isset($reply['worker_session']) && !is_array($reply['worker_session']))) {
                throw new WorkflowClaimAborted('Remote activity observation returned malformed ownership metadata.');
            }
            foreach (['heartbeat', 'start_to_close', 'schedule_to_close'] as $kind) {
                if (isset($reply['deadlines'][$kind])) {
                    $bounds[] = $reply['deadlines'][$kind];
                }
            }
            if (isset($reply['worker_session'])) {
                if (($reply['worker_session']['status'] ?? null) !== 'active'
                    || ($reply['worker_session']['lease_owner'] ?? null) !== ($task['lease_owner'] ?? $this->workerId)) {
                    throw new WorkflowClaimAborted('Remote activity observation lost its required worker session.');
                }
                $bounds[] = $reply['worker_session']['lease_expires_at'] ?? null;
                $bounds[] = $reply['worker_session']['ttl_expires_at'] ?? null;
            }
            $timestamps = [];
            foreach ($bounds as $bound) {
                if (!is_string($bound) || preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/D', $bound) !== 1) {
                    throw new WorkflowClaimAborted('Remote activity observation returned an invalid deadline.');
                }
                $timestamps[] = (float) (new \DateTimeImmutable($bound))->format('U.u');
            }
            if ($this->now() >= min($timestamps)) {
                throw new WorkflowClaimAborted('Remote activity ownership or execution deadline elapsed.');
            }
            $this->heartbeatIfDue($budget);
            if ($this->shutdownRequested) {
                throw new WorkflowClaimAborted('Worker shutdown abandoned its remote activity claim.');
            }
            if ($this->now() >= min($timestamps)) {
                throw new WorkflowClaimAborted('Remote activity ownership or execution deadline elapsed.');
            }
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new WorkflowClaimAborted('Remote activity ownership observation failed.', previous: $error);
        }
    }

    /** @param array<string, mixed> $task */
    private function executeQueryTask(array $task): void
    {
        $traceScope = null;
        $taskId = (string) ($task['query_task_id'] ?? $task['task_id'] ?? '');
        $attempt = (int) ($task['query_task_attempt'] ?? 1);
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
        $authority = $this->queryTaskAuthority;
        try {
            if ($authority === null) {
                $this->queryTaskAuthority = $authority = $this->queryAuthorityForTask($task);
            } elseif (!$authority->matches($task)) {
                throw new WorkflowClaimRevoked('query_claim_changed', 'Loaded query identity differs from its original claim.');
            }
            $authority?->confirm();
            if ($this->client->observabilityContextAvailable()) {
                $traceScope = $this->telemetry?->enterTask('query', $task, $this->client->namespace, $this->taskQueue);
            }
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
            if ($this->shutdownRequested) {
                throw new WorkflowClaimRevoked('worker_stopped', 'Worker stopped before query dispatch.');
            }
            $authority?->confirm();
            $result = $handler($context, ...$arguments);
            $authority?->confirm();
            $this->retryStorageAdmission('query_complete', fn (): array =>
                $this->client->completeQueryTask($taskId, $leaseOwner, $attempt, $result, $this->queryRequestBudget($authority)));
        } catch (WorkflowClaimRevoked $exception) {
            $this->diagnostic('worker.claim_aborted', ['task_kind' => 'query', 'task_id' => $taskId,
                'reason' => $exception->reason, 'message' => $exception->getMessage()], 'warning');
        } catch (Throwable $exception) {
            try {
                $authority?->confirm();
            } catch (WorkflowClaimRevoked $lost) {
                $this->diagnostic('worker.claim_aborted', ['task_kind' => 'query', 'task_id' => $taskId,
                    'reason' => $lost->reason, 'message' => $lost->getMessage()], 'warning');
                return;
            }
            $this->acknowledgeTaskFailure(
                'query',
                $taskId,
                $exception,
                function (Throwable $failure) use ($taskId, $leaseOwner, $attempt, $authority): void {
                    $this->client->failQueryTask($taskId, $leaseOwner, $attempt, $failure->getMessage(),
                        budget: $this->queryRequestBudget($authority));
                },
            );
        } finally {
            $traceScope?->__invoke();
            $this->queryTaskAuthority = null;
        }
    }

    /** @param array<string, mixed> $task */
    private function queryAuthorityForTask(array $task): ?QueryTaskAuthority
    {
        if ($this->requireQueryTaskAuthority && !$this->queryTaskAuthoritySupported) {
            throw new WorkflowClaimRevoked('query_authority_unavailable', 'This Worker requires runtime query authority.');
        }
        if (!$this->queryTaskAuthoritySupported && !array_key_exists('lease_expires_at', $task)
            && !array_key_exists('query_deadline_at', $task) && !array_key_exists('query_admitted_at', $task)) {
            // Older deliveries disclose no original timing. They retain legacy
            // behavior and cannot provide the strong query-authority guarantee.
            return null;
        }

        return new QueryTaskAuthority($this->client, $task, $this->workerId, $this->taskQueue,
            $this->queryTaskAuthoritySupported);
    }

    private function queryRequestBudget(?QueryTaskAuthority $authority): ?RequestBudget
    {
        return $authority !== null && $this->client->supportsBoundedWorkerRequests() ? $authority->budget() : null;
    }

    /** @param callable(Throwable): void $failureAcknowledgement */
    private function acknowledgeTaskFailure(
        string $taskKind,
        string $taskId,
        Throwable $taskFailure,
        callable $failureAcknowledgement,
        ?int $workflowTaskAttempt = null,
    ): void {
        if ($this->isTaskAuthorityLost($taskKind, $taskId, $taskFailure, $workflowTaskAttempt)) {
            if ($taskKind === 'workflow' && $taskFailure instanceof ServerException
                && ($taskFailure->details['task_status'] ?? null) === 'ready') {
                $this->diagnostic('worker.claim_aborted', ['task_id' => $taskId, 'task_kind' => $taskKind,
                    'workflow_task_attempt' => $workflowTaskAttempt, 'reason' => 'task_not_leased'], 'warning');
            }
            return;
        }
        $this->handlerFailure($taskKind, $taskId, $taskFailure);
        if ($taskFailure instanceof ServerException) {
            throw $taskFailure;
        }

        try {
            $this->retryStorageAdmission("{$taskKind}_fail", static function () use ($failureAcknowledgement, $taskFailure): void {
                $failureAcknowledgement($taskFailure);
            });
        } catch (Throwable $acknowledgementFailure) {
            if (!$this->isTaskAuthorityLost($taskKind, $taskId, $acknowledgementFailure, $workflowTaskAttempt)) {
                throw $acknowledgementFailure;
            }
        }
    }

    private function isTaskAuthorityLost(string $taskKind, string $taskId, Throwable $exception, ?int $workflowTaskAttempt = null): bool
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
                    && in_array($details['task_status'] ?? null, ['cancelled', 'completed'], true))
                || ($reason === 'task_not_leased'
                    && (in_array($details['task_status'] ?? null, ['cancelled', 'completed'], true)
                        || (($details['task_status'] ?? null) === 'ready' && $workflowTaskAttempt !== null
                            && ($details['workflow_task_attempt'] ?? null) === $workflowTaskAttempt)))
                || ($reason === 'lease_expired' && $workflowTaskAttempt !== null
                    && ($details['workflow_task_attempt'] ?? null) === $workflowTaskAttempt
                    && ($details['task_status'] ?? null) === 'leased'
                    && ($details['lease_owner'] ?? null) === $this->workerId),
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

        if (!StickyWorkflowCache::startsWithWorkflowStart($history)) {
            throw new \RuntimeException(
                'Authoritative workflow history must contain its start prefix after a sticky cache miss.',
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
                if ($this->enableCooperativeCancellation) {
                    $this->assertLocalWorkflowClaimActive($task);
                }
                if ($this->shutdownRequested || ($this->claimCancellation === null && (bool) ($task['cancel_requested'] ?? false))) {
                    throw new ActivityCancelled('The workflow requested local activity cancellation.');
                }
                $localHeartbeat = function (array $details) use (
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
                    if ($this->enableCooperativeCancellation) {
                        $this->assertLocalWorkflowClaimActive($task, renew: false);
                    }
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
                };
                $callback = fn (\Closure $heartbeat): mixed => $this->invokeActivity($handler, new ActivityContext(
                    $this->client, (string) ($task['task_id'] ?? ''), $attemptId,
                    (string) ($task['lease_owner'] ?? $this->workerId), $activityType, $attemptNumber,
                    localHeartbeat: $heartbeat,
                ), $arguments, ['activity_id' => null] + ExecutionTelemetry::taskFields($task, $this->client->namespace, $this->taskQueue), true);
                if ($this->enableCooperativeCancellation) {
                    $nextRenewal = 0.0;
                    $check = function (bool $force) use ($task, &$nextRenewal, $attemptStartedAt,
                        $executionStartedAt, $startToClose, $scheduleToClose, $heartbeatTimeout, &$lastHeartbeatAt): void {
                        $renew = $force || hrtime(true) / 1e9 >= $nextRenewal;
                        $this->assertLocalWorkflowClaimActive($task, renew: $renew);
                        if ($renew) {
                            $nextRenewal = hrtime(true) / 1e9 + 1;
                        }
                        $now = $this->now();
                        if ($heartbeatTimeout !== null && $now - $lastHeartbeatAt > $heartbeatTimeout) {
                            throw new LocalActivityTimedOut('heartbeat', 'Local activity heartbeat timeout elapsed.');
                        }
                        if ($startToClose !== null && $now - $attemptStartedAt > $startToClose) {
                            throw new LocalActivityTimedOut('start_to_close', 'Local activity start-to-close timeout elapsed.');
                        }
                        if ($scheduleToClose !== null && $now - $executionStartedAt > $scheduleToClose) {
                            throw new LocalActivityTimedOut('schedule_to_close', 'Local activity schedule-to-close timeout elapsed.');
                        }
                        try {
                            $this->heartbeatIfDue();
                        } catch (Throwable $error) {
                            throw new WorkflowClaimAborted('Worker registration heartbeat failed during local execution.', previous: $error);
                        }
                    };
                    $result = (new CooperativeActivityExecutor($this->client->payloadCodec()))->execute(
                        $callback,
                        static function (array $details) use ($localHeartbeat): mixed {
                            $localHeartbeat($details);
                            return null;
                        },
                        $check,
                        function (int $relay, int $callback) use ($task, $attemptId, $activityType): void {
                            $this->diagnostic('worker.activity_process_started', [
                                'task_id' => $task['task_id'] ?? '', 'activity_attempt_id' => $attemptId,
                                'activity_type' => $activityType,
                                'relay_pid' => $relay, 'callback_pid' => $callback, 'local' => true,
                            ]);
                        },
                    );
                } else {
                    $result = $callback($localHeartbeat);
                }
                if ($this->enableCooperativeCancellation) {
                    // Check the lease and request before the result can be encoded or reported.
                    $this->assertLocalWorkflowClaimActive($task);
                }
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
                if ($exception instanceof WorkflowClaimAborted) {
                    throw $exception;
                }
                if ($exception instanceof ServerException && $exception->isStorageAdmissionFailure()) {
                    throw $exception;
                }
                if ($exception instanceof ActivityExecutionFailure && $exception->storageAdmissionFailure) {
                    throw new WorkflowClaimAborted('Activity callback received a storage admission refusal.', previous: $exception);
                }
                $timeoutKind = $exception instanceof LocalActivityTimedOut ? $exception->timeoutKind
                    : ($exception instanceof ActivityExecutionFailure ? $exception->timeoutKind : null);
                $timedOut = $timeoutKind !== null;
                $cancelled = $exception instanceof ActivityCancelled
                    || ($exception instanceof ActivityExecutionFailure && $exception->cancelled);
                $invalidExecutionReport = $exception instanceof ActivityExecutionFailure
                    && ($exception->invalidReport || $exception->duringEncoding);
                $type = $invalidExecutionReport ? InvalidLocalActivityReport::class
                    : ($exception instanceof ActivityExecutionFailure ? $exception->originalType : $exception::class);
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
                $invalidReport = $exception instanceof InvalidLocalActivityReport || $invalidFailureMetadata || $invalidExecutionReport;
                $isNonRetryable = $cancelled || $invalidReport || in_array($type, $nonRetryable, true)
                    || in_array(substr($type, (int) strrpos('\\'.$type, '\\')), $nonRetryable, true);
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
                        if ($this->enableCooperativeCancellation) {
                            $until = hrtime(true) / 1e9 + $backoffSeconds;
                            while (hrtime(true) / 1e9 < $until) {
                                $this->assertLocalWorkflowClaimActive($task);
                                usleep((int) min(1_000_000, max(1, ($until - hrtime(true) / 1e9) * 1_000_000)));
                            }
                        } else {
                            ($this->sleeper)($backoffSeconds * 1_000_000);
                        }
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

    /** @param array<string, mixed> $task */
    private function assertLocalWorkflowClaimActive(array $task, bool $renew = true): void
    {
        if ($this->shutdownRequested) {
            throw new WorkflowClaimRevoked('worker_shutdown', 'Worker shutdown abandoned its local workflow claim.');
        }
        $this->assertCancellationDeadline();
        if ($renew) {
            try {
                if (!$this->renewWorkflowTaskLease((string) $task['task_id'],
                    (string) ($task['lease_owner'] ?? $this->workerId), (int) ($task['workflow_task_attempt'] ?? 1))) {
                    throw new WorkflowClaimRevoked('worker_shutdown', 'Local workflow claim was not renewed.');
                }
            } catch (WorkflowClaimAborted $error) {
                throw $error;
            } catch (Throwable $error) {
                if ($this->isTaskAuthorityLost('workflow', (string) $task['task_id'], $error, (int) ($task['workflow_task_attempt'] ?? 1))) {
                    throw new WorkflowClaimRevoked('terminal_task_fence', 'Local workflow claim has closed.', previous: $error);
                }
                throw new WorkflowClaimAborted('Local workflow claim renewal failed.', previous: $error);
            }
        }
        $this->assertCancellationDeadline();
        if ($this->claimCancellation !== null
            && $this->claimDeliveredCancellationId !== $this->claimCancellation->requestId) {
            throw new CooperativeCancellationObserved('Cooperative request observed on the actual task heartbeat.');
        }
    }

    private function assertCancellationDeadline(): void
    {
        if ($this->claimCancellation !== null
            && $this->now() >= (float) (new \DateTimeImmutable($this->claimCancellation->cleanupDeadlineAt))->format('U.u')) {
            throw new WorkflowClaimRevoked('cleanup_deadline_expired', 'The original cooperative cleanup deadline elapsed.');
        }
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

    /** @phpstan-impure */
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

        // An idle worker must refresh its existing registration before another
        // acquisition can race ahead of a failed heartbeat's retry deadline.
        // This does not run while a task is already being executed, and the
        // heartbeat API retains the Server's fencing and rollout decisions.
        while ($this->heartbeatRetryAttempt > 0 && !$this->shutdownRequested) {
            $this->waitForTransientRetry(max(0.0, $this->heartbeatRetryAt - $this->now()));
            if ($this->heartbeatRetryAttempt > 0 && !$this->shutdownRequested) {
                $this->heartbeat();
            }
        }

        return min($timeoutSeconds, max(1, $this->heartbeatIntervalSeconds - 1));
    }

    private function heartbeatIfDue(?RequestBudget $budget = null): void
    {
        if (!$this->registered || $this->shutdownRequested || $this->activityLeaseProcess !== null) {
            return;
        }

        if ($this->elapsedSinceHeartbeat() < $this->heartbeatIntervalSeconds) {
            return;
        }

        $this->heartbeat($budget);
    }

    private function stopActivityLease(): void
    {
        if ($this->unbindActivityLease !== null) {
            ($this->unbindActivityLease)();
            $this->unbindActivityLease = null;
        }
        $this->activityLeaseProcess?->stop();
        $this->activityLeaseProcess = null;
    }

    private function heartbeat(?RequestBudget $budget = null): void
    {
        if ($this->shutdownRequested || $this->now() < $this->heartbeatRetryAt) {
            return;
        }

        try {
            $heartbeatClient = $budget === null ? $this->client : $this->client->withBoundedWorkerRequests();
            $acknowledgement = $heartbeatClient->heartbeatWorker($this->workerId, [
                'workflow_available' => 1,
                'activity_available' => 1,
            ], $budget);
        } catch (ServerException $exception) {
            if (!$exception->isTransientConnectionFailure()
                && !$exception->isTransientUpstreamFailure()
                && !$this->isTransientDatabaseFailure($exception, 'heartbeat_worker')
                && !$exception->isStorageAdmissionFailure()) {
                throw $exception;
            }

            // Do not enter the heartbeat-aware wait recursively or delay an
            // already leased task. The next poll/wait services this deadline.
            $attempt = ++$this->heartbeatRetryAttempt;
            $delaySeconds = $this->transientRetryDelay($attempt, $exception->details['retry_after_seconds'] ?? null);
            $this->heartbeatRetryAt = $this->now() + $delaySeconds;
            if ($this->transientPollRetryObserver !== null) {
                ($this->transientPollRetryObserver)('heartbeat', $attempt, $delaySeconds, $exception);
            }
            $this->diagnostic('worker.retrying', [
                'worker_id' => $this->workerId,
                'operation' => 'heartbeat',
                'attempt' => $attempt,
                'delay_seconds' => $delaySeconds,
                'exception' => $exception,
            ], 'warning');

            return;
        }

        $this->applyHeartbeatInterval($acknowledgement);
        $this->lastHeartbeatAt = $this->now();
        $this->heartbeatRetryAt = 0.0;
        $this->heartbeatRetryAttempt = 0;
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
     * Restore string declaration names after PHP coerces numeric dictionary keys.
     *
     * @param array<array-key, mixed> $declarations
     * @return list<string>
     */
    private function declarationNames(array $declarations): array
    {
        return array_map('strval', array_keys($declarations));
    }

    /**
     * @return array<array-key, array{
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
        foreach ($this->declarationNames($this->workflows) as $workflowType) {
            $queries = $this->queries[$workflowType] ?? [];
            $signals = $this->signals[$workflowType] ?? [];
            $updates = $this->updates[$workflowType] ?? [];

            $contracts[$workflowType] = [
                'queries' => $this->declarationNames($queries),
                'query_contracts' => $this->commandHandlerContracts($queries, QueryContext::class),
                'signals' => $this->declarationNames($signals),
                'signal_contracts' => $this->commandHandlerContracts($signals),
                'updates' => $this->declarationNames($updates),
                'update_contracts' => $this->commandHandlerContracts($updates, QueryContext::class),
                'update_validators' => [],
            ];
        }

        return $contracts;
    }

    /** @return array<array-key, string> */
    private function workflowDefinitionFingerprints(): array
    {
        $fingerprints = [];

        foreach ($this->workflows as $workflowType => $handler) {
            $fingerprint = WorkflowDefinitionFingerprint::forHandler((string) $workflowType, $handler);

            if ($fingerprint !== null) {
                $fingerprints[$workflowType] = $fingerprint;
            }
        }

        return $fingerprints;
    }

    /**
     * @param array<array-key, HandlerDefinition|callable> $handlers
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
                'name' => (string) $name,
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
                    $this->assertUnique($workflows, (string) $name, 'workflow');
                    $workflows[$name] = $handler;
                }
                foreach ($discovery->activities as $name => $handler) {
                    $this->assertUnique($activities, (string) $name, 'activity');
                    $activities[$name] = $handler;
                }
                foreach ($discovery->queries as $workflowType => $handlers) {
                    $queries[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertUnique($queries[$workflowType], (string) $name, 'query');
                        $queries[$workflowType][$name] = $handler;
                    }
                }
                foreach ($discovery->signals as $workflowType => $handlers) {
                    $signals[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertSignalNameIsNotRuntimeReserved((string) $name);
                        $this->assertUnique($signals[$workflowType], (string) $name, 'signal');
                        $signals[$workflowType][$name] = $handler;
                    }
                }
                foreach ($discovery->updates as $workflowType => $handlers) {
                    $updates[$workflowType] ??= [];
                    foreach ($handlers as $name => $handler) {
                        $this->assertUnique($updates[$workflowType], (string) $name, 'update');
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
        ExecutionTelemetry::failure($exception);
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

    /** @param callable(ActivityContext, mixed ...$arguments): mixed $handler
     *  @param list<mixed> $arguments
     *  @param array<string, mixed> $fields
     *  @param array<string, mixed>|null $authorizedTask
     */
    private function invokeActivity(callable $handler, ActivityContext $context, array $arguments, array $fields, bool $local = false, ?array $authorizedTask = null): mixed
    {
        if ($this->telemetry === null) {
            return $handler($context, ...$arguments);
        }
        $fields['activity_type'] = $context->activityType;
        $fields['activity_attempt_id'] = $context->activityAttemptId;
        $fields['activity_attempt_number'] = $context->attemptNumber;

        $scope = !$local && $authorizedTask !== null && $this->client->observabilityContextAvailable()
            ? $this->telemetry->enterTask('activity', $authorizedTask, $this->client->namespace, $this->taskQueue) : null;
        try {
            return $this->telemetry->activity($fields, fn () => $handler($context, ...$arguments), $local);
        } finally {
            $scope?->__invoke();
        }
    }

    /** @return array<string, mixed> */
    private function workflowFailureCommand(
        Throwable $exception,
        ?int $failedActivitySequence = null,
        ?string $failedActivityExecutionId = null,
    ): array
    {
        $command = [
            'type' => 'fail_workflow',
            'message' => $exception->getMessage(),
            'exception_type' => $exception::class,
        ];
        if ($failedActivitySequence !== null && $failedActivityExecutionId !== null) {
            $command['failed_step_sequence'] = $failedActivitySequence;
            $command['failed_activity_execution_id'] = $failedActivityExecutionId;
        }

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

    /** @param array<array-key, mixed> $registry */
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
