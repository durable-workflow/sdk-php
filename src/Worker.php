<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use Throwable;

/** Managed synchronous remote worker for workflow, activity, query, and update tasks. */
final class Worker
{
    private const DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;
    private const INITIAL_TRANSIENT_RETRY_DELAY_SECONDS = 0.1;
    private const MAX_HEARTBEAT_INTERVAL_SECONDS = 3600;
    private const MAX_TRANSIENT_RETRY_DELAY_SECONDS = 5.0;
    private const TRANSIENT_RETRY_SLEEP_SLICE_SECONDS = 0.1;

    /** @var array<string, callable(WorkflowContext, mixed ...$arguments): mixed> */
    private array $workflows = [];
    /** @var array<string, callable(ActivityContext, mixed ...$arguments): mixed> */
    private array $activities = [];
    /** @var array<string, array<string, callable(QueryContext, mixed ...$arguments): mixed>> */
    private array $queries = [];
    /** @var array<string, array<string, callable(mixed ...$arguments): mixed>> */
    private array $signals = [];
    /** @var array<string, array<string, callable(QueryContext, mixed ...$arguments): mixed>> */
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
    ) {
        $this->workerId = $workerId ?? 'php-worker-'.bin2hex(random_bytes(8));
        $this->heartbeatIntervalSeconds = $this->validHeartbeatInterval($heartbeatIntervalSeconds)
            ?? self::DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
        $this->clock = $clock ?? static fn (): float => microtime(true);
        $this->sleeper = $sleeper ?? static function (int $microseconds): void {
            usleep($microseconds);
        };
        $this->replayer = new Replayer($client->payloadCodec());
    }

    /** @param callable(WorkflowContext, mixed ...$arguments): mixed $handler */
    public function registerWorkflow(string $workflowType, callable $handler): self
    {
        $this->assertUnique($this->workflows, $workflowType, 'workflow');
        $this->workflows[$workflowType] = $handler;

        return $this;
    }

    /** @param callable(ActivityContext, mixed ...$arguments): mixed $handler */
    public function registerActivity(string $activityType, callable $handler): self
    {
        $this->assertUnique($this->activities, $activityType, 'activity');
        $this->activities[$activityType] = $handler;

        return $this;
    }

    /** @param callable(QueryContext, mixed ...$arguments): mixed $handler */
    public function registerQuery(string $workflowType, string $queryName, callable $handler): self
    {
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
        $this->assertValidDeclarationName($workflowType, 'workflow type');
        $this->assertValidDeclarationName($signalName, 'signal');
        $this->signals[$workflowType] ??= [];
        $this->assertUnique($this->signals[$workflowType], $signalName, 'signal');
        $this->signals[$workflowType][$signalName] = $signature ?? static fn (): mixed => null;

        return $this;
    }

    /** @param callable(QueryContext, mixed ...$arguments): mixed $handler */
    public function registerUpdate(string $workflowType, string $updateName, callable $handler): self
    {
        $this->updates[$workflowType] ??= [];
        $this->assertUnique($this->updates[$workflowType], $updateName, 'update');
        $this->updates[$workflowType][$updateName] = $handler;

        return $this;
    }

    public function requestShutdown(): void
    {
        $this->shutdownRequested = true;
    }

    public function run(int $pollTimeoutSeconds = 5): void
    {
        $this->installSignalHandlers();
        $registration = $this->client->registerWorker(
            $this->workerId,
            $this->taskQueue,
            array_keys($this->workflows),
            array_keys($this->activities),
            ['query_tasks', 'workflow_updates', 'durable_history_replay', 'graceful_shutdown'],
            buildId: $this->buildId,
            workflowCommandContracts: $this->workflowCommandContracts(),
        );
        $this->applyHeartbeatInterval($registration);
        $this->registered = true;
        $this->lastHeartbeatAt = $this->now();

        while (!$this->shutdownRequested) {
            $this->tick($pollTimeoutSeconds);
            $this->heartbeatIfDue();
        }
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
        $this->heartbeatIfDue();
        $workflowTask = $this->taskFromPoll($workflowPoll);
        if ($workflowTask !== null) {
            $this->executeWorkflowTask($workflowTask);
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
            $this->executeActivityTask($activityTask);
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
            $this->executeQueryTask($queryTask);
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
    private function executeWorkflowTask(array $task): void
    {
        $taskId = (string) ($task['task_id'] ?? '');
        $attempt = (int) ($task['workflow_task_attempt'] ?? 1);
        $leaseOwner = (string) ($task['lease_owner'] ?? $this->workerId);
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
                    $commands = $this->replayer->replay($handler, $history, $input, $this->taskQueue, $task)->commands;
                } catch (NonDeterministicWorkflow $exception) {
                    throw $exception;
                } catch (Throwable $exception) {
                    $commands = [[
                        'type' => 'fail_workflow',
                        'message' => $exception->getMessage(),
                        'exception_type' => $exception::class,
                    ]];
                }
            }
            $this->client->completeWorkflowTask($taskId, $leaseOwner, $attempt, $commands);
        } catch (Throwable $exception) {
            $this->acknowledgeTaskFailure(
                'workflow',
                $exception,
                function (Throwable $failure) use ($taskId, $leaseOwner, $attempt): void {
                    $this->client->failWorkflowTask(
                        $taskId,
                        $leaseOwner,
                        $attempt,
                        'PHP workflow task execution failed: '.$failure->getMessage(),
                        $failure::class,
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
        Throwable $taskFailure,
        callable $failureAcknowledgement,
    ): void {
        if ($this->isTerminalTaskConflict($taskKind, $taskFailure)) {
            return;
        }
        if ($taskFailure instanceof ServerException) {
            throw $taskFailure;
        }

        try {
            $failureAcknowledgement($taskFailure);
        } catch (Throwable $acknowledgementFailure) {
            if (!$this->isTerminalTaskConflict($taskKind, $acknowledgementFailure)) {
                throw $acknowledgementFailure;
            }
        }
    }

    private function isTerminalTaskConflict(string $taskKind, Throwable $exception): bool
    {
        if (!$exception instanceof ServerException || $exception->status !== 409) {
            return false;
        }

        $details = $exception->details;
        if ($details === null || array_is_list($details)) {
            return false;
        }

        $reason = $exception->reason;

        return match ($taskKind) {
            'workflow' => $reason === 'run_closed'
                && ($details['can_continue'] ?? null) === false
                && ($details['task_status'] ?? null) === 'cancelled',
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

        return $history;
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
            ];
        }

        return $contracts;
    }

    /**
     * @param array<string, callable> $handlers
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
            $reflection = new \ReflectionFunction(\Closure::fromCallable($handler));

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

    /** @param array<string, mixed> $registry */
    private function assertUnique(array $registry, string $name, string $kind): void
    {
        if (isset($registry[$name])) {
            throw new \InvalidArgumentException("Duplicate {$kind} registration: {$name}.");
        }
    }

    private function assertValidDeclarationName(string $name, string $kind): void
    {
        if ($name === '' || trim($name) !== $name) {
            throw new \InvalidArgumentException("Signal declaration {$kind} must be non-empty without surrounding whitespace.");
        }
    }
}
