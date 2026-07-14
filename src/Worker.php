<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
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
    private const MAX_HEARTBEAT_INTERVAL_SECONDS = 3600;

    /** @var array<string, callable(WorkflowContext, mixed ...$arguments): mixed> */
    private array $workflows = [];
    /** @var array<string, callable(ActivityContext, mixed ...$arguments): mixed> */
    private array $activities = [];
    /** @var array<string, array<string, callable(QueryContext, mixed ...$arguments): mixed>> */
    private array $queries = [];
    /** @var array<string, array<string, callable(QueryContext, mixed ...$arguments): mixed>> */
    private array $updates = [];
    private bool $shutdownRequested = false;
    private bool $registered = false;
    private float $lastHeartbeatAt = 0.0;
    private int $heartbeatIntervalSeconds;
    /** @var \Closure(): float */
    private readonly \Closure $clock;
    private readonly string $workerId;
    private readonly Replayer $replayer;

    public function __construct(
        private readonly Client $client,
        public readonly string $taskQueue,
        ?string $workerId = null,
        int $heartbeatIntervalSeconds = self::DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        private readonly ?string $buildId = null,
        ?\Closure $clock = null,
    ) {
        $this->workerId = $workerId ?? 'php-worker-'.bin2hex(random_bytes(8));
        $this->heartbeatIntervalSeconds = $this->validHeartbeatInterval($heartbeatIntervalSeconds)
            ?? self::DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
        $this->clock = $clock ?? static fn (): float => microtime(true);
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
        $workflowPoll = $this->client->pollWorkflowTaskResponse(
            $this->workerId,
            $this->taskQueue,
            $this->preparePoll($pollTimeoutSeconds),
        );
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

        $activityPoll = $this->client->pollActivityTaskResponse(
            $this->workerId,
            $this->taskQueue,
            $this->preparePoll($handled ? 0 : $pollTimeoutSeconds),
        );
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

        $queryPoll = $this->client->pollQueryTaskResponse(
            $this->workerId,
            $this->taskQueue,
            $this->preparePoll($handled ? 0 : $pollTimeoutSeconds),
        );
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
            $this->client->heartbeatWorkflowTask($taskId, $leaseOwner, $attempt);
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
            $this->client->failWorkflowTask(
                $taskId,
                $leaseOwner,
                $attempt,
                'PHP workflow task execution failed: '.$exception->getMessage(),
                $exception::class,
            );
        }
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
            $this->client->failActivityTask(
                $taskId,
                $attemptId,
                $leaseOwner,
                $exception->getMessage(),
                $exception::class,
                $exception instanceof ActivityCancelled,
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
            $this->client->failQueryTask($taskId, $leaseOwner, $attempt, $exception->getMessage());
        }
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

    /** @param array<string, mixed> $registry */
    private function assertUnique(array $registry, string $name, string $kind): void
    {
        if (isset($registry[$name])) {
            throw new \InvalidArgumentException("Duplicate {$kind} registration: {$name}.");
        }
    }
}
