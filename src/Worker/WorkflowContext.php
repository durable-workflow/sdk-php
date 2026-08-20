<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\WorkflowCancelled;
use Closure;
use Fiber;
use LogicException;

/** Straight-line deterministic operations available while a workflow Fiber is replayed. */
final class WorkflowContext
{
    /** @var Fiber<mixed, mixed, mixed, mixed>|null */
    private readonly ?Fiber $execution;

    /**
     * @param list<array<string, mixed>> $history
     * @param Fiber<mixed, mixed, mixed, mixed>|null $execution
     */
    public function __construct(
        public readonly string $workflowId,
        public readonly string $runId,
        private readonly array $history,
        private readonly PayloadCodec $codec,
        private readonly bool $cancellationRequested = false,
        ?Fiber $execution = null,
    ) {
        $this->execution = $execution;
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function activity(string $activityType, array $arguments = [], array $options = []): mixed
    {
        return $this->suspend(WorkflowCommand::activity($activityType, $arguments, $options));
    }

    public function sleep(int|float $seconds): void
    {
        $this->suspend(WorkflowCommand::timer((int) ceil($seconds)));
    }

    /**
     * Suspend until the deterministic predicate is satisfied or its durable timeout elapses.
     *
     * The result is true only when the condition was satisfied and false only when it timed out.
     * Give repeated or otherwise ambiguous waits a stable key so replay can identify them.
     *
     * @param callable(): bool $predicate
     */
    public function waitCondition(
        callable $predicate,
        ?string $key = null,
        int|float|null $timeout = null,
    ): bool {
        $condition = Closure::fromCallable($predicate);
        $timeoutSeconds = $timeout === null ? null : max(0, (int) ceil($timeout));

        return (bool) $this->suspend(WorkflowCommand::conditionWait(
            $condition,
            self::conditionKey($key),
            ConditionWaitDefinition::fingerprint($condition),
            $timeoutSeconds,
        ));
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function childWorkflow(string $workflowType, array $arguments = [], array $options = []): mixed
    {
        return $this->suspend(WorkflowCommand::childWorkflow($workflowType, $arguments, $options));
    }

    /** @param callable(): mixed $operation */
    public function sideEffect(callable $operation): mixed
    {
        return $this->suspend(WorkflowCommand::sideEffect($operation));
    }

    /** @param list<mixed> $arguments */
    public function continueAsNew(
        array $arguments = [],
        ?string $workflowType = null,
        ?string $taskQueue = null,
    ): never {
        $this->suspend(WorkflowCommand::continueAsNew($arguments, $workflowType, $taskQueue));

        throw new LogicException('A continue-as-new command cannot resume the current workflow execution.');
    }

    /** @param array<string, mixed> $attributes */
    public function upsertSearchAttributes(array $attributes): void
    {
        $this->suspend(WorkflowCommand::upsertSearchAttributes($attributes));
    }

    public function isCancellationRequested(): bool
    {
        return $this->cancellationRequested;
    }

    public function throwIfCancellationRequested(): void
    {
        if ($this->cancellationRequested) {
            throw new WorkflowCancelled('Workflow cancellation was requested.');
        }
    }

    /** @return list<list<mixed>> */
    public function signals(string $signalName): array
    {
        $signals = [];
        foreach ($this->history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SignalReceived') {
                continue;
            }
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            if (($payload['signal_name'] ?? null) !== $signalName) {
                continue;
            }
            $raw = $payload['value'] ?? $payload['input'] ?? $payload['arguments'] ?? null;
            $decoded = (is_array($raw) || is_string($raw)) ? $this->codec->decodeEnvelope($raw) : null;
            $signals[] = is_array($decoded) && array_is_list($decoded) ? $decoded : [$decoded];
        }

        return $signals;
    }

    /** @return list<list<mixed>> */
    public function updates(string $updateName): array
    {
        $updates = [];
        $seen = [];
        foreach ($this->history as $event) {
            if (!in_array($event['event_type'] ?? $event['type'] ?? null, ['UpdateAccepted', 'UpdateApplied'], true)) {
                continue;
            }
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            if (($payload['update_name'] ?? null) !== $updateName || !isset($payload['arguments'])) {
                continue;
            }
            $updateId = isset($payload['update_id']) ? (string) $payload['update_id'] : '';
            if ($updateId !== '' && isset($seen[$updateId])) {
                continue;
            }
            if ($updateId !== '') {
                $seen[$updateId] = true;
            }
            $raw = $payload['arguments'];
            $decoded = (is_array($raw) || is_string($raw)) ? $this->codec->decodeEnvelope($raw) : null;
            $updates[] = is_array($decoded) && array_is_list($decoded) ? $decoded : [$decoded];
        }

        return $updates;
    }

    private function suspend(WorkflowCommand $command): mixed
    {
        if ($this->execution === null || Fiber::getCurrent() !== $this->execution) {
            throw new LogicException('WorkflowContext operations may only be called by their active workflow Fiber.');
        }

        return Fiber::suspend($command);
    }

    private static function conditionKey(?string $key): ?string
    {
        if ($key === null) {
            return null;
        }

        $key = trim($key);
        if ($key === '' || strlen($key) > 128 || preg_match('/\A[A-Za-z0-9._:-]+\z/', $key) !== 1) {
            throw new LogicException(
                'Condition wait keys must be non-empty URL-safe strings up to 128 characters using only letters, numbers, ".", "_", "-", and ":".',
            );
        }

        return $key;
    }
}
