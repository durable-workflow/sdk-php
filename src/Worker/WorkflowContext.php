<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use Closure;
use Fiber;
use LogicException;

/** Straight-line deterministic operations available while a workflow Fiber is replayed. */
final class WorkflowContext
{
    public const MAX_PARALLEL_OPERATIONS = 1000;

    private const MIN_VERSION = -2_147_483_648;

    private const MAX_VERSION = 2_147_483_647;

    /** @var Fiber<mixed, mixed, mixed, mixed>|null */
    private readonly ?Fiber $execution;

    private int $workflowStreamCommandOrdinal = 0;

    /** @var list<list<DeferredWorkflowOperation|ParallelWorkflowCommand>> */
    private array $captureFrames = [];

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
        private readonly ?string $workflowCommandId = null,
    ) {
        $this->execution = $execution;
    }

    /**
     * Append a replay-safe batch to a named run-scoped Workflow Stream.
     *
     * @param list<WorkflowStreamAppendItem> $items
     */
    public function appendWorkflowStream(
        string $streamName,
        array $items,
        ?int $maxPendingItems = null,
    ): void {
        $ordinal = $this->workflowStreamCommandOrdinal++;
        $identity = $this->workflowCommandId ?: $this->runId;
        $wireItems = [];
        foreach ($items as $index => $item) {
            $wireItems[] = $item->toWire(
                $this->codec,
                sprintf('dw-stream:%s:%d:%d', $identity, $ordinal, $index),
            );
        }

        $this->suspend(WorkflowCommand::workflowStream(array_filter([
            'operation' => 'append',
            'stream_name' => $streamName,
            'command_identity' => $identity,
            'command_ordinal' => $ordinal,
            'items' => $wireItems,
            'max_pending_items' => $maxPendingItems,
        ], static fn (mixed $value): bool => $value !== null)));
    }

    public function closeWorkflowStream(
        string $streamName,
        ?int $retentionSeconds = null,
    ): void {
        $this->finishWorkflowStream($streamName, null, $retentionSeconds);
    }

    public function errorWorkflowStream(
        string $streamName,
        string $errorReason,
        ?int $retentionSeconds = null,
    ): void {
        $this->finishWorkflowStream($streamName, $errorReason, $retentionSeconds);
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function activity(string $activityType, array $arguments = [], array $options = []): mixed
    {
        $operation = $this->deferActivity($activityType, $arguments, $options);
        if ($this->isCapturing()) {
            $this->capture($operation);

            return $operation;
        }

        return $this->suspend($operation->command);
    }

    public function sleep(int|float $seconds): void
    {
        $operation = $this->deferTimer($seconds);
        if ($this->isCapturing()) {
            $this->capture($operation);

            return;
        }

        $this->suspend($operation->command);
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
        $operation = $this->deferChildWorkflow($workflowType, $arguments, $options);
        if ($this->isCapturing()) {
            $this->capture($operation);

            return $operation;
        }

        return $this->suspend($operation->command);
    }

    /**
     * Prepare an activity without scheduling it until an all/parallel barrier is reached.
     *
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function deferActivity(string $activityType, array $arguments = [], array $options = []): DeferredWorkflowOperation
    {
        $this->assertActiveFiber();

        return new DeferredWorkflowOperation(WorkflowCommand::activity($activityType, $arguments, $options));
    }

    /** Prepare a durable timer for an all/parallel barrier. */
    public function deferTimer(int|float $seconds): DeferredWorkflowOperation
    {
        $this->assertActiveFiber();

        return new DeferredWorkflowOperation(WorkflowCommand::timer((int) ceil($seconds)));
    }

    /**
     * Prepare a child workflow without starting it until an all/parallel barrier is reached.
     *
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function deferChildWorkflow(
        string $workflowType,
        array $arguments = [],
        array $options = [],
    ): DeferredWorkflowOperation {
        $this->assertActiveFiber();

        return new DeferredWorkflowOperation(WorkflowCommand::childWorkflow($workflowType, $arguments, $options));
    }

    /**
     * Schedule every deferred leaf, then return results in declaration order.
     *
     * Closures are captured without suspending, so ordinary activity(), childWorkflow(),
     * and sleep() calls remain straight-line. Nested all()/parallel() calls preserve their
     * result shape. The first durable failure is thrown at this barrier.
     *
     * @param iterable<int, callable(): mixed|DeferredWorkflowOperation> $operations
     * @return list<mixed>
     */
    public function all(iterable $operations): array
    {
        $this->assertActiveFiber();
        $resolved = [];
        foreach ($operations as $operation) {
            $resolved[] = is_callable($operation)
                ? $this->captureOperation($operation)
                : $this->assertDeferredOperation($operation);
        }

        $group = new ParallelWorkflowCommand($resolved);
        if ($group->leafCount() > self::MAX_PARALLEL_OPERATIONS) {
            throw new LogicException(sprintf(
                'WorkflowContext::all() fan-out of %d exceeds the deterministic limit of %d operations.',
                $group->leafCount(),
                self::MAX_PARALLEL_OPERATIONS,
            ));
        }

        if ($this->isCapturing()) {
            if ($group->leafCount() === 0) {
                throw new LogicException(
                    'WorkflowContext::all() does not allow an empty nested barrier because replay cannot identify it.',
                );
            }
            $this->capture($group);

            return [];
        }
        if ($group->leafCount() === 0) {
            return [];
        }

        /** @var list<mixed> */
        return $this->suspend($group);
    }

    /**
     * Alias for {@see self::all()}.
     *
     * @param iterable<int, callable(): mixed|DeferredWorkflowOperation> $operations
     * @return list<mixed>
     */
    public function parallel(iterable $operations): array
    {
        return $this->all($operations);
    }

    /** @param callable(): mixed $operation */
    public function sideEffect(callable $operation): mixed
    {
        return $this->suspend(WorkflowCommand::sideEffect($operation));
    }

    /**
     * Select the newest supported version for a change, or replay its recorded decision.
     */
    public function getVersion(string $changeId, int $minSupported, int $maxSupported): int
    {
        $result = $this->version($changeId, $minSupported, $maxSupported, 'version');

        return (int) $result;
    }

    /** Record or replay the standard -1 (legacy) / 1 (patched) decision. */
    public function patched(string $changeId): bool
    {
        return $this->version($changeId, -1, 1, 'patched') === true;
    }

    /** Keep a patch marker alive after the legacy branch has been removed. */
    public function deprecatePatch(string $changeId): void
    {
        $this->version($changeId, -1, 1, 'deprecate_patch');
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

    private function suspend(WorkflowCommand|ParallelWorkflowCommand $command): mixed
    {
        $this->assertActiveFiber();

        return Fiber::suspend($command);
    }

    private function assertActiveFiber(): void
    {
        if ($this->execution === null || Fiber::getCurrent() !== $this->execution) {
            throw new LogicException('WorkflowContext operations may only be called by their active workflow Fiber.');
        }
    }

    private function isCapturing(): bool
    {
        return $this->captureFrames !== [];
    }

    private function capture(DeferredWorkflowOperation|ParallelWorkflowCommand $operation): void
    {
        $frame = array_key_last($this->captureFrames);
        if ($frame === null) {
            throw new LogicException('Deferred workflow operation capture is not active.');
        }
        $this->captureFrames[$frame][] = $operation;
    }

    /** @param callable(): mixed $callback */
    private function captureOperation(callable $callback): DeferredWorkflowOperation|ParallelWorkflowCommand
    {
        $this->captureFrames[] = [];
        $frame = array_key_last($this->captureFrames);
        try {
            $returned = $callback();
            $captured = $this->captureFrames[$frame];
        } finally {
            array_pop($this->captureFrames);
        }

        if (count($captured) === 1
            && ($returned === null
                || $returned === $captured[0]
                || ($captured[0] instanceof ParallelWorkflowCommand && $returned === []))) {
            return $captured[0];
        }
        if ($captured === []
            && ($returned instanceof DeferredWorkflowOperation || $returned instanceof ParallelWorkflowCommand)) {
            return $returned;
        }

        throw new LogicException(sprintf(
            'Each WorkflowContext::all() closure must declare exactly one deferred operation or nested barrier; captured %d.',
            count($captured),
        ));
    }

    private function assertDeferredOperation(mixed $operation): DeferredWorkflowOperation|ParallelWorkflowCommand
    {
        if ($operation instanceof DeferredWorkflowOperation || $operation instanceof ParallelWorkflowCommand) {
            return $operation;
        }

        throw new LogicException(sprintf(
            'WorkflowContext::all() accepts deferred operations or closures; received %s.',
            get_debug_type($operation),
        ));
    }

    private function finishWorkflowStream(
        string $streamName,
        ?string $errorReason,
        ?int $retentionSeconds,
    ): void {
        $ordinal = $this->workflowStreamCommandOrdinal++;
        $identity = $this->workflowCommandId ?: $this->runId;
        $this->suspend(WorkflowCommand::workflowStream(array_filter([
            'operation' => $errorReason === null ? 'close' : 'error',
            'stream_name' => $streamName,
            'command_identity' => $identity,
            'command_ordinal' => $ordinal,
            'error_reason' => $errorReason,
            'retention_seconds' => $retentionSeconds,
        ], static fn (mixed $value): bool => $value !== null)));
    }

    private function version(
        string $changeId,
        int $minSupported,
        int $maxSupported,
        string $resultKind,
    ): int|bool|null {
        if (trim($changeId) === '') {
            throw new NonDeterministicWorkflow(
                'Version markers require a stable non-empty change ID.',
                expected: 'non-empty change ID',
                actual: $changeId,
                reason: 'version_change_id_invalid',
            );
        }
        if ($minSupported > $maxSupported
            || $minSupported < self::MIN_VERSION
            || $maxSupported > self::MAX_VERSION) {
            throw new NonDeterministicWorkflow(
                "Version marker {$changeId} has an invalid supported range {$minSupported}..{$maxSupported}.",
                expected: '32-bit minSupported <= maxSupported',
                actual: "{$minSupported}..{$maxSupported}",
                reason: 'version_range_invalid',
            );
        }

        return $this->suspend(WorkflowCommand::versionMarker(
            $changeId,
            $minSupported,
            $maxSupported,
            $resultKind,
        ));
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
