<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use Closure;
use DateTimeImmutable;
use Fiber;
use LogicException;
use WeakReference;

/** Straight-line deterministic operations available while a workflow Fiber is replayed. */
final class WorkflowContext
{
    public const MESSAGE_STREAM_SIGNAL = '__durable_workflow_message_stream';

    public const MESSAGE_STREAM_SCHEMA = 'durable-workflow.v2.message-stream.message';

    public const MESSAGE_STREAM_CURSOR_SCHEMA = 'durable-workflow.v2.message-stream.cursor';

    public const MAX_PARALLEL_OPERATIONS = 1000;

    private const MIN_VERSION = -2_147_483_648;

    private const MAX_VERSION = 2_147_483_647;

    /** @var Fiber<mixed, mixed, mixed, mixed>|null */
    private readonly ?Fiber $execution;

    private int $workflowStreamCommandOrdinal = 0;

    private int $cancellationShieldDepth = 0;

    private string $cancellationScopeId = 'root';

    private ?string $deliveredCancellationRequestId = null;

    private ?CancellationContext $deliveredCancellationContext = null;

    /** @var array<string, ScopedCancellationContext> */
    private array $deliveredScopeCancellations = [];

    /** @var array<string, array<string, string>> Original delivery snapshots, never renewed budgets. */
    private array $deliveredScopeCleanup = [];

    private ?CancellationReplayClock $cancellationReplayClock = null;

    /** @var list<list<DeferredWorkflowOperation|ParallelWorkflowCommand>> */
    private array $captureFrames = [];

    /** @var array<string, list<MessageStreamMessage>> */
    private array $messageStreamMessages = [];

    /** @var array<string, int> */
    private array $messageStreamCursors = [];

    /** @var array<string, int> */
    private array $messageStreamWaits = [];

    /**
     * @param list<array<string, mixed>> $history
     * @param Fiber<mixed, mixed, mixed, mixed>|null $execution
     * @param list<string> $localActivityCancellationPolicies
     */
    public function __construct(
        public readonly string $workflowId,
        public readonly string $runId,
        private readonly array $history,
        private readonly PayloadCodec $codec,
        private readonly bool $cancellationRequested = false,
        ?Fiber $execution = null,
        private readonly ?string $workflowCommandId = null,
        private readonly ?Closure $localActivityExecutor = null,
        private readonly bool $prepareLocalActivities = false,
        private readonly bool $prepareLocalActivityGroups = false,
        private readonly array $localActivityCancellationPolicies = [],
        private readonly bool $allowCancellationScopeAuthoring = false,
        private bool $hasAuthoredCancellationScopes = false,
        private readonly bool $allowScopedPreparedLocalActivities = false,
    ) {
        $this->execution = $execution;
        $this->loadMessageStreamMessages();
    }

    /**
     * @internal Unfrozen candidate authoring, without scoped delivery support.
     * The body starts only after its original opening has committed. Deferred
     * operations keep this immediate membership after the body returns.
     *
     * @param callable(): mixed $body
     */
    public function cancellationScope(callable $body, bool $shieldParent = false): mixed
    {
        $this->assertActiveFiber();
        if (!$this->allowCancellationScopeAuthoring) {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not enabled candidate scope authoring.');
        }
        if (isset($this->deliveredScopeCancellations[$this->cancellationScopeId])) {
            throw new WorkflowClaimAborted('cancellation_scope_cleanup_authority_missing: a delivered scope cannot admit a new scope.');
        }
        if ($this->isCapturing()) {
            throw new WorkflowClaimAborted('cancellation_scope_opening_inside_group_not_supported: open the scope before capturing its operations.');
        }
        $parent = $this->cancellationScopeId;
        $scopeId = $this->suspend(new WorkflowCommand('open_cancellation_scope', 'cancellation_scope', [
            'parent_scope_id' => $parent, 'shield_parent' => $shieldParent,
        ]));
        if (!is_string($scopeId) || $scopeId === '' || $scopeId === 'root') {
            throw new WorkflowClaimAborted('Scope body requires its original canonical opening identity.');
        }
        $this->cancellationScopeId = $scopeId;
        $this->hasAuthoredCancellationScopes = true;
        try {
            return $body();
        } finally {
            $this->cancellationScopeId = $parent;
        }
    }

    public function messageStream(string $name): MessageStream
    {
        if (!preg_match('/^[A-Za-z0-9._:-]{1,128}$/', $name)) {
            throw new \InvalidArgumentException(
                'Message stream names must contain 1-128 letters, numbers, periods, underscores, colons, or hyphens.',
            );
        }

        return new MessageStream($this, $name);
    }

    public function hasPendingMessageStreamMessages(string $name): bool
    {
        return ($this->messageStreamMessages[$name] ?? []) !== [];
    }

    public function messageStreamCursor(string $name): int
    {
        return $this->messageStreamCursors[$name] ?? 0;
    }

    public function recordMessageStreamWait(string $name, int $afterPosition): void
    {
        $this->messageStreamWaits[$name] = $afterPosition;
    }

    /** @return list<MessageStreamMessage> */
    public function consumeMessageStreamMessages(string $name, int $maxItems): array
    {
        $queue = $this->messageStreamMessages[$name] ?? [];
        $batch = array_slice($queue, 0, $maxItems);
        $this->messageStreamMessages[$name] = array_slice($queue, count($batch));
        unset($this->messageStreamWaits[$name]);

        if ($batch !== []) {
            $last = $batch[array_key_last($batch)];
            $this->messageStreamCursors[$name] = $last->position;
        }

        return $batch;
    }

    /** @return list<array{stream_name: string, through_position: int}> */
    public function messageStreamCursorAcknowledgements(): array
    {
        ksort($this->messageStreamCursors);

        return array_map(
            static fn (string $name, int $position): array => [
                'stream_name' => $name,
                'through_position' => $position,
            ],
            array_keys($this->messageStreamCursors),
            array_values($this->messageStreamCursors),
        );
    }

    /** @return list<array{stream_name: string, after_position: int}> */
    public function messageStreamPendingWaits(): array
    {
        ksort($this->messageStreamWaits);

        return array_map(
            static fn (string $name, int $position): array => [
                'stream_name' => $name,
                'after_position' => $position,
            ],
            array_keys($this->messageStreamWaits),
            array_values($this->messageStreamWaits),
        );
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

    /**
     * Execute a registered activity in this workflow worker and record its outcome durably.
     *
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function localActivity(string $activityType, array $arguments = [], array $options = []): mixed
    {
        $this->assertActiveFiber();
        $deliveredScope = isset($this->deliveredScopeCancellations[$this->cancellationScopeId])
            || ($this->cancellationScopeId === 'root' && $this->deliveredCancellationContext !== null);
        if ($this->hasAuthoredCancellationScopes && (!$this->prepareLocalActivities
            || ($deliveredScope ? !$this->isCancellationShielded()
                : (!$this->allowScopedPreparedLocalActivities || $this->isCapturing())))) {
            throw new WorkflowClaimAborted('cancellation_scope_local_activity_not_supported: this PHP worker has not qualified selective callback supervision.');
        }
        if ($this->prepareLocalActivities && $this->isCapturing() && !$this->prepareLocalActivityGroups) {
            throw new WorkflowClaimAborted(
                'prepared_local_parallel_admission_unavailable: the installed prepared-local contract cannot atomically admit this group.',
            );
        }
        if ($this->localActivityExecutor === null) {
            throw new LogicException('This worker explicitly refuses local activity execution.');
        }
        foreach (['connection', 'queue', 'worker_session', 'schedule_to_start_timeout'] as $field) {
            if (array_key_exists($field, $options)) {
                throw new \InvalidArgumentException("Local activities do not accept {$field} routing options.");
            }
        }

        $command = WorkflowCommand::localActivity(
            $activityType,
            $arguments,
            $options,
            $this->localActivityExecutor,
            prepared: $this->prepareLocalActivities,
        );
        $command = $this->withCancellationScope($command);
        if (array_key_exists('cancellation_policy', $command->attributes)
            && !in_array($command->attributes['cancellation_policy'], $this->localActivityCancellationPolicies, true)) {
            throw new WorkflowClaimAborted('prepared_local_activity_cancellation_policy_not_supported: requested '.$command->attributes['cancellation_policy']
                .', installed policies '.($this->localActivityCancellationPolicies === [] ? 'none' : implode(', ', $this->localActivityCancellationPolicies))
                .'. A negotiated original claim requires prepared_local_activity_cancellation_policies.');
        }
        if ($this->prepareLocalActivities && $this->isCapturing()) {
            $operation = new DeferredWorkflowOperation($command);
            $this->capture($operation);

            return $operation;
        }

        return $this->suspend($command);
    }

    /** Create an isolated deterministic saga for activity compensation. */
    public function saga(): Saga
    {
        $this->assertActiveFiber();

        return new Saga($this);
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
        $operation = $this->deferCondition($predicate, $key, $timeout);
        if ($this->isCapturing()) {
            $this->capture($operation);

            return false;
        }

        return (bool) $this->suspend($operation->command);
    }

    /** Prepare a deterministic condition or signal-derived wait for a durable group. */
    public function deferCondition(
        callable $predicate,
        ?string $key = null,
        int|float|null $timeout = null,
    ): DeferredWorkflowOperation {
        $this->assertActiveFiber();
        $condition = Closure::fromCallable($predicate);
        $timeoutSeconds = $timeout === null ? null : max(0, (int) ceil($timeout));

        return new DeferredWorkflowOperation($this->withCancellationScope(WorkflowCommand::conditionWait(
            $condition,
            self::conditionKey($key),
            ConditionWaitDefinition::fingerprint($condition),
            $timeoutSeconds,
        )));
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

        return new DeferredWorkflowOperation($this->withCancellationScope(WorkflowCommand::activity($activityType, $arguments, $options)));
    }

    /** Prepare a durable timer for an all/parallel barrier. */
    public function deferTimer(int|float $seconds): DeferredWorkflowOperation
    {
        $this->assertActiveFiber();

        return new DeferredWorkflowOperation($this->withCancellationScope(WorkflowCommand::timer((int) ceil($seconds))));
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

        return new DeferredWorkflowOperation($this->withCancellationScope(WorkflowCommand::childWorkflow($workflowType, $arguments, $options)));
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

    /**
     * Schedule every durable member and resume with the first committed winner.
     * Non-winning operations continue and remain available through durable handles.
     *
     * @param iterable<int|string, callable(): mixed|DeferredWorkflowOperation|ParallelWorkflowCommand> $operations
     */
    public function select(iterable $operations): SelectionResult
    {
        $this->assertActiveFiber();
        if ($this->isCapturing()) {
            throw new LogicException('WorkflowContext::select() cannot be nested inside another durable group.');
        }

        $resolved = (function () use ($operations): \Generator {
            foreach ($operations as $key => $operation) {
                yield $key => (is_callable($operation)
                    ? $this->captureOperation($operation)
                    : $this->assertDeferredOperation($operation));
            }
        })();

        $group = new ParallelWorkflowCommand($resolved, 'select');
        if ($group->leafCount() > self::MAX_PARALLEL_OPERATIONS) {
            throw new LogicException(sprintf(
                'WorkflowContext::select() fan-out of %d exceeds the deterministic limit of %d operations.',
                $group->leafCount(),
                self::MAX_PARALLEL_OPERATIONS,
            ));
        }

        /** @var SelectionResult */
        return $this->suspend($group);
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

    /**
     * Merge non-indexed workflow memo metadata. Null removes a key; all other
     * Avro values replace that key while unrelated memo entries are preserved.
     *
     * @param array<string, mixed> $entries
     */
    public function upsertMemo(array $entries): void
    {
        $this->suspend(WorkflowCommand::upsertMemo($entries));
    }

    public function isCancellationRequested(): bool
    {
        return $this->cancellationRequested || $this->deliveredCancellationRequestId !== null
            || isset($this->deliveredScopeCancellations[$this->cancellationScopeId]);
    }

    public function throwIfCancellationRequested(): void
    {
        if ($this->isCancellationRequested() && !$this->isCancellationShielded()) {
            $context = $this->cancellationContext();
            throw new WorkflowCancelled(
                'Workflow cancellation was requested.', requestId: $context->requestId ?? $this->deliveredCancellationRequestId,
                context: $context,
            );
        }
    }

    /** The original context becomes visible at its committed authored boundary. */
    public function cancellationContext(): CancellationContext|ScopedCancellationContext|null
    {
        return $this->deliveredCancellationContext ?? $this->deliveredScopeCancellations[$this->cancellationScopeId] ?? null;
    }

    /** @internal Immediate authored address, retained while its body unwinds. */
    public function currentCancellationScopeId(): string
    {
        return $this->cancellationScopeId;
    }

    /** @internal Retained contexts share the clock of consumed blocking history. */
    public function hasDeliveredCancellation(): bool
    {
        return $this->deliveredCancellationRequestId !== null || $this->deliveredScopeCancellations !== [];
    }

    /**
     * Permit deterministic cleanup without delivering the same request again.
     * Server still owns the original cleanup deadline and task lease.
     *
     * @template TResult
     * @param callable(): TResult $cleanup
     * @return TResult
     */
    public function cancellationShield(callable $cleanup): mixed
    {
        $this->assertActiveFiber();
        ++$this->cancellationShieldDepth;
        try {
            return $cleanup();
        } finally {
            --$this->cancellationShieldDepth;
        }
    }

    /** @internal Replay checks shielding at the authored cancellation boundary. */
    public function isCancellationShielded(): bool
    {
        return $this->cancellationShieldDepth > 0;
    }

    /** @internal Only a verified committed subtree marker can supply these contexts.
     * @param array<string, ScopedCancellationContext> $contexts
     * @param array<string, array<string, string>> $cleanupSnapshots
     */
    public function deliveredScopeCascade(array $contexts, array $cleanupSnapshots): WorkflowCancelled
    {
        $active = $contexts[$this->cancellationScopeId] ?? null;
        if (!$active instanceof ScopedCancellationContext) {
            throw new LogicException('Committed subtree delivery must include the active authored scope.');
        }
        foreach ($contexts as $scopeId => $context) {
            if ($context->scopeId !== $scopeId || $context->workflowRunId !== $this->runId
                || $context->workflowInstanceId !== $this->workflowId
                || $context->rootContext->toArray() !== $active->rootContext->toArray()) {
                throw new LogicException('Committed subtree delivery changes its original run or root.');
            }
            $snapshot = $cleanupSnapshots[$scopeId] ?? null;
            if ($snapshot === null || $snapshot['scope_id'] !== $scopeId || $snapshot['request_id'] !== $context->requestId) {
                throw new LogicException('Committed subtree delivery requires its original cleanup snapshot.');
            }
            $this->deliveredScopeCleanup[$scopeId] = $snapshot;
        }
        $error = $this->deliveredCancellation($active->requestId, $active);
        $clock = $this->cancellationClock();
        foreach ($contexts as $scopeId => $context) {
            if ($scopeId === $active->scopeId) { continue; }
            $this->deliveredScopeCancellations[$scopeId] = $context->withReplayClock($clock);
        }

        return $error;
    }

    /** @internal Only a committed delivery marker authorizes this state change. */
    public function deliveredCancellation(string $requestId, CancellationContext|ScopedCancellationContext|null $context = null): WorkflowCancelled
    {
        if ($context instanceof ScopedCancellationContext && ($context->requestId !== $requestId
            || $context->workflowRunId !== $this->runId || $context->workflowInstanceId !== $this->workflowId
            || $context->scopeId !== $this->cancellationScopeId)) {
            throw new LogicException('Committed scope delivery must match its original request and active authored address.');
        }
        if ($context !== null) {
            $context = $context->withReplayClock($this->cancellationClock());
        }
        if ($context instanceof ScopedCancellationContext) {
            $this->deliveredScopeCancellations[$context->scopeId] = $context;
        } else {
            $this->deliveredCancellationRequestId = $requestId;
            $this->deliveredCancellationContext = $context;
        }

        return new WorkflowCancelled('Workflow cancellation was requested.', requestId: $requestId, context: $context);
    }

    /** @return Closure(): DateTimeImmutable */
    private function cancellationClock(): Closure
    {
        $reference = WeakReference::create($this);

        return static function () use ($reference): DateTimeImmutable {
            $workflow = $reference->get();
            if (!$workflow instanceof self) { throw new LogicException('Cancellation remaining() requires an active workflow.'); }
            $workflow->assertActiveFiber();

            return ($workflow->cancellationReplayClock ??= new CancellationReplayClock())->time();
        };
    }

    /**
     * @internal
     * @param array<string, mixed>|null $event
     */
    public function observeCancellationReplayTime(?array $event): void
    {
        ($this->cancellationReplayClock ??= new CancellationReplayClock())->observe($event);
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

    private function loadMessageStreamMessages(): void
    {
        $seenPositions = [];
        $seenMessageIds = [];
        foreach ($this->signals(self::MESSAGE_STREAM_SIGNAL) as $arguments) {
            if (count($arguments) !== 1 || !is_array($arguments[0])) {
                continue;
            }
            $envelope = $arguments[0];
            $streamName = $envelope['stream_name'] ?? null;
            $throughPosition = $envelope['through_position'] ?? null;
            if (($envelope['schema'] ?? null) === self::MESSAGE_STREAM_CURSOR_SCHEMA
                && is_string($streamName)
                && is_int($throughPosition)
                && $throughPosition >= 0) {
                $this->messageStreamCursors[$streamName] = max(
                    $throughPosition,
                    $this->messageStreamCursors[$streamName] ?? 0,
                );
                $this->messageStreamMessages[$streamName] = array_values(array_filter(
                    $this->messageStreamMessages[$streamName] ?? [],
                    static fn (MessageStreamMessage $message): bool => $message->position > $throughPosition,
                ));

                continue;
            }

            $messageId = $envelope['message_id'] ?? null;
            $position = $envelope['position'] ?? null;
            $payloadEnvelope = $envelope['payload_envelope'] ?? null;
            if (($envelope['schema'] ?? null) !== self::MESSAGE_STREAM_SCHEMA
                || !is_string($streamName)
                || !is_string($messageId)
                || !is_int($position)
                || $position < 1
                || !is_array($payloadEnvelope)) {
                continue;
            }
            try {
                $values = $this->codec->decodeEnvelope($payloadEnvelope);
            } catch (\Throwable) {
                continue;
            }
            if (!is_array($values) || !array_is_list($values)) {
                continue;
            }
            if ($position <= ($this->messageStreamCursors[$streamName] ?? 0)) {
                continue;
            }
            if (isset($seenPositions[$streamName][$position]) || isset($seenMessageIds[$streamName][$messageId])) {
                continue;
            }
            $seenPositions[$streamName][$position] = true;
            $seenMessageIds[$streamName][$messageId] = true;
            $this->messageStreamMessages[$streamName][] = new MessageStreamMessage(
                $streamName,
                $messageId,
                $position,
                $values,
            );
        }

        foreach ($this->messageStreamMessages as &$messages) {
            usort($messages, static fn (MessageStreamMessage $left, MessageStreamMessage $right): int => $left->position <=> $right->position);
        }
        unset($messages);
    }

    private function suspend(WorkflowCommand|ParallelWorkflowCommand $command): mixed
    {
        $this->assertActiveFiber();
        if (isset($this->deliveredScopeCancellations[$this->cancellationScopeId])
            && !($command instanceof WorkflowCommand && $this->isScopeCleanup($command))) {
            throw new WorkflowClaimAborted('cancellation_scope_cleanup_authority_missing: a delivered scope cannot admit a new command.');
        }

        return WorkflowFiberSuspension::suspend($command);
    }

    private function withCancellationScope(WorkflowCommand $command): WorkflowCommand
    {
        if (isset($this->deliveredScopeCancellations[$this->cancellationScopeId]) && !$this->isScopeCleanup($command)) {
            throw new WorkflowClaimAborted('cancellation_scope_cleanup_authority_missing: a delivered scope cannot admit a new operation.');
        }
        if (array_key_exists('cancellation_scope_id', $command->attributes)) {
            throw new \InvalidArgumentException('Operation scope membership is assigned by the workflow authoring boundary.');
        }
        if ($command->type === 'start_timer' && isset($this->deliveredScopeCancellations[$this->cancellationScopeId])) {
            $snapshot = $this->deliveredScopeCleanup[$this->cancellationScopeId]
                ?? throw new WorkflowClaimAborted('cancellation_scope_cleanup_authority_missing: cleanup timer requires its original delivery.');
            $command = $command->withAttributes(['cancellation_cleanup' => array_intersect_key($snapshot,
                array_flip(['scope_id', 'request_id', 'delivery_history_event_id']))]);
        }

        return $this->cancellationScopeId === 'root' ? $command
            : $command->withAttributes(['cancellation_scope_id' => $this->cancellationScopeId]);
    }

    private function isScopeCleanup(WorkflowCommand $command): bool
    {
        return $this->isCancellationShielded() && ($command->type === 'start_timer'
            || ($this->prepareLocalActivities && $command->type === 'record_local_activity'));
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
                || ($captured[0] instanceof ParallelWorkflowCommand && $returned === [])
                || ($captured[0] instanceof DeferredWorkflowOperation
                    && $captured[0]->command->type === 'open_condition_wait'
                    && $returned === false))) {
            return $captured[0];
        }
        if ($captured === []
            && ($returned instanceof DeferredWorkflowOperation || $returned instanceof ParallelWorkflowCommand)) {
            return $returned;
        }

        throw new LogicException(sprintf(
            'Each durable group closure must declare exactly one deferred operation or nested barrier; captured %d.',
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
