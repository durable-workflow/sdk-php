<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\DurableOperationCancelled;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowFiberDiscarded;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use Closure;
use Fiber;
use LogicException;
use Throwable;

/** Re-executes a straight-line workflow Fiber against committed, sequence-ordered history. */
final class Replayer
{
    /**
     * Every durable history event that claims a workflow sequence, by the
     * command kind that owns it, following Core's
     * WorkflowStepHistory::WORKFLOW_STEP_EVENT_TYPES. A command's lifecycle
     * rows (terminal, retry, heartbeat and cancelled rows included) share its
     * sequence. Internal wait-timeout timer rows belong to their wait; see
     * durableCommandKind(). ActivityOutcomeRejected is deliberately absent:
     * it is a payload-free audit fact that neither resolves an activity nor
     * consumes a command position. Rows that only point at a step claim
     * nothing: signal receipt and rejection, updates, message cursors,
     * selection markers, cancellation requests and scope requests or
     * preparations. Cancellation deliveries claim their whole span; see
     * DELIVERY_EVENTS.
     */
    private const DURABLE_COMMAND_KINDS = [
        'ActivityScheduled' => 'activity', 'ActivityStarted' => 'activity', 'ActivityHeartbeatRecorded' => 'activity',
        'ActivityRetryScheduled' => 'activity', 'ActivityCompleted' => 'activity', 'ActivityFailed' => 'activity',
        'ActivityCancelled' => 'activity', 'ActivityTimedOut' => 'activity', 'ActivityCancellationAcknowledged' => 'activity',
        'TimerScheduled' => 'timer', 'TimerFired' => 'timer', 'TimerCancelled' => 'timer',
        'ChildWorkflowScheduled' => 'child', 'ChildRunStarted' => 'child', 'ChildRunCompleted' => 'child',
        'ChildRunFailed' => 'child', 'ChildRunCancelled' => 'child', 'ChildRunTerminated' => 'child',
        'ConditionWaitOpened' => 'condition', 'ConditionWaitSatisfied' => 'condition',
        'ConditionWaitTimedOut' => 'condition', 'ConditionWaitCancelled' => 'condition',
        'SignalWaitOpened' => 'signal', 'SignalWaitReceived' => 'signal', 'SignalApplied' => 'signal',
        'SignalWaitTimedOut' => 'signal', 'SignalWaitCancelled' => 'signal',
        'ServiceOperationScheduled' => 'service_call', 'ServiceCallStarted' => 'service_call',
        'ServiceCallCompleted' => 'service_call', 'ServiceCallFailed' => 'service_call',
        'ServiceCallCancelled' => 'service_call', 'ServiceCallTimedOut' => 'service_call',
        'SideEffectRecorded' => 'side_effect',
        'VersionMarkerRecorded' => 'version_marker',
        'MemoUpserted' => 'memo',
        'SearchAttributesUpserted' => 'search_attributes',
        'CancellationScopeOpened' => 'cancellation_scope',
        'WorkflowContinuedAsNew' => 'continue_as_new',
    ];

    /**
     * A delivered cancellation consumes the interrupted call even when that
     * call was never scheduled, and its sequence_span reserves the whole
     * interrupted range, sequence .. sequence + span - 1 inclusive (Core
     * WorkflowStepHistory::nextSequenceFromEvents()).
     */
    private const DELIVERY_EVENTS = ['CooperativeCancellationDelivered', 'CancellationScopeDelivered'];

    /** An upsert is the only command at its sequence. */
    private const UPSERT_COMMAND_KINDS = ['memo', 'search_attributes'];

    private const MIN_VERSION = -2_147_483_648;

    private const MAX_VERSION = 2_147_483_647;

    /** The occurrence-deadline opt-in on an anchored condition-wait open (#269). */
    public const CONDITION_DEADLINE_POLICY = 'occurrence';

    /** Whether this task's Server advertises condition_wait_occurrence_deadline. */
    private bool $conditionOccurrenceDeadline = false;

    public function __construct(private readonly PayloadCodec $codec)
    {
    }

    /**
     * @internal The Worker sets this from the Server's advertised
     * condition_wait_occurrence_deadline capability on every workflow poll. It
     * decides whether a new chain's first open opts in. A recorded anchored
     * chain keeps its first open's policy on every reopen, and reopens only
     * while the capability is advertised; otherwise the claim is aborted
     * before the reopen is sent (#269).
     */
    public function useConditionOccurrenceDeadline(bool $supported): void
    {
        $this->conditionOccurrenceDeadline = $supported;
    }

    /**
     * @param callable(WorkflowContext, mixed ...$input): mixed $handler
     * @param list<array<string, mixed>> $history
     * @param list<mixed> $input
     * @param array<string, mixed> $task
     * @param list<string> $localActivityCancellationPolicies
     */
    public function replay(
        callable $handler,
        array $history,
        array $input,
        string $taskQueue,
        array $task = [],
        ?callable $localActivityExecutor = null,
        bool $prepareLocalActivities = false,
        bool $prepareLocalActivityGroups = false,
        array $localActivityCancellationPolicies = [],
        bool $allowCancellationScopeAuthoring = false,
        bool $replayCommittedCancellationScopes = false,
        bool $prepareCancellationScopeDelivery = false,
        bool $allowScopedPreparedLocalActivities = false,
    ): ReplayResult {
        if ($prepareLocalActivityGroups && !$prepareLocalActivities) {
            throw new LogicException('Prepared local groups require prepared local activity admission.');
        }
        if ($localActivityCancellationPolicies !== [] && !$prepareLocalActivities) {
            throw new LogicException('Local cancellation policies require prepared local activity admission.');
        }
        if ($replayCommittedCancellationScopes && !$allowCancellationScopeAuthoring) {
            throw new LogicException('Committed scope replay requires candidate scope authoring.');
        }
        if ($prepareCancellationScopeDelivery && !$replayCommittedCancellationScopes) {
            throw new LogicException('Pending scope selection requires canonical committed scope replay.');
        }
        if ($allowScopedPreparedLocalActivities && (!$prepareLocalActivities || !$prepareCancellationScopeDelivery)) {
            throw new LogicException('Scoped local callbacks require prepared admission and canonical scope delivery.');
        }
        WorkflowEntryArguments::validate($handler, $input, (string) ($task['workflow_type'] ?? ''));
        $hasScopes = $this->assertCancellationScopeReplaySupported($history, $allowCancellationScopeAuthoring, $task, $replayCommittedCancellationScopes);
        $scopes = new CancellationScopeHistory($hasScopes ? $history : [], (string) ($task['run_id'] ?? ''));
        // Unfrozen source profile, enabled only by the Worker's explicit scope opt-in.
        $scopeDeliveries = [];
        $committedScopes = null;
        if ($replayCommittedCancellationScopes) {
            $committedScopes = new CommittedCancellationScopeHistory($history, (string) ($task['run_id'] ?? ''),
                (string) ($task['workflow_id'] ?? ''), $scopes, requireCommittedDelivery: !$prepareCancellationScopeDelivery,
                inspectOperationProjections: true, allowPreparedLocalBoundary: $allowScopedPreparedLocalActivities);
            $deliveredScopeIds = [];
            foreach ($committedScopes->deliveries as $scopeSequence => $delivery) {
                $states = $committedScopes->scopeStatesForDelivery($delivery);
                if (array_intersect_key($deliveredScopeIds, $states) !== []) {
                    throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not qualified overlapping subtree deliveries.');
                }
                $deliveredScopeIds += $states;
                $scopeDeliveries[$scopeSequence] = [...$delivery, 'states' => $states];
            }
        }
        $observation = $task['cancellation_request'] ?? null;
        if ($observation !== null && (!is_array($observation) || array_is_list($observation))) {
            throw new NonDeterministicWorkflow('Workflow cancellation observation must be an object.');
        }
        try {
            $cancellation = CancellationHistory::fromEvents(
                $history, (string) ($task['run_id'] ?? ''),
                $observation === null ? null : CancellationRequest::fromObservation($observation),
            );
        } catch (\InvalidArgumentException $error) {
            throw new NonDeterministicWorkflow($error->getMessage(), reason: 'invalid_cooperative_cancellation_history');
        }
        $strictSequences = $cancellation->request !== null || $scopes->openings !== [];
        $steps = $this->recordedSteps($history, $strictSequences, $scopes);
        $stepsBySequence = [];
        foreach ($steps as $step) {
            $stepsBySequence[$step['sequence']] = $step;
        }
        // A committed cancellation is durable authority even without an opened wait.
        /** @var list<CancellationDelivery|null> $cancellationBoundaries */
        $cancellationBoundaries = [
            $cancellation->delivery,
            ...array_column($scopeDeliveries, 'boundary'),
            ...array_column($committedScopes->preparations ?? [], 'boundary'),
        ];
        $cancelledConditionSequences = [];
        foreach ($cancellationBoundaries as $conditionCancellation) {
            if ($conditionCancellation?->callKind === 'condition') {
                $cancelledConditionSequences[$conditionCancellation->sequence] = true;
            }
        }
        $selectionResolutionOrders = [];
        $selectionResolutions = $this->selectionResolutions($history, $selectionResolutionOrders);
        $selectionCancellationOrders = [];
        $selectionCancellations = $this->selectionCancellations($history, $selectionCancellationOrders);
        $selectionOperationIdentities = $this->selectionOperationIdentities($history);
        $completedHistory = $this->hasCompletedHistory($history);
        $allowScopeAuthoring = $allowCancellationScopeAuthoring
            && ($cancellation->request === null || $replayCommittedCancellationScopes);
        $context = null;
        $businessReplayClock = new BusinessReplayClock($history, (string) ($task['workflow_id'] ?? ''), (string) ($task['run_id'] ?? ''));
        $execution = new Fiber(ExecutionTelemetry::bind(function () use ($handler, $history, $input, $task, $businessReplayClock, $cancellation, $scopes, $localActivityExecutor, $prepareLocalActivities, $prepareLocalActivityGroups, $localActivityCancellationPolicies, $allowScopeAuthoring, $allowScopedPreparedLocalActivities, &$context): mixed {
            $current = Fiber::getCurrent();
            if ($current === null) {
                throw new LogicException('Workflow execution did not start inside its Fiber.');
            }
            $context = new WorkflowContext(
                (string) ($task['workflow_id'] ?? ''),
                (string) ($task['run_id'] ?? ''),
                $history,
                $this->codec,
                $cancellation->request === null && (bool) ($task['cancel_requested'] ?? false),
                $current,
                isset($task['workflow_command_id']) && (string) $task['workflow_command_id'] !== ''
                    ? (string) $task['workflow_command_id']
                    : (isset($task['task_id']) ? (string) $task['task_id'] : null),
                $localActivityExecutor === null ? null : Closure::fromCallable($localActivityExecutor),
                $prepareLocalActivities,
                $prepareLocalActivityGroups,
                $localActivityCancellationPolicies,
                $allowScopeAuthoring,
                $scopes->openings !== [],
                $allowScopedPreparedLocalActivities,
                $businessReplayClock,
            );

            try {
                return $handler($context, ...$input);
            } catch (WorkflowFiberDiscarded) {
                // No executor observes a return from a force-closed Fiber.
                return null;
            }
        }));

        $stepCursor = 0;
        $commands = [];
        /** @var array<string, array{version: int, sequence: ?int}> $versionDecisions */
        $versionDecisions = [];
        $suspended = $businessReplayClock->invoke($execution->start(...));
        $nextSequence = 1;
        $cancellationConsumed = false;
        $consumedScopeDeliveries = [];
        $consumedConditionTail = null;

        while (!$execution->isTerminated()) {
            if ($consumedConditionTail !== null) {
                $remaining = $this->restoreConsumedConditionTail($suspended, $consumedConditionTail);
                if ($remaining !== []) {
                    array_splice($steps, $stepCursor, 0, $remaining);
                }
                $consumedConditionTail = null;
            }
            if ($suspended instanceof WorkflowCommand && !isset($cancelledConditionSequences[$nextSequence])
                && ($conditionResult = $this->unrecordedConditionResult($suspended, $steps[$stepCursor] ?? null)) !== null) {
                $suspended = $this->resumeExecution($execution, $businessReplayClock, $conditionResult);
                continue;
            }
            if (!$cancellationConsumed && $cancellation->delivery?->sequence === $nextSequence
                && !$this->isRootCancellationBoundary($suspended, $cancellation->delivery, $stepsBySequence, $context)) {
                throw new NonDeterministicWorkflow('Committed root cancellation cannot replace a scoped operation.', $nextSequence,
                    reason: 'cooperative_cancellation_boundary_mismatch');
            }
            foreach ($scopeDeliveries as $scopeSequence => $scopeDelivery) {
                if (isset($consumedScopeDeliveries[$scopeSequence])) {
                    continue;
                }
                $delivery = $scopeDelivery['boundary'];
                if ($scopeSequence < $nextSequence) {
                    throw new NonDeterministicWorkflow('Workflow passed its committed scope cancellation boundary.', $scopeSequence,
                        reason: 'cancellation_scope_boundary_mismatch');
                }
                if ($scopeSequence !== $nextSequence) {
                    break;
                }
                $boundary = $this->cancellationBoundary($suspended, $nextSequence, $stepsBySequence, $delivery->requestId, $delivery);
                $activeScope = $context->currentCancellationScopeId();
                if ($boundary != $delivery || !isset($scopeDelivery['states'][$activeScope])
                    || $context->isCancellationShielded()) {
                    throw new NonDeterministicWorkflow('Committed scope cancellation changed its authored call, membership or shielding.', $scopeSequence,
                        reason: 'cancellation_scope_boundary_mismatch');
                }
                $this->assertCancellationCallMatches($suspended, $delivery, $stepsBySequence, $context);
                while (isset($steps[$stepCursor]) && $steps[$stepCursor]['sequence'] < $scopeSequence + $delivery->sequenceSpan) {
                    if ($steps[$stepCursor]['sequence'] < $scopeSequence) {
                        throw new NonDeterministicWorkflow('Scope cancellation skipped an earlier authored operation.', $steps[$stepCursor]['sequence'],
                            reason: 'cancellation_scope_boundary_mismatch');
                    }
                    ++$stepCursor;
                }
                $nextSequence = $scopeSequence + $delivery->sequenceSpan;
                $consumedScopeDeliveries[$scopeSequence] = true;
                $context->observeCancellationReplayTime($scopeDelivery['event']);
                $suspended = $this->throwIntoExecution($execution, $businessReplayClock, $context->deliveredScopeCascade(array_map(
                    static fn (array $state): ScopedCancellationContext => $state['context'], $scopeDelivery['states'],
                ), array_map(static fn (array $state): array => CancellationScopeTimerCleanup::snapshot(
                    $state['context'], $state['authority_deadline_at'], $scopeDelivery['event'],
                ), $scopeDelivery['states'])));
                continue 2;
            }
            if ($prepareCancellationScopeDelivery && !$context->isCancellationShielded()) {
                $pending = $committedScopes->pendingRequestForScope($context->currentCancellationScopeId(), $scopes);
                if ($pending !== null) {
                    $request = $pending['context'];
                    $preparation = $committedScopes->preparations[$request->scopeId] ?? null;
                    $original = $preparation['boundary'] ?? null;
                    if ($original !== null && $original->sequence < $nextSequence) {
                        throw new NonDeterministicWorkflow('Workflow passed its original prepared scope boundary.', $original->sequence,
                            reason: 'cancellation_scope_boundary_mismatch');
                    }
                    $boundary = $this->cancellationBoundary($suspended, $nextSequence, $stepsBySequence, $request->requestId, $original);
                    $atOriginal = $original !== null && $original->sequence === $nextSequence;
                    if ($atOriginal || ($original === null && $boundary !== null
                        && CancellationHistory::rangeEligibleBefore($history, $pending['index'], $boundary->sequence, $boundary->sequenceSpan))) {
                        if ($boundary === null || ($original !== null && $boundary != $original)) {
                            throw new NonDeterministicWorkflow('Pending scope delivery changed its original prepared call.', $nextSequence,
                                reason: 'cancellation_scope_boundary_mismatch');
                        }
                        if (!in_array($boundary->callKind, ['activity', 'timer', 'condition', 'child', 'parallel'], true)
                            && !($boundary->callKind === 'local_activity' && $allowScopedPreparedLocalActivities)) {
                            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this pending operation kind is not qualified by this PHP worker.');
                        }
                        $this->assertCancellationCallMatches($suspended, $boundary, $stepsBySequence, $context);
                        if ($suspended instanceof ParallelWorkflowCommand) {
                            $this->assertPendingScopeGroup($suspended, $boundary, $stepsBySequence, $context->currentCancellationScopeId());
                        }
                        if ($preparation !== null && !isset($committedScopes->scopeStatesForDelivery($preparation)[$context->currentCancellationScopeId()])) {
                            throw new NonDeterministicWorkflow('Pending scope call is absent from its original frozen subtree.', $nextSequence,
                                reason: 'cancellation_scope_boundary_mismatch');
                        }

                        return new ReplayResult($commands, $context->messageStreamCursorAcknowledgements(), $context->messageStreamPendingWaits(),
                            cancellationScopeDelivery: new CancellationScopeDeliveryIntent($request, $boundary,
                                $preparation['event']['id'] ?? null,
                                $preparation === null ? null : new \DateTimeImmutable($preparation['event']['payload']['authority_deadline_at'])));
                    }
                }
            }
            if ($cancellation->request !== null && !$cancellationConsumed) {
                $boundary = $this->cancellationBoundary($suspended, $nextSequence, $stepsBySequence, $cancellation->request->requestId, $cancellation->delivery);
                $delivery = $cancellation->delivery;
                if ($delivery !== null && $delivery->sequence < $nextSequence) {
                    throw new NonDeterministicWorkflow(
                        'Workflow passed its committed cancellation boundary.', $delivery->sequence,
                        reason: 'cooperative_cancellation_boundary_mismatch',
                    );
                }
                if ($delivery !== null && $delivery->sequence === $nextSequence) {
                    if ($boundary != $delivery || $context->isCancellationShielded()
                        || !$this->isRootCancellationBoundary($suspended, $delivery, $stepsBySequence, $context)) {
                        throw new NonDeterministicWorkflow(
                            'Committed cancellation call kind, operation range or shielding changed.', $delivery->sequence,
                            reason: 'cooperative_cancellation_boundary_mismatch',
                        );
                    }
                    $this->assertCancellationCallMatches($suspended, $delivery, $stepsBySequence, $context);
                    while (isset($steps[$stepCursor]) && $steps[$stepCursor]['sequence'] < $delivery->sequence + $delivery->sequenceSpan) {
                        if ($steps[$stepCursor]['sequence'] < $delivery->sequence) {
                            throw new NonDeterministicWorkflow('Cancellation skipped an earlier authored operation.', $steps[$stepCursor]['sequence']);
                        }
                        ++$stepCursor;
                    }
                    $nextSequence = $delivery->sequence + $delivery->sequenceSpan;
                    $cancellationConsumed = true;
                    $context->observeCancellationReplayTime($cancellation->deliveryIndex === null
                        ? null : ($history[$cancellation->deliveryIndex] ?? null));
                    $suspended = $this->throwIntoExecution($execution, $businessReplayClock, $context->deliveredCancellation($delivery->requestId, $cancellation->request->context));
                    continue;
                }
                if ($delivery === null && $boundary !== null && !$context->isCancellationShielded()
                    && $cancellation->eligible(
                        $boundary->operationSequence ?? $boundary->sequence,
                        $boundary->operationSequence === null ? $boundary->sequenceSpan : $boundary->operationSequenceSpan,
                    )) {
                    if ($this->isRootCancellationBoundary($suspended, $boundary, $stepsBySequence, $context)) {
                        $this->assertCancellationCallMatches($suspended, $boundary, $stepsBySequence, $context);

                        return new ReplayResult(
                            $commands, $context->messageStreamCursorAcknowledgements(), $context->messageStreamPendingWaits(),
                            cancellationDelivery: $boundary,
                        );
                    }
                    if ($context->currentCancellationScopeId() === 'root') {
                        throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: root delivery cannot replace deferred scoped group or selection members.');
                    }
                }
            }
            if ($suspended instanceof ParallelWorkflowCommand) {
                $commandsBeforeGroup = count($commands);
                $baseSequence = !$strictSequences && isset($steps[$stepCursor])
                    ? $steps[$stepCursor]['sequence']
                    : $nextSequence;
                $descriptors = $suspended->leafDescriptors($baseSequence);
                $preparedGroup = false;
                if ($prepareLocalActivities) {
                    foreach ($descriptors as $descriptor) {
                        if ($descriptor['operation']->command->type === 'record_local_activity') {
                            if (!$prepareLocalActivityGroups || $suspended->mode !== 'all') {
                                throw new WorkflowClaimAborted(
                                    'prepared_local_parallel_admission_unavailable: a complete all group requires the negotiated atomic admission capability.',
                                );
                            }
                            $preparedGroup = true;
                        }
                    }
                }
                if ($preparedGroup && count($descriptors) > 100) {
                    throw new WorkflowClaimAborted('prepared_local_group_limit_exceeded: atomic admission supports at most 100 members.');
                }
                if ($preparedGroup) {
                    foreach ($descriptors as $descriptor) {
                        if (in_array($descriptor['operation']->command->type, ['open_condition_wait', 'open_signal_wait'], true)) {
                            throw new WorkflowClaimAborted('prepared_local_group_wait_admission_unavailable: this atomic group cannot include turn-closing waits.');
                        }
                    }
                }
                $preparedCalls = [];
                $results = [];
                $pending = false;
                $matched = 0;
                $failure = null;
                $failures = [];
                $resolvedOrders = [];
                $clockMembers = [];
                $missingMember = false;

                foreach ($descriptors as $offset => $descriptor) {
                    $command = $descriptor['operation']->command;
                    $path = $descriptor['group_path'];
                    $step = $steps[$stepCursor + $offset] ?? null;
                    if ($step === null) {
                        $missingMember = true;
                        if ($preparedGroup) {
                            continue;
                        }
                        $metadata = $path[array_key_last($path)] ?? [];
                        $command = $command->withAttributes([
                            ...$metadata,
                            'parallel_group_path' => $path,
                        ]);
                        if (($encodingFailure = $this->appendWorkflowCommand(
                            $commands,
                            $command,
                            $taskQueue,
                            $context,
                        )) !== null) {
                            return $encodingFailure;
                        }
                        continue;
                    }

                    ++$matched;
                    if ($strictSequences && $step['sequence'] !== $baseSequence + $offset) {
                        throw new NonDeterministicWorkflow('Recorded parallel member belongs to a different authored call.', $baseSequence + $offset);
                    }
                    $this->assertCommandMatchesStep($command, $step);
                    $this->assertParallelPathMatches($step, $path);
                    if ($command->type === 'open_condition_wait') {
                        $this->assertConditionWaitCompatible($step, $command);
                        $command = $command->withRecordedConditionDefinition($step['condition_definition_fingerprint']);
                    }
                    $nextSequence = max($nextSequence, $step['sequence'] + 1);

                    if (!$step['resolved']) {
                        if ($preparedGroup && $command->type === 'record_local_activity') {
                            $metadata = $path[array_key_last($path)] ?? [];
                            $preparedCalls[] = $this->preparedLocalCall($command->withAttributes([
                                ...$metadata, 'parallel_group_path' => $path,
                            ]), $step['sequence'], PreparedLocalActivityCall::needsRecovery($history, $step['sequence']),
                                $history, $cancellation, $context, $cancellationConsumed);
                        }
                        if ($command->type === 'open_condition_wait') {
                            if ($command->conditionSatisfied()) {
                                $results[$offset] = true;
                                continue;
                            }

                            $metadata = $path[array_key_last($path)] ?? [];
                            // Grouped reopens keep the authored timeout: replay of a
                            // grouped physical reopen is not qualified yet (#278).
                            $command = $command->withAttributes([
                                ...$metadata,
                                'parallel_group_path' => $path,
                            ]);
                            if (($encodingFailure = $this->appendWorkflowCommand(
                                $commands,
                                $command,
                                $taskQueue,
                                $context,
                            )) !== null) {
                                return $encodingFailure;
                            }
                        }
                        $pending = true;
                        continue;
                    }
                    foreach ($this->blockingResolutionOrders($step) as $order) {
                        $resolvedOrders[] = $order;
                    }
                    $clockMembers[] = ['step' => $step, 'path' => $path, 'offset' => $offset];
                    if ($step['failure'] instanceof Throwable) {
                        $failures[$offset] = $step['failure'];
                        if ($failure === null
                            || $step['resolution_order'] < $failure['resolution_order']
                            || ($step['resolution_order'] === $failure['resolution_order'] && $offset < $failure['offset'])) {
                            $failure = [
                                'exception' => $step['failure'],
                                'resolution_order' => $step['resolution_order'],
                                'offset' => $offset,
                            ];
                        }
                        continue;
                    }

                    $results[$offset] = $step['value'];
                }

                $stepCursor += $matched;
                $nextSequence = max($nextSequence, $baseSequence + count($descriptors));
                if ($preparedGroup && $missingMember) {
                    if ($matched !== 0) {
                        throw new NonDeterministicWorkflow(
                            'Prepared local group history is missing a declared member.', $baseSequence,
                            'all parallel members scheduled', 'partial prepared local group',
                            'parallel_group_partially_scheduled',
                        );
                    }
                    $groupCommands = [];
                    foreach ($descriptors as $offset => $descriptor) {
                        $path = $descriptor['group_path'];
                        $metadata = $path[array_key_last($path)] ?? [];
                        $command = $descriptor['operation']->command->withAttributes([
                            ...$metadata, 'parallel_group_path' => $path,
                        ]);
                        if ($command->type === 'record_local_activity') {
                            $call = $this->preparedLocalCall($command, $baseSequence + $offset, false,
                                $history, $cancellation, $context, $cancellationConsumed);
                            $preparedCalls[] = $call;
                            $groupCommands[] = [...$call->descriptor($this->codec), 'type' => 'prepare_local_activity'];
                        } else {
                            $groupCommands[] = $command->toWire($this->codec, $taskQueue);
                        }
                    }
                    json_encode($groupCommands, JSON_THROW_ON_ERROR);

                    return $this->result($commands, $context, preparedLocalActivityGroup: new PreparedLocalActivityGroup(
                        $baseSequence, count($descriptors), $groupCommands, $preparedCalls, false,
                    ));
                }
                if ($missingMember && $failure !== null) {
                    throw new NonDeterministicWorkflow(
                        'Parallel history contains a failure before every declared member was durably scheduled.',
                        $baseSequence,
                        'all parallel members scheduled',
                        'failed group with missing members',
                        'parallel_group_partially_scheduled',
                    );
                }
                if ($suspended->mode === 'select') {
                    $groupId = $descriptors[0]['group_path'][0]['parallel_group_id'] ?? null;
                    $winner = is_string($groupId) ? ($selectionResolutions[$groupId] ?? null) : null;
                    if (is_array($winner)) {
                        $winner = $this->validatedSelectionResolution(
                            $suspended,
                            $baseSequence,
                            $winner,
                            $stepsBySequence,
                            $history,
                            $selectionOperationIdentities,
                        );
                        ksort($results);
                        ksort($failures);
                        $selection = $suspended->selectionResult(
                            $baseSequence,
                            $winner,
                            $results,
                            $failures,
                            $context->workflowId,
                            $context->runId,
                            $selectionOperationIdentities,
                        );
                        $this->validateSelectionCancellationsForHandles(
                            $selection->handles,
                            $selectionCancellations,
                        );
                        $this->advanceCancellationClock($context, $history, [$selectionResolutionOrders[(string) $groupId]]);
                        $suspended = $this->resumeExecution($execution, $businessReplayClock, $selection);
                        continue;
                    }
                    return $this->result($commands, $context);
                }
                if ($failure !== null) {
                    $this->advanceCancellationClock($context, $history, array_values(array_filter($resolvedOrders,
                        static fn (int $order): bool => $order <= $failure['resolution_order'])));
                    $suspended = $this->throwGroupIntoExecution($execution, $businessReplayClock, $failure['exception'],
                        $clockMembers, count($descriptors), $failure['resolution_order']);
                    continue;
                }
                if ($preparedCalls !== []) {
                    return $this->result($commands, $context, preparedLocalActivityGroup: new PreparedLocalActivityGroup(
                        $baseSequence, count($descriptors), [], $preparedCalls, true,
                    ));
                }
                if ($pending || count($commands) > $commandsBeforeGroup) {
                    return $this->result($commands, $context);
                }

                ksort($results);
                $this->advanceCancellationClock($context, $history, $resolvedOrders);
                $suspended = $this->resumeGroupExecution($execution, $businessReplayClock,
                    $suspended->nestedResults(array_values($results)), $clockMembers, count($descriptors));
                continue;
            }

            if ($suspended instanceof DurableOperationHandle) {
                $this->assertDurableHandleExecutionIdentity($suspended, $context);
                $resolution = $this->durableHandleResolution(
                    $suspended,
                    $stepsBySequence,
                    $selectionCancellations,
                    $selectionCancellationOrders,
                );
                if (!$resolution['resolved']) {
                    return $this->result($commands, $context);
                }
                $this->advanceCancellationClock($context, $history, $resolution['resolution_orders']);
                $suspended = $resolution['failure'] instanceof Throwable
                    ? $this->throwIntoExecution($execution, $businessReplayClock, $resolution['failure'])
                    : $this->resumeExecution($execution, $businessReplayClock, $resolution['value']);
                continue;
            }

            if ($suspended instanceof CancelDurableOperationCommand) {
                $this->assertDurableHandleExecutionIdentity($suspended->handle, $context);
                if ($this->selectionCancellationForHandle($suspended->handle, $selectionCancellations) === null) {
                    $commands[] = $suspended->toWire();
                }
                $suspended = $this->resumeExecution($execution, $businessReplayClock, null);
                continue;
            }

            if (!$suspended instanceof WorkflowCommand) {
                throw new NonDeterministicWorkflow('Workflow suspended with an unsupported value instead of WorkflowCommand.');
            }

            if ($suspended->type === 'open_cancellation_scope') {
                $step = $steps[$stepCursor] ?? null;
                if ($step === null) {
                    if ($completedHistory) {
                        throw new NonDeterministicWorkflow('Completed history has no authored scope opening.', $nextSequence);
                    }

                    return $this->result($commands, $context, cancellationScopeOpening: new CancellationScopeOpening(
                        $nextSequence, (string) $suspended->attributes['parent_scope_id'], (bool) $suspended->attributes['shield_parent'],
                    ));
                }
                if ($step['shape'] !== 'cancellation_scope' || $step['sequence'] !== $nextSequence
                    || ($step['scope_parent'] ?? null) !== $suspended->attributes['parent_scope_id']
                    || ($step['scope_shield'] ?? null) !== $suspended->attributes['shield_parent']) {
                    throw new NonDeterministicWorkflow('Authored scope opening, parent or shielding changed.', $nextSequence,
                        reason: 'cancellation_scope_opening_changed');
                }
                ++$stepCursor;
                ++$nextSequence;
                $suspended = $this->resumeExecution($execution, $businessReplayClock, $step['value']);
                continue;
            }
            if ($suspended->type === 'continue_as_new') {
                $this->assertCancellationConsumed($cancellation, $cancellationConsumed);
                $this->assertNoRemainingSteps($steps, $stepCursor, 'continue_as_new');
                if (($encodingFailure = $this->appendWorkflowCommand(
                    $commands,
                    $suspended,
                    $taskQueue,
                    $context,
                )) !== null) {
                    return $encodingFailure;
                }

                return $this->result($commands, $context);
            }

            if ($suspended->type === 'record_version_marker') {
                $changeId = (string) $suspended->attributes['change_id'];
                $decision = $versionDecisions[$changeId] ?? null;
                if ($decision !== null) {
                    $this->assertVersionDecisionCompatible($changeId, $decision, $suspended);
                    $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->versionResult($decision['version']));
                    continue;
                }

                $step = $steps[$stepCursor] ?? null;
                if ($step !== null) {
                    if ($step['shape'] !== 'version_marker') {
                        $version = -1;
                        $this->assertVersionSupported(
                            $changeId,
                            $version,
                            (int) $suspended->attributes['min_supported'],
                            (int) $suspended->attributes['max_supported'],
                            $step['sequence'],
                        );
                        $versionDecisions[$changeId] = [
                            'version' => $version,
                            'sequence' => $step['sequence'],
                        ];
                        $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->versionResult($version));
                        continue;
                    }
                    if ($step['detail'] !== $changeId) {
                        throw new NonDeterministicWorkflow(
                            "Recorded version marker {$step['detail']} does not match change ID {$changeId}.",
                            $step['sequence'],
                            $step['detail'],
                            $changeId,
                            'version_change_id_mismatch',
                        );
                    }
                    $version = (int) $step['value'];
                    $this->assertVersionSupported(
                        $changeId,
                        $version,
                        (int) $suspended->attributes['min_supported'],
                        (int) $suspended->attributes['max_supported'],
                        $step['sequence'],
                    );
                    ++$stepCursor;
                    $nextSequence = max($nextSequence, $step['sequence'] + 1);
                    $versionDecisions[$changeId] = [
                        'version' => $version,
                        'sequence' => $step['sequence'],
                    ];
                    $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->versionResult($version));
                    continue;
                }

                if ($completedHistory) {
                    $version = -1;
                    $this->assertVersionSupported(
                        $changeId,
                        $version,
                        (int) $suspended->attributes['min_supported'],
                        (int) $suspended->attributes['max_supported'],
                        null,
                    );
                    $versionDecisions[$changeId] = [
                        'version' => $version,
                        'sequence' => null,
                    ];
                    $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->versionResult($version));
                    continue;
                }

                $version = (int) $suspended->attributes['max_supported'];
                if (($encodingFailure = $this->appendWorkflowCommand(
                    $commands,
                    $suspended,
                    $taskQueue,
                    $context,
                )) !== null) {
                    return $encodingFailure;
                }
                ++$nextSequence;
                $versionDecisions[$changeId] = [
                    'version' => $version,
                    'sequence' => null,
                ];
                $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->versionResult($version));
                continue;
            }

            if ($suspended->type === 'await_child_workflow') {
                // A started child's await consumes no sequence: it reads the
                // original call's resolution, through continuation and retries.
                $awaited = $stepsBySequence[$suspended->attributes['sequence']] ?? null;
                if (($awaited['shape'] ?? null) !== 'child_workflow') {
                    throw new NonDeterministicWorkflow('An awaited child workflow has no recorded child call.',
                        (int) $suspended->attributes['sequence'], reason: 'child_workflow_handle_mismatch');
                }
                if ($awaited['resolved'] !== true) {
                    return $this->result($commands, $context);
                }
                $this->advanceCancellationClock($context, $history, $this->blockingResolutionOrders($awaited));
                $suspended = $awaited['failure'] instanceof Throwable
                    ? $this->throwIntoExecution($execution, $businessReplayClock, $awaited['failure'], $awaited)
                    : $this->resumeExecution($execution, $businessReplayClock, $awaited['value'], $awaited);
                continue;
            }

            $step = $steps[$stepCursor] ?? null;
            if ($step !== null) {
                if ($strictSequences && $step['sequence'] !== $nextSequence) {
                    throw new NonDeterministicWorkflow('Recorded result belongs to a different authored call.', $nextSequence);
                }
                $this->assertCommandMatchesStep($suspended, $step);
                if ($step['value'] instanceof ServiceOperationHistory) {
                    $step = array_replace($step, $step['value']->delivery($suspended));
                }
                if ($step['parallel_path'] !== []) {
                    throw new NonDeterministicWorkflow(
                        'Recorded parallel-group history was replaced by a sequential workflow operation.',
                        $step['sequence'],
                        'parallel barrier',
                        $suspended->historyShape,
                        'parallel_group_missing_from_workflow',
                    );
                }
                ++$stepCursor;
                $nextSequence = max($nextSequence, $step['sequence'] + 1);
                if ($suspended->type === 'start_child_workflow' && ($suspended->attributes['await'] ?? null) === 'started') {
                    $identity = $this->stepChildIdentity($step);
                    if ($identity === null) {
                        throw new NonDeterministicWorkflow('A committed child start lacks its child call, instance or run identity.',
                            $step['sequence'], reason: 'child_start_identity_missing');
                    }
                    $suspended = $this->resumeExecution($execution, $businessReplayClock, new ChildWorkflowHandle($context, $step['sequence'], $identity));
                    continue;
                }
                if ($suspended->type === 'open_condition_wait') {
                    $this->assertConditionWaitCompatible($step, $suspended);
                    $nextStep = $steps[$stepCursor] ?? null;
                    if ($cancellation->request !== null && $nextStep !== null
                        && $this->isConditionReopen($step, $nextStep)) {
                        continue;
                    }
                    if ($step['resolved']) {
                        $this->advanceCancellationClock($context, $history, $this->blockingResolutionOrders($step));
                        $consumedConditionTail = $this->conditionTailAfterConsumption($step, $suspended, $step['value'] === true);
                        $suspended = $this->resumeExecution($execution, $businessReplayClock, $step['value'], $step);
                        continue;
                    }
                    if ($suspended->conditionSatisfied()) {
                        $consumedConditionTail = $this->conditionTailAfterConsumption($step, $suspended, true);
                        $suspended = $this->resumeExecution($execution, $businessReplayClock, true);
                        continue;
                    }

                    if (($encodingFailure = $this->appendWorkflowCommand(
                        $commands,
                        $this->conditionReopen($suspended, $step, $history),
                        $taskQueue,
                        $context,
                    )) !== null) {
                        return $encodingFailure;
                    }

                    return $this->result($commands, $context);
                }
                if ($step['resolved'] === false) {
                    if ($prepareLocalActivities && $suspended->type === 'record_local_activity') {
                        return $this->result($commands, $context, preparedLocalActivity: $this->preparedLocalCall(
                            $suspended, $step['sequence'], PreparedLocalActivityCall::needsRecovery($history, $step['sequence']),
                            $history, $cancellation, $context, $cancellationConsumed,
                        ));
                    }
                    return $this->result($commands, $context);
                }
                if ($prepareLocalActivities || $suspended->type !== 'record_local_activity') {
                    $this->advanceCancellationClock($context, $history, $this->blockingResolutionOrders($step));
                }
                if ($step['failure'] instanceof Throwable) {
                    try {
                        $suspended = $this->throwIntoExecution($execution, $businessReplayClock, $step['failure'], $step);
                    } catch (Throwable $failure) {
                        $activityId = $failure instanceof ActivityFailed && is_array($failure->failure)
                            ? ($failure->failure['activity_execution_id'] ?? null)
                            : null;
                        if ($failure === $step['failure']
                            && $failure instanceof ActivityFailed
                            && $failure->historyEventType === 'ActivityFailed'
                            && is_string($activityId)
                            && $activityId !== ''
                        ) {
                            return $this->result($commands, $context, $failure, $step['sequence'], $activityId);
                        }

                        throw $failure;
                    }
                } else {
                    $suspended = $this->resumeExecution($execution, $businessReplayClock, $step['value'], $step);
                }
                continue;
            }

            if ($suspended->type === 'record_side_effect') {
                $suspended = $suspended->resolveSideEffect();
            }
            if ($suspended->type === 'record_local_activity') {
                if ($prepareLocalActivities) {
                    return $this->result($commands, $context, preparedLocalActivity: $this->preparedLocalCall(
                        $suspended, $nextSequence, false, $history, $cancellation, $context, $cancellationConsumed,
                    ));
                }
                try {
                    $suspended = $suspended->resolveLocalActivity($this->codec);
                } catch (Throwable $failure) {
                    return $this->payloadEncodingFailure($commands, $context, $failure);
                }
                if (($encodingFailure = $this->appendWorkflowCommand(
                    $commands,
                    $suspended,
                    $taskQueue,
                    $context,
                )) !== null) {
                    return $encodingFailure;
                }
                ++$nextSequence;
                try {
                    if ($suspended->localResult instanceof Throwable) {
                        $suspended = $this->throwIntoExecution($execution, $businessReplayClock, $suspended->localResult);
                    } else {
                        $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->localResult);
                    }
                } catch (Throwable $failure) {
                    return $this->result($commands, $context, $failure);
                }
                continue;
            }
            if ($suspended->type === 'open_condition_wait') {
                $suspended = $this->anchorNewConditionChain($suspended);
            }
            if (($encodingFailure = $this->appendWorkflowCommand(
                $commands,
                $suspended,
                $taskQueue,
                $context,
            )) !== null) {
                return $encodingFailure;
            }
            ++$nextSequence;
            if (in_array($suspended->type, ['record_side_effect', 'upsert_memo', 'upsert_search_attributes'], true)) {
                $suspended = $this->resumeExecution($execution, $businessReplayClock, $suspended->localResult);
                continue;
            }

            return $this->result($commands, $context);
        }

        $this->assertCancellationConsumed($cancellation, $cancellationConsumed);
        foreach ($scopeDeliveries as $sequence => $_delivery) {
            if (!isset($consumedScopeDeliveries[$sequence])) {
                throw new NonDeterministicWorkflow('Workflow terminated without replaying its committed scope cancellation boundary.', $sequence,
                    reason: 'cancellation_scope_boundary_mismatch');
            }
        }
        if ($prepareCancellationScopeDelivery) {
            foreach ($committedScopes->preparations as $preparation) {
                if (!isset($consumedScopeDeliveries[$preparation['boundary']->sequence])) {
                    throw new NonDeterministicWorkflow('Workflow terminated without selecting its original prepared scope boundary.',
                        $preparation['boundary']->sequence, reason: 'cancellation_scope_boundary_mismatch');
                }
            }
        }
        $this->assertNoRemainingSteps($steps, $stepCursor, 'complete_workflow');
        $result = $execution->getReturn();
        if ($result instanceof \Generator) {
            throw new LogicException(
                'Workflow handlers must call WorkflowContext operations directly; Generator results are not supported.',
            );
        }
        if (($encodingFailure = $this->appendCompleteCommand($commands, $result, $context)) !== null) {
            return $encodingFailure;
        }

        return $this->result($commands, $context);
    }

    /** @param array<string, mixed>|null $step */
    private function unrecordedConditionResult(WorkflowCommand $command, ?array $step): ?bool
    {
        if ($command->type !== 'open_condition_wait'
            || ($step !== null && $step['shape'] === 'condition_wait'
                && $step['condition_key'] === $this->stringValue($command->attributes['condition_key'] ?? null))) {
            return null;
        }

        // Immediate sequential conditions have no durable step. Leave the next
        // history entry and sequence untouched, including cancellation boundaries.
        // A recorded wait still owns its original identity and resolved outcome.
        if ($command->conditionSatisfied()) {
            return true;
        }

        return ($command->attributes['timeout_seconds'] ?? null) === 0 ? false : null;
    }

    /** @param list<array<string, mixed>> $history */
    private function preparedLocalCall(
        WorkflowCommand $command,
        int $sequence,
        bool $recover,
        array $history,
        CancellationHistory $cancellation,
        WorkflowContext $context,
        bool $cancellationConsumed,
    ): PreparedLocalActivityCall {
        if ($context->cancellationContext() instanceof ScopedCancellationContext) {
            if (!$context->isCancellationShielded()) {
                throw new NonDeterministicWorkflow('Prepared scoped cleanup requires a shield after canonical delivery.', $sequence,
                    reason: 'local_activity_cleanup_authority_missing');
            }

            return PreparedLocalActivityCall::fromCommittedScopeDelivery(
                $command, $sequence, $recover, $history, $context->runId, $context->workflowId,
            );
        }
        if ($cancellation->request === null) {
            return new PreparedLocalActivityCall($command, $sequence, $recover);
        }
        $delivery = $cancellation->delivery;
        $eventId = $cancellation->deliveryIndex === null ? null : ($history[$cancellation->deliveryIndex]['id'] ?? null);
        if (!$cancellationConsumed || !$context->isCancellationShielded()
            || $delivery === null || $cancellation->request->context === null
            || $sequence < $delivery->sequence + $delivery->sequenceSpan
            || !is_string($eventId) || trim($eventId) === '') {
            throw new NonDeterministicWorkflow(
                'Prepared local cleanup requires a shield after canonical cancellation delivery.', $sequence,
                reason: 'local_activity_cleanup_authority_missing',
            );
        }

        return new PreparedLocalActivityCall(
            $command, $sequence, $recover, $cancellation->request->context, $eventId,
        );
    }

    /**
     * Materialize and JSON-validate a command before adding it to the task
     * completion. Once a local activity has run, a later payload failure must
     * return its durable record instead of discarding it on task redelivery.
     *
     * @param list<array<string, mixed>> $commands
     */
    private function appendWorkflowCommand(
        array &$commands,
        WorkflowCommand $command,
        string $taskQueue,
        ?WorkflowContext $context,
    ): ?ReplayResult {
        try {
            $wire = $command->toWire($this->codec, $taskQueue);
            json_encode($wire, JSON_THROW_ON_ERROR);
        } catch (Throwable $failure) {
            return $this->payloadEncodingFailure($commands, $context, $failure);
        }

        $commands[] = $wire;

        return null;
    }

    /** @param list<array<string, mixed>> $commands */
    private function appendCompleteCommand(
        array &$commands,
        mixed $result,
        ?WorkflowContext $context,
    ): ?ReplayResult {
        try {
            $wire = $this->completeCommand($result);
            json_encode($wire, JSON_THROW_ON_ERROR);
        } catch (Throwable $failure) {
            return $this->payloadEncodingFailure($commands, $context, $failure);
        }

        $commands[] = $wire;

        return null;
    }

    /** @param list<array<string, mixed>> $commands */
    private function payloadEncodingFailure(
        array $commands,
        ?WorkflowContext $context,
        Throwable $failure,
    ): ReplayResult {
        foreach ($commands as $command) {
            if (($command['type'] ?? null) === 'record_local_activity') {
                return $this->result($commands, $context, $failure);
            }
        }

        throw $failure;
    }

    /** @param list<array<string, mixed>> $commands */
    private function result(
        array $commands,
        ?WorkflowContext $context,
        ?Throwable $terminalFailure = null,
        ?int $failedActivitySequence = null,
        ?string $failedActivityExecutionId = null,
        ?PreparedLocalActivityCall $preparedLocalActivity = null,
        ?PreparedLocalActivityGroup $preparedLocalActivityGroup = null,
        ?CancellationScopeOpening $cancellationScopeOpening = null,
    ): ReplayResult {
        return new ReplayResult(
            $commands,
            $context?->messageStreamCursorAcknowledgements() ?? [],
            $context?->messageStreamPendingWaits() ?? [],
            $terminalFailure,
            $failedActivitySequence,
            $failedActivityExecutionId,
            preparedLocalActivity: $preparedLocalActivity,
            preparedLocalActivityGroup: $preparedLocalActivityGroup,
            cancellationScopeOpening: $cancellationScopeOpening,
        );
    }

    /** @param array<int, array<string, mixed>> $stepsBySequence */
    private function cancellationBoundary(
        mixed $command,
        int $sequence,
        array $stepsBySequence,
        string $requestId,
        ?CancellationDelivery $delivery = null,
    ): ?CancellationDelivery {
        $span = 1;
        $operationSequence = null;
        $operationSpan = 1;
        if ($command instanceof ParallelWorkflowCommand) {
            $kind = 'parallel';
            $span = count($command->leafDescriptors($sequence));
        } elseif ($command instanceof DurableOperationHandle) {
            $kind = 'selection_handle';
            $operationSequence = $command->baseSequence;
            $operationSpan = $command->size;
        } elseif ($command instanceof WorkflowCommand) {
            $kind = match ($command->type) {
                'schedule_activity' => 'activity',
                'record_local_activity' => 'local_activity',
                'start_timer' => 'timer',
                'start_child_workflow' => 'child',
                'start_service_operation' => ($command->attributes['wait_for'] ?? 'completed') !== 'accepted'
                    && ($command->attributes['mode_override'] ?? 'sync') !== 'async' ? 'service_operation' : null,
                'open_condition_wait' => isset($stepsBySequence[$sequence])
                    || $delivery?->sequence === $sequence
                    || (!$command->conditionSatisfied() && ($command->attributes['timeout_seconds'] ?? null) !== 0)
                        ? 'condition' : null,
                default => null,
            };
        } else {
            $kind = null;
        }
        if ($kind === null) {
            return null;
        }

        try {
            return CancellationDelivery::fromPayload([
                'workflow_command_id' => $requestId,
                'sequence' => $sequence,
                'call_kind' => $kind,
                'sequence_span' => $span,
                'operation_sequence' => $operationSequence,
                'operation_sequence_span' => $operationSpan,
            ]);
        } catch (\InvalidArgumentException $error) {
            throw new NonDeterministicWorkflow($error->getMessage(), $sequence, reason: 'cooperative_cancellation_boundary_mismatch');
        }
    }

    /** @param array<int, array<string, mixed>> $stepsBySequence */
    private function assertPendingScopeGroup(ParallelWorkflowCommand $group, CancellationDelivery $boundary,
        array $stepsBySequence, string $scopeId): void
    {
        if ($group->mode !== 'all') {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: scope selection groups require their own qualified delivery contract.');
        }
        foreach ($group->leafDescriptors($boundary->sequence) as $offset => $descriptor) {
            $command = $descriptor['operation']->command;
            $step = $stepsBySequence[$boundary->sequence + $offset] ?? null;
            if ($step === null || !in_array($command->type, ['schedule_activity', 'start_timer', 'start_child_workflow', 'open_condition_wait'], true)
                || ($command->attributes['cancellation_scope_id'] ?? 'root') !== $scopeId
                || array_filter($descriptor['group_path'], static fn (array $path): bool => ($path['parallel_group_mode'] ?? 'all') !== 'all') !== []) {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: scope all-groups require every original qualified member in one authored scope.');
            }
        }
    }

    /** @param array<int, array<string, mixed>> $stepsBySequence */
    private function assertCancellationCallMatches(
        mixed $command,
        CancellationDelivery $boundary,
        array $stepsBySequence,
        ?WorkflowContext $context,
    ): void {
        if ($command instanceof DurableOperationHandle) {
            $this->assertDurableHandleExecutionIdentity($command, $context);
            return;
        }
        if ($command instanceof ParallelWorkflowCommand) {
            $descriptors = $command->leafDescriptors($boundary->sequence);
        } elseif ($command instanceof WorkflowCommand) {
            $descriptors = [['operation' => new DeferredWorkflowOperation($command), 'group_path' => []]];
        } else {
            throw new NonDeterministicWorkflow('Cancellation requires an authored durable operation.', $boundary->sequence);
        }
        foreach ($descriptors as $offset => $descriptor) {
            $step = $stepsBySequence[$boundary->sequence + $offset] ?? null;
            if ($step === null) {
                continue;
            }
            $leaf = $descriptor['operation']->command;
            $this->assertCommandMatchesStep($leaf, $step);
            if ($command instanceof ParallelWorkflowCommand) {
                $this->assertParallelPathMatches($step, $descriptor['group_path']);
            } elseif ($step['parallel_path'] !== []) {
                throw new NonDeterministicWorkflow('Cancellation replaced a recorded parallel group with a sequential call.', $step['sequence']);
            }
            if ($leaf->type === 'open_condition_wait') {
                $this->assertConditionWaitCompatible($step, $leaf);
            }
        }
    }

    private function assertCancellationConsumed(CancellationHistory $cancellation, bool $consumed): void
    {
        if ($cancellation->delivery !== null && !$consumed) {
            throw new NonDeterministicWorkflow(
                'Workflow terminated without replaying its committed cancellation boundary.',
                $cancellation->delivery->sequence,
                reason: 'cooperative_cancellation_boundary_mismatch',
            );
        }
    }

    /** @param array<int, array<string, mixed>> $stepsBySequence */
    private function isRootCancellationBoundary(
        mixed $command,
        CancellationDelivery $boundary,
        array $stepsBySequence,
        WorkflowContext $context,
    ): bool {
        if ($context->currentCancellationScopeId() !== 'root') {
            return false;
        }
        $base = $boundary->operationSequence ?? $boundary->sequence;
        if ($command instanceof DurableOperationHandle) {
            $command = $command->operation;
        }
        if ($command instanceof DeferredWorkflowOperation) {
            $command = $command->command;
        }
        if (!$command instanceof WorkflowCommand && !$command instanceof ParallelWorkflowCommand) {
            return false;
        }
        $leaves = $command instanceof ParallelWorkflowCommand ? $command->leafDescriptors($base)
            : [['operation' => new DeferredWorkflowOperation($command)]];
        foreach ($leaves as $offset => $leaf) {
            if (($leaf['operation']->command->attributes['cancellation_scope_id'] ?? 'root') !== 'root'
                || ($stepsBySequence[$base + $offset]['cancellation_scope_id'] ?? 'root') !== 'root') {
                return false;
            }
        }

        return true;
    }

    /** @param array<string, mixed> $payload */
    private function childIdentity(array $payload): ?ChildWorkflowIdentity
    {
        $call = $payload['child_call_id'] ?? null;
        $instance = $payload['child_workflow_instance_id'] ?? $payload['workflow_instance_id'] ?? null;
        $run = $payload['child_workflow_run_id'] ?? $payload['workflow_run_id'] ?? null;
        foreach ([$call, $instance, $run] as $value) {
            if (!is_string($value) || trim($value) === '') {
                return null;
            }
        }

        return new ChildWorkflowIdentity($call, $instance, $run);
    }

    /** @param array<string, mixed> $step */
    private function stepChildIdentity(array $step): ?ChildWorkflowIdentity
    {
        $identity = $step['child_identity'] ?? null;

        return $identity instanceof ChildWorkflowIdentity ? $identity : null;
    }

    /** @param array<string, mixed> $step */
    private function stepOccurrence(array $step): ?string
    {
        $occurrence = $step['occurrence_id'] ?? null;

        return is_string($occurrence) && $occurrence !== '' ? $occurrence : null;
    }

    /** @param array<string, mixed> $previous
     *  @param array<string, mixed> $next
     */
    private function isConditionReopen(array $previous, array $next): bool
    {
        return $previous['shape'] === 'condition_wait' && $next['shape'] === 'condition_wait'
            && $previous['condition_key'] === $next['condition_key']
            && $previous['condition_definition_fingerprint'] === $next['condition_definition_fingerprint']
            && ($previous['timeout_seconds'] === $next['timeout_seconds']
                || (is_int($previous['timeout_seconds']) && is_int($next['timeout_seconds'])
                    && $next['timeout_seconds'] < $previous['timeout_seconds']))
            && $this->conditionWaitStillLive($previous, $next)
            // Two physical opens with different recorded occurrences are two calls (#281).
            && ($this->stepOccurrence($previous) === null || $this->stepOccurrence($next) === null
                || $this->stepOccurrence($previous) === $this->stepOccurrence($next))
            // Grouped members are separate calls; grouped reopens are #278.
            && $previous['parallel_path'] === [] && $next['parallel_path'] === [];
    }

    /**
     * A later open continues a wait the Server satisfied (a false wake) or a
     * wait that was still open when the later one opened (a repeated open,
     * #269 part 2). What counts is the state at that open: a repeated open's
     * orphan may time out afterwards (#283) and is still the same wait. A wait
     * that timed out or failed before the later open is finished, so that
     * open is a separate call, such as a grace period.
     *
     * @param array<string, mixed> $previous
     * @param array<string, mixed> $next
     */
    private function conditionWaitStillLive(array $previous, array $next): bool
    {
        if ($previous['resolved'] !== true || ($previous['value'] === true && $previous['failure'] === null)) {
            return true;
        }
        $nextOpened = $next['opened_order'] ?? null;
        $resolved = $previous['resolution_order'] ?? null;

        return is_int($nextOpened) && is_int($resolved) && $resolved > $nextOpened;
    }

    /**
     * A physical reopen of one logical condition wait keeps that wait's first
     * opening time and authored timeout. Workers before #269 reopened with the
     * full timeout; later ones reopen with the time remaining, so a reopen may
     * record a smaller timeout than the wait it continues.
     *
     * @param list<array<string, mixed>> $steps
     * @return list<array<string, mixed>>
     */
    private function bindConditionReopens(array $steps): array
    {
        foreach ($steps as $index => $step) {
            $previous = $steps[$index - 1] ?? null;
            if ($previous !== null && $this->isConditionReopen($previous, $step)) {
                $this->assertConditionChainPolicy($previous, $step);
                $steps[$index]['timeout_seconds'] = $previous['timeout_seconds'];
                $steps[$index]['opened_at'] = $previous['opened_at'] ?? null;
                // The first open owns the logical wait's id. A first open from a
                // worker before #274 has none, and a later open must not lend it one.
                $steps[$index]['occurrence_id'] = $this->stepOccurrence($previous);
                // The chain keeps its first open's deadline policy (#269).
                $steps[$index]['deadline_policy'] = $previous['deadline_policy'] ?? null;
                $steps[$index]['timeout_budget'] = $previous['timeout_budget'] ?? null;
            } elseif (($step['shape'] ?? null) === 'condition_wait' && ($step['deadline_policy'] ?? null) !== null
                && $step['timeout_budget'] !== $step['timeout_seconds']) {
                // A chain's first anchored open authors its whole budget.
                throw new NonDeterministicWorkflow('Anchored condition wait opened with a budget other than its timeout.',
                    $step['sequence'], reason: 'condition_wait_occurrence_mismatch');
            }
        }

        return $steps;
    }

    /**
     * Every physical open of one anchored chain names the same policy, its
     * original authored budget and the chain's occurrence; a chain that began
     * without the policy never gains it (#269).
     *
     * @param array<string, mixed> $first The chain so far, carrying its first open's policy.
     * @param array<string, mixed> $reopen
     */
    private function assertConditionChainPolicy(array $first, array $reopen): void
    {
        $policy = $first['deadline_policy'] ?? null;
        if (($reopen['deadline_policy'] ?? null) !== $policy
            || ($policy !== null && (($reopen['timeout_budget'] ?? null) !== ($first['timeout_budget'] ?? null)
                || $this->stepOccurrence($first) === null || $this->stepOccurrence($reopen) !== $this->stepOccurrence($first)))) {
            throw new NonDeterministicWorkflow('Condition wait reopen changed its occurrence deadline policy or budget.',
                $reopen['sequence'], reason: 'condition_wait_occurrence_mismatch');
        }
    }

    /**
     * The recorded opt-in of one physical open. Both fields are present
     * together, with the only defined policy and a non-negative integer budget
     * no smaller than this open's non-negative remaining timeout, or both are
     * absent. The check runs on every physical open, before a reopen's timeout
     * is replaced by its chain's first-open value.
     *
     * @param array<string, mixed> $payload
     * @return array{policy: string, budget: int}|null
     */
    private function recordedConditionDeadlinePolicy(array $payload, int $sequence): ?array
    {
        $hasPolicy = array_key_exists('deadline_policy', $payload);
        $hasBudget = array_key_exists('condition_wait_timeout_budget_seconds', $payload);
        if (!$hasPolicy && !$hasBudget) {
            return null;
        }
        $policy = $payload['deadline_policy'] ?? null;
        $budget = $payload['condition_wait_timeout_budget_seconds'] ?? null;
        $timeout = $payload['timeout_seconds'] ?? null;
        $occurrence = $this->stringValue($payload['condition_wait_occurrence_id'] ?? null);
        if ($policy !== self::CONDITION_DEADLINE_POLICY || !is_int($budget) || $budget < 0
            || !is_int($timeout) || $timeout < 0 || $timeout > $budget || $occurrence === null) {
            throw new NonDeterministicWorkflow('Recorded condition wait has an invalid occurrence deadline policy.',
                $sequence, reason: 'condition_wait_deadline_policy_invalid');
        }

        return ['policy' => $policy, 'budget' => $budget];
    }

    /**
     * A new top-level chain opts in only when the Server advertises the
     * capability and the call has an occurrence id and a timeout. The budget is
     * the authored timeout; timeout_seconds stays the replay input (#269).
     * Grouped waits keep the authored timeout until grouped reopens are
     * qualified (#278).
     */
    private function anchorNewConditionChain(WorkflowCommand $command): WorkflowCommand
    {
        $timeout = $command->attributes['timeout_seconds'] ?? null;
        if (!$this->conditionOccurrenceDeadline || !is_int($timeout)
            || !is_string($command->attributes['condition_wait_occurrence_id'] ?? null)
            || array_key_exists('parallel_group_path', $command->attributes)) {
            return $command;
        }

        return $command->withAttributes([
            'deadline_policy' => self::CONDITION_DEADLINE_POLICY,
            'condition_wait_timeout_budget_seconds' => $timeout,
        ]);
    }

    /**
     * Keep the raw opens until workflow code has consumed the earlier result.
     * Equal keys/fingerprints alone cannot decide whether a later legacy open
     * was another false wake or a new authored call. Only a recorded satisfied
     * wait followed by a different recorded wait identity can supply a tail;
     * its own original timeout/opening is used if code actually authors it.
     *
     * @param array<string, mixed> $step
     * @return array{consumed: bool, occurrence: ?string, terminal: array<string, mixed>, steps: list<array<string, mixed>>}|null
     */
    private function conditionTailAfterConsumption(array $step, WorkflowCommand $command, bool $satisfied): ?array
    {
        $physical = $step['physical_condition_steps'] ?? [];
        foreach ($physical as $index => $previous) {
            if (!$this->isLegacySatisfiedCondition($previous)) {
                continue;
            }
            foreach (array_slice($physical, $index + 1) as $offset => $next) {
                if (is_int($next['opened_order'] ?? null)
                    && $next['opened_order'] > $previous['resolution_order']) {
                    return ['consumed' => $satisfied,
                        'occurrence' => $this->stringValue($command->attributes['condition_wait_occurrence_id'] ?? null),
                        'terminal' => $previous,
                        'steps' => array_slice($physical, $index + $offset + 1)];
                }
            }
        }

        return null;
    }

    /** @param array<string, mixed> $step */
    private function isLegacySatisfiedCondition(array $step): bool
    {
        return $this->stepOccurrence($step) === null && $step['resolved'] === true
            && $step['value'] === true && $step['failure'] === null
            && is_string($step['condition_definition_fingerprint'] ?? null)
            && preg_match('/\Asha256:[a-f0-9]{64}\z/', $step['condition_definition_fingerprint']) === 1;
    }

    /**
     * @param array{consumed: bool, occurrence: ?string, terminal: array<string, mixed>, steps: list<array<string, mixed>>} $pending
     * @return list<array<string, mixed>>
     */
    private function restoreConsumedConditionTail(mixed $command, array $pending): array
    {
        $first = $pending['steps'][0];
        if (!$command instanceof WorkflowCommand || $command->type !== 'open_condition_wait'
            || $first['condition_key'] !== $this->stringValue($command->attributes['condition_key'] ?? null)) {
            return [];
        }
        if ($this->stepOccurrence($first) !== null && $this->stepOccurrence($first) === $pending['occurrence']) {
            // A later canonical id can prove this was still the consumed call,
            // even when its first legacy open had no occurrence metadata.
            return [];
        }
        if (!$pending['consumed'] || !$this->conditionTerminalIdentityMatches($pending['terminal'])
            || !is_string($first['condition_wait_id'] ?? null)
            || ($first['condition_open_sequence'] ?? null) !== $first['sequence']
            || $first['condition_wait_id'] === $pending['terminal']['condition_wait_id']) {
            throw new NonDeterministicWorkflow(
                'Legacy condition history cannot distinguish this authored call from a physical reopen.',
                $first['sequence'], reason: 'legacy_condition_call_boundary_ambiguous',
            );
        }
        $this->assertConditionWaitCompatible($first, $command);
        $steps = $pending['steps'];
        foreach ($steps as $index => $step) {
            $steps[$index]['physical_condition_steps'] = [$step];
        }

        return $this->collapseConditionReopens($this->bindConditionReopens($steps));
    }

    /** @param array<string, mixed> $step */
    private function conditionTerminalIdentityMatches(array $step): bool
    {
        $terminal = $step['condition_terminal'] ?? [];

        return is_string($step['condition_wait_id'] ?? null)
            && ($step['condition_open_sequence'] ?? null) === $step['sequence']
            && ($terminal['condition_wait_id'] ?? null) === $step['condition_wait_id']
            && ($terminal['sequence'] ?? null) === $step['sequence']
            && is_int($step['opened_order'] ?? null) && $step['resolution_order'] > $step['opened_order'];
    }

    /**
     * The command that reopens a recorded condition wait, bounded by the wait's
     * original deadline as embedded await($condition, $timeout) is. The Server
     * satisfies an open wait on any signal and starts a new timer from each
     * physical open, so the full timeout would move the deadline forward.
     *
     * @param array<string, mixed> $step
     * @param list<array<string, mixed>> $history
     */
    private function conditionReopen(WorkflowCommand $command, array $step, array $history): WorkflowCommand
    {
        $command = $this->remainingConditionReopen($command, $step, $history);
        if (($step['deadline_policy'] ?? null) === self::CONDITION_DEADLINE_POLICY && is_int($step['timeout_budget'] ?? null)) {
            if (!$this->conditionOccurrenceDeadline) {
                // A Server that does not advertise the capability on this task
                // (an older binary after a rollback, or a withdrawn capability)
                // strips the policy fields and records a legacy reopen inside
                // the anchored chain, moving its wall deadline. Leave the claim
                // before any command is sent, without completing or failing the
                // task: it stays until its lease or another ordinary authority
                // transition ends it, so no retry task is created, and a capable
                // Server resumes the run.
                throw new WorkflowClaimDeferred('condition_wait_occurrence_deadline_unavailable: an anchored condition wait cannot reopen '
                    .'because this Server does not advertise condition_wait_occurrence_deadline.');
            }
            // Same chain, same policy: the Server keeps the first open's
            // deadline; timeout_seconds stays the remaining replay budget.
            return $command->withAttributes([
                'deadline_policy' => self::CONDITION_DEADLINE_POLICY,
                'condition_wait_timeout_budget_seconds' => $step['timeout_budget'],
            ]);
        }

        return $command;
    }

    /**
     * @param array<string, mixed> $step
     * @param list<array<string, mixed>> $history
     */
    private function remainingConditionReopen(WorkflowCommand $command, array $step, array $history): WorkflowCommand
    {
        $command = $command->withRecordedConditionDefinition($step['condition_definition_fingerprint']);
        $timeout = $step['timeout_seconds'] ?? null;
        $openedAt = $step['opened_at'] ?? null;
        if (!is_int($timeout) || !$openedAt instanceof \DateTimeImmutable) {
            return $command;
        }
        $now = null;
        foreach ($history as $event) {
            $time = $this->eventTime($event);
            if ($time !== null && ($now === null || $time > $now)) {
                $now = $time;
            }
        }
        if ($now === null) {
            return $command;
        }
        $elapsed = (float) $now->format('U.u') - (float) $openedAt->format('U.u');
        $remaining = max(0, min($timeout, (int) ceil($timeout - $elapsed)));

        return $command->withAttributes(['timeout_seconds' => $remaining]);
    }

    /** @param mixed $event */
    private function eventTime(mixed $event): ?\DateTimeImmutable
    {
        $value = is_array($event) ? ($event['timestamp'] ?? $event['recorded_at'] ?? null) : null;
        // RFC 3339 with an explicit offset only: "now" or a zoneless time would
        // read the wall clock or the host time zone and break determinism.
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            return null;
        }
        try {
            return new \DateTimeImmutable($value);
        } catch (\Exception) {
            return null;
        }
    }

    /** @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    private function assertCancellationScopeReplaySupported(array $history, bool $allowAuthoring, array $task, bool $replayCommittedScopes): bool
    {
        $hasScopes = false;
        $hasCancellation = ($task['cancellation_request'] ?? null) !== null || ($task['cancel_requested'] ?? false) === true;
        $hasRunRequest = ($task['cancellation_request'] ?? null) !== null;
        foreach ($history as $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $hasCancellation = $hasCancellation || in_array($kind, [CancellationHistory::REQUEST_EVENT, CancellationHistory::DELIVERY_EVENT], true);
            $hasRunRequest = $hasRunRequest || $kind === CancellationHistory::REQUEST_EVENT;
            if ($kind === 'CancellationScopeRequestConflicted'
                || (!$replayCommittedScopes && in_array($kind, ['CancellationScopeRequested', 'CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true))
                || (!$allowAuthoring && $kind === 'CancellationScopeOpened')) {
                throw new WorkflowClaimAborted(
                    'cancellation_scope_execution_not_supported: this PHP worker has not qualified canonical scope replay and delivery.',
                );
            }
            $hasScopes = $hasScopes || $kind === 'CancellationScopeOpened';
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            foreach ([$payload, $payload['activity'] ?? [], $payload['timer'] ?? [], $payload['child_workflow'] ?? []] as $snapshot) {
                if (is_array($snapshot) && array_key_exists('cancellation_scope_id', $snapshot)
                    && $snapshot['cancellation_scope_id'] !== 'root') {
                    $hasScopes = true;
                    if (!$allowAuthoring) {
                        throw new WorkflowClaimAborted(
                            'cancellation_scope_execution_not_supported: this PHP worker cannot execute scoped operations without qualified scope replay and delivery.',
                        );
                    }
                }
            }
        }
        if ($hasScopes && $hasCancellation && (!$replayCommittedScopes || !$hasRunRequest)) {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not qualified cancellation delivery into authored scopes.');
        }

        return $hasScopes;
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int,
     *     child_policies?: array<string, string>,
     *     activity_cancellation_policy?: string,
     *     cancellation_scope_id?: string,
     *     cancellation_cleanup?: array<string, string>,
     *     scope_parent?: string,
     *     scope_shield?: bool
     * }>
     */
    private function recordedSteps(array $history, bool $preserveConditionReopens, CancellationScopeHistory $scopes): array
    {
        $steps = [];
        $childPolicies = [];
        $activityPolicies = [];
        $timerCleanup = [];
        $conditionStepsByWaitId = [];
        $conditionOpenedAt = [];
        $childIdentities = [];
        $conditionOpenedOrder = [];
        $conditionOccurrences = [];
        $conditionWaitIds = [];
        $conditionOpenSequences = [];
        $conditionDeadlinePolicies = [];
        $conditionTerminals = [];
        $versionMarkerSequences = [];
        $versionMarkerChangeIds = [];
        $memoSequences = [];
        $commandKindsBySequence = [];
        $deliveryRanges = [];
        $upsertSequences = [];
        $fallbackSequence = 1_000_000;
        foreach ($history as $resolutionOrder => $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            $sequence = match ($type) {
                'VersionMarkerRecorded' => $this->versionMarkerSequence($payload),
                'MemoUpserted' => $this->memoSequence($payload),
                'SearchAttributesUpserted' => $this->searchAttributeSequence($payload),
                default => $this->sequence($payload) ?? $fallbackSequence++,
            };
            $key = (string) $sequence;

            $incomingShape = $this->historyEventShape($type, $payload);
            if (($steps[$key]['shape'] ?? null) === 'service_operation'
                && $incomingShape !== null && $incomingShape !== 'service_operation') {
                throw new NonDeterministicWorkflow('Service call collides with another durable command.', $sequence,
                    reason: 'durable_command_sequence_collision');
            }

            // One workflow sequence is one durable command: only rows of the
            // same command's lifecycle share it. A collision that involves an
            // upsert is refused in either order, whether or not the replay
            // parser below would build a step from the other row.
            if (in_array($type, self::DELIVERY_EVENTS, true)) {
                // A delivery claims its whole span. Compare ranges rather than
                // expanding them, so a large span costs nothing.
                [$first, $last] = $this->deliveryRange($type, $payload);
                foreach ($upsertSequences as $upsertSequence => $upsertKind) {
                    if ($upsertSequence >= $first && $upsertSequence <= $last) {
                        $this->refuseUpsertCollision($upsertSequence, $type, $upsertKind, 'cancellation_delivery');
                    }
                }
                $deliveryRanges[] = [$first, $last, $type];
            } elseif (($claim = $this->durableCommandKind($type, $payload)) !== null) {
                $kinds = $commandKindsBySequence[$key] ?? [];
                $kinds[$claim] = true;
                if (count($kinds) > 1 && array_intersect(array_keys($kinds), self::UPSERT_COMMAND_KINDS) !== []) {
                    $this->refuseUpsertCollision($sequence, $type, implode(', ', array_diff(array_keys($kinds), [$claim])), $claim);
                }
                $commandKindsBySequence[$key] = $kinds;
                if (in_array($claim, self::UPSERT_COMMAND_KINDS, true)) {
                    foreach ($deliveryRanges as [$first, $last]) {
                        if ($sequence >= $first && $sequence <= $last) {
                            $this->refuseUpsertCollision($sequence, $type, 'cancellation_delivery', $claim);
                        }
                    }
                    $upsertSequences[$sequence] = $claim;
                }
            }

            if ($type !== 'CancellationScopeOpened' && isset($scopes->openings[$sequence])
                && $this->historyEventShape($type, $payload) !== null) {
                throw new NonDeterministicWorkflow('Scope opening collides with another durable command.', $sequence,
                    reason: 'durable_command_sequence_collision');
            }

            if (in_array($type, ['ActivityScheduled', 'ActivityStarted', 'ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'], true)) {
                $policy = $activityPolicies[$key] ?? null;
                $snapshot = is_array($payload['activity'] ?? null) ? $payload['activity'] : [];
                foreach ([$payload, $snapshot] as $source) {
                    if (!array_key_exists('cancellation_policy', $source)) {
                        continue;
                    }
                    $incoming = $source['cancellation_policy'];
                    if (!is_string($incoming) || CancellationPolicy::tryFrom($incoming) === null) {
                        throw new NonDeterministicWorkflow(
                            'Recorded Activity cancellation_policy must be a supported policy.',
                            $sequence,
                            reason: 'invalid_activity_cancellation_policy_history',
                        );
                    }
                    if ($policy !== null && $policy !== $incoming) {
                        throw new NonDeterministicWorkflow(
                            'Recorded Activity cancellation_policy changed between history events.',
                            $sequence,
                            $policy,
                            $incoming,
                            'activity_cancellation_policy_history_conflict',
                        );
                    }
                    $policy = $incoming;
                }
                $activityPolicies[$key] = $policy ?? CancellationPolicy::TryCancel->value;
            }

            if (in_array($type, ['ChildWorkflowScheduled', 'ChildRunStarted', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true)) {
                try {
                    $incomingPolicies = WorkflowCommand::canonicalChildWorkflowPolicies($payload);
                } catch (\InvalidArgumentException $error) {
                    throw new NonDeterministicWorkflow($error->getMessage(), $sequence, reason: 'invalid_child_workflow_policy_history');
                }
                $previousPolicies = $childPolicies[$key] ?? null;
                foreach ($incomingPolicies as $field => $value) {
                    if ($previousPolicies !== null && $previousPolicies[$field] !== $value) {
                        throw new NonDeterministicWorkflow(
                            "Recorded child workflow {$field} changed between history events.",
                            $sequence,
                            $previousPolicies[$field],
                            $value,
                            'child_workflow_policy_history_conflict',
                        );
                    }
                }
                $childPolicies[$key] = array_merge(
                    ['parent_close_policy' => ParentClosePolicy::Abandon->value, 'cancellation_policy' => CancellationPolicy::Abandon->value],
                    $previousPolicies ?? [],
                    $incomingPolicies,
                );
            }

            if ($type !== 'VersionMarkerRecorded'
                && isset($versionMarkerSequences[$key])
                && $this->historyEventShape($type, $payload) !== null) {
                throw new NonDeterministicWorkflow(
                    "Workflow sequence {$sequence} contains a version marker and another durable command.",
                    $sequence,
                    'one durable command shape',
                    'version marker and '.$this->historyEventShape($type, $payload),
                    'durable_command_sequence_collision',
                );
            }

            if (in_array($type, ['ServiceCallStarted', 'ServiceCallCompleted', 'ServiceCallFailed', 'ServiceCallCancelled'], true)) {
                if (isset($steps[$key]) && $steps[$key]['shape'] !== 'service_operation') {
                    throw new NonDeterministicWorkflow('Service call collides with another durable command.', $sequence,
                        reason: 'durable_command_sequence_collision');
                }
                $steps[$key] ??= $this->step($sequence, 'service_operation');
                $delivery = $steps[$key]['value'] instanceof ServiceOperationHistory
                    ? $steps[$key]['value'] : new ServiceOperationHistory($this->codec, $sequence);
                $delivery->record($type, $payload, $resolutionOrder);
                $steps[$key]['value'] = $delivery;
            } elseif ($type === 'CancellationScopeOpened') {
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow('Scope opening collides with another durable command.', $sequence, reason: 'durable_command_sequence_collision');
                }
                $opening = $scopes->openings[$sequence] ?? throw new NonDeterministicWorkflow('Unvalidated scope opening.', $sequence);
                $steps[$key] = [
                    ...$this->resolvedStep($sequence, 'cancellation_scope', $opening['scope_id'], resolutionOrder: $resolutionOrder),
                    'scope_parent' => $opening['parent_scope_id'], 'scope_shield' => $opening['shield_parent'],
                ];
            } elseif (in_array($type, ['ActivityScheduled', 'ActivityStarted'], true)) {
                $steps[$key] ??= $this->step(
                    $sequence,
                    'activity',
                    $this->payloadDetail($payload, 'activity'),
                    parallelPath: $this->parallelPath($payload, $sequence),
                );
            } elseif ($type === 'ActivityCompleted') {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'activity',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'activity') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $failure = new ActivityFailed(
                    (string) ($payload['message'] ?? $payload['closed_reason'] ?? 'Activity failed.'),
                    isset($payload['activity_type']) ? (string) $payload['activity_type'] : null,
                    isset($payload['exception_type']) ? (string) $payload['exception_type'] : null,
                    (bool) ($payload['non_retryable'] ?? false),
                    $payload,
                    $type,
                );
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'activity',
                    null,
                    $failure,
                    $this->payloadDetail($payload, 'activity') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'TimerScheduled') {
                if (isset($payload['cancellation_cleanup'])) {
                    $timerCleanup[$key] = array_intersect_key($payload['cancellation_cleanup'],
                        array_flip(['scope_id', 'request_id', 'delivery_history_event_id']));
                }
                if (!in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
                    $steps[$key] ??= $this->step(
                        $sequence,
                        'timer',
                        $this->payloadDetail($payload, 'timer'),
                        parallelPath: $this->parallelPath($payload, $sequence),
                    );
                }
            } elseif ($type === 'TimerFired') {
                if (($payload['timer_kind'] ?? null) === 'condition_timeout') {
                    $conditionStepKey = $this->conditionStepKey($payload, $conditionStepsByWaitId);
                    if ($conditionStepKey !== null
                        && isset($steps[$conditionStepKey])
                        && $steps[$conditionStepKey]['resolved'] === false) {
                        $steps[$conditionStepKey] = $this->resolvedStep(
                            $steps[$conditionStepKey]['sequence'],
                            'condition_wait',
                            false,
                            detail: $steps[$conditionStepKey]['detail'],
                            conditionKey: $steps[$conditionStepKey]['condition_key'],
                            conditionDefinitionFingerprint: $steps[$conditionStepKey]['condition_definition_fingerprint'],
                            timeoutSeconds: $steps[$conditionStepKey]['timeout_seconds'],
                            parallelPath: $this->resolutionParallelPath(
                                $payload,
                                $steps[$conditionStepKey],
                                $steps[$conditionStepKey]['sequence'],
                            ),
                            resolutionOrder: $resolutionOrder,
                        );
                    }
                } elseif (($payload['timer_kind'] ?? null) !== 'signal_timeout') {
                    if (($steps[$key]['resolved'] ?? false) === true) {
                        continue;
                    }
                    $steps[$key] = $this->resolvedStep(
                        $sequence,
                        'timer',
                        null,
                        detail: $this->payloadDetail($payload, 'timer') ?? ($steps[$key]['detail'] ?? null),
                        parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                        resolutionOrder: $resolutionOrder,
                    );
                }
            } elseif ($type === 'TimerCancelled'
                && !in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'timer',
                    null,
                    new WorkflowCancelled((string) ($payload['message'] ?? 'Durable timer was cancelled.')),
                    $this->payloadDetail($payload, 'timer') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ChildWorkflowScheduled', 'ChildRunStarted'], true)) {
                $steps[$key] ??= $this->step(
                    $sequence,
                    'child_workflow',
                    $this->payloadDetail($payload, 'child_workflow'),
                    parallelPath: $this->parallelPath($payload, $sequence),
                );
                // Preserve a missing initial identity too. A later continued
                // or retried run cannot establish which run started the child.
                if (!array_key_exists($key, $childIdentities)) {
                    $childIdentities[$key] = $this->childIdentity($payload);
                }
            } elseif ($type === 'ChildRunCompleted') {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    null,
                    new ChildWorkflowFailed(
                        (string) ($payload['message'] ?? 'Child workflow failed.'),
                        isset($payload['child_workflow_type']) ? (string) $payload['child_workflow_type'] : null,
                        $type,
                        $payload,
                    ),
                    $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'SideEffectRecorded') {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'side_effect',
                    $this->decodeResult($payload),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'VersionMarkerRecorded') {
                if (isset($versionMarkerSequences[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains more than one VersionMarkerRecorded event.",
                        $sequence,
                        'one VersionMarkerRecorded event',
                        'multiple VersionMarkerRecorded events',
                        'duplicate_version_marker_record',
                    );
                }
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains a version marker and another durable command.",
                        $sequence,
                        'one durable command shape',
                        $steps[$key]['shape'].' and version marker',
                        'durable_command_sequence_collision',
                    );
                }
                [$changeId, $version] = $this->versionMarker($payload, $sequence);
                if (isset($versionMarkerChangeIds[$changeId])) {
                    throw new NonDeterministicWorkflow(
                        "Version marker {$changeId} is recorded more than once in workflow history.",
                        $sequence,
                        'one marker for the change ID',
                        "markers at sequences {$versionMarkerChangeIds[$changeId]} and {$sequence}",
                        'duplicate_version_marker',
                    );
                }
                $versionMarkerSequences[$key] = true;
                $versionMarkerChangeIds[$changeId] = $sequence;
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'version_marker',
                    $version,
                    detail: $changeId,
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'MemoUpserted') {
                if (isset($memoSequences[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains more than one MemoUpserted event.",
                        $sequence,
                        'one MemoUpserted event',
                        'multiple MemoUpserted events',
                        'duplicate_memo_upsert_record',
                    );
                }
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains a memo upsert and another durable command.",
                        $sequence,
                        'one durable command shape',
                        $steps[$key]['shape'].' and memo upsert',
                        'durable_command_sequence_collision',
                    );
                }
                if (!isset($payload['entries']) || !is_array($payload['entries'])) {
                    throw new NonDeterministicWorkflow(
                        "MemoUpserted history at sequence {$sequence} is missing replay identity entries.",
                        $sequence,
                        'memo entries object',
                        'missing entries',
                        'memo_entries_missing',
                    );
                }
                if (!isset($payload['merged']) || !is_array($payload['merged'])) {
                    throw new NonDeterministicWorkflow(
                        "MemoUpserted history at sequence {$sequence} is missing its merged projection.",
                        $sequence,
                        'merged memo projection',
                        'missing merged',
                        'memo_merged_projection_missing',
                    );
                }
                $entries = $this->decodeMemoHistoryMap(
                    $payload['entries'],
                    $sequence,
                    'entries',
                    true,
                );
                $this->decodeMemoHistoryMap($payload['merged'], $sequence, 'merged', false);
                $memoSequences[$key] = true;
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'memo',
                    null,
                    detail: $this->codec->encode($entries),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'SearchAttributesUpserted') {
                if (($steps[$key]['shape'] ?? null) === 'search_attributes') {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains more than one SearchAttributesUpserted event.",
                        $sequence,
                        'one SearchAttributesUpserted event',
                        'multiple SearchAttributesUpserted events',
                        'duplicate_search_attribute_update',
                    );
                }
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains a search-attribute upsert and another durable command.",
                        $sequence,
                        'one durable command shape',
                        $steps[$key]['shape'].' and search attribute upsert',
                        'durable_command_sequence_collision',
                    );
                }
                $attributes = $payload['attributes'] ?? null;
                if (!is_array($attributes) || $attributes === []) {
                    // The same reason sdk-rust reports for this history.
                    throw new NonDeterministicWorkflow(
                        "SearchAttributesUpserted history at sequence {$sequence} is missing its recorded attributes.",
                        $sequence,
                        'non-empty search attribute object',
                        'missing attributes',
                        'search_attribute_update_missing',
                    );
                }
                $steps[$key] = [
                    ...$this->resolvedStep(
                        $sequence,
                        'search_attributes',
                        null,
                        resolutionOrder: $resolutionOrder,
                    ),
                    'search_attributes' => $this->canonicalSearchAttributes($attributes, $sequence, 'recorded'),
                    'search_attribute_types' => array_key_exists('attribute_types', $payload)
                        ? $this->recordedSearchAttributeTypes($attributes, $payload['attribute_types'], $sequence)
                        : null,
                ];
            } elseif ($type === 'ConditionWaitOpened') {
                $conditionKey = $this->stringValue($payload['condition_key'] ?? null);
                $conditionDefinitionFingerprint = $this->stringValue(
                    $payload['condition_definition_fingerprint'] ?? null,
                );
                $timeoutSeconds = $this->intValue($payload['timeout_seconds'] ?? null);
                $steps[$key] ??= $this->step(
                    $sequence,
                    'condition_wait',
                    $this->conditionDetail($conditionKey, $conditionDefinitionFingerprint),
                    $conditionKey,
                    $conditionDefinitionFingerprint,
                    $timeoutSeconds,
                    $this->parallelPath($payload, $sequence),
                );
                $conditionWaitId = $this->stringValue($payload['condition_wait_id'] ?? null);
                if ($conditionWaitId !== null) {
                    $conditionStepsByWaitId[$conditionWaitId] = $key;
                    $conditionWaitIds[$key] = $conditionWaitId;
                }
                $conditionOpenedAt[$key] ??= $this->eventTime($event);
                $conditionOpenedOrder[$key] ??= $resolutionOrder;
                $conditionOccurrences[$key] ??= $this->stringValue($payload['condition_wait_occurrence_id'] ?? null);
                $conditionOpenSequences[$key] ??= $this->sequence($payload);
                $conditionDeadlinePolicies[$key] ??= $this->recordedConditionDeadlinePolicy($payload, $sequence);
            } elseif (in_array($type, ['ConditionWaitSatisfied', 'ConditionWaitTimedOut'], true)) {
                $conditionStepKey = $this->conditionStepKey($payload, $conditionStepsByWaitId);
                if ($conditionStepKey !== null
                    && isset($steps[$conditionStepKey])
                    && $steps[$conditionStepKey]['resolved'] === false) {
                    $steps[$conditionStepKey] = $this->resolvedStep(
                        $steps[$conditionStepKey]['sequence'],
                        'condition_wait',
                        $type === 'ConditionWaitSatisfied',
                        detail: $steps[$conditionStepKey]['detail'],
                        conditionKey: $steps[$conditionStepKey]['condition_key'],
                        conditionDefinitionFingerprint: $steps[$conditionStepKey]['condition_definition_fingerprint'],
                        timeoutSeconds: $steps[$conditionStepKey]['timeout_seconds'],
                        parallelPath: $this->resolutionParallelPath(
                            $payload,
                            $steps[$conditionStepKey],
                            $steps[$conditionStepKey]['sequence'],
                        ),
                        resolutionOrder: $resolutionOrder,
                    );
                    $conditionTerminals[$conditionStepKey] = [
                        'sequence' => $this->sequence($payload),
                        'condition_wait_id' => $this->stringValue($payload['condition_wait_id'] ?? null),
                    ];
                }
            }
        }
        foreach ($scopes->openings as $sequence => $_opening) {
            if (($steps[(string) $sequence]['shape'] ?? null) !== 'cancellation_scope') {
                throw new NonDeterministicWorkflow('Scope opening collides with another durable command.', $sequence, reason: 'durable_command_sequence_collision');
            }
        }
        foreach ($scopes->memberships as $sequence => $membership) {
            if (isset($steps[(string) $sequence])) {
                $steps[(string) $sequence]['cancellation_scope_id'] = $membership;
            }
        }
        foreach ($childPolicies as $key => $policies) {
            if (($steps[$key]['shape'] ?? null) === 'child_workflow') {
                $steps[$key]['child_policies'] = $policies;
            }
        }
        foreach ($activityPolicies as $key => $policy) {
            if (($steps[$key]['shape'] ?? null) === 'activity') {
                $steps[$key]['activity_cancellation_policy'] = $policy;
            }
        }
        foreach ($timerCleanup as $key => $proof) {
            if (isset($steps[$key])) { $steps[$key]['cancellation_cleanup'] = $proof; }
        }
        foreach ($conditionOpenedAt as $key => $openedAt) {
            if (($steps[$key]['shape'] ?? null) === 'condition_wait') {
                $steps[$key]['opened_at'] = $openedAt;
                $steps[$key]['opened_order'] = $conditionOpenedOrder[$key] ?? null;
                $steps[$key]['occurrence_id'] = $conditionOccurrences[$key] ?? null;
                $steps[$key]['condition_wait_id'] = $conditionWaitIds[$key] ?? null;
                $steps[$key]['condition_open_sequence'] = $conditionOpenSequences[$key] ?? null;
                $steps[$key]['condition_terminal'] = $conditionTerminals[$key] ?? null;
                $steps[$key]['deadline_policy'] = $conditionDeadlinePolicies[$key]['policy'] ?? null;
                $steps[$key]['timeout_budget'] = $conditionDeadlinePolicies[$key]['budget'] ?? null;
                // Snapshot before bindConditionReopens changes timeout/opening
                // metadata. This is internal replay state, never a wire field.
                $steps[$key]['physical_condition_steps'] = [$steps[$key]];
            }
        }
        foreach ($childIdentities as $key => $identity) {
            if (($steps[$key]['shape'] ?? null) === 'child_workflow' && $identity !== null) {
                $steps[$key]['child_identity'] = $identity;
            }
        }
        ksort($steps, SORT_NUMERIC);
        $steps = $this->bindConditionReopens(array_values($steps));

        return $preserveConditionReopens ? $steps : $this->collapseConditionReopens($steps);
    }

    /** @param list<array<string, mixed>> $history
     *  @param array<string, int> $resolutionOrders
     *  @return array<string, array<string, mixed>>
     */
    private function selectionResolutions(array $history, array &$resolutionOrders): array
    {
        $resolutions = [];
        foreach ($history as $order => $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SelectionResolved') {
                continue;
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $groupId = $payload['selection_group_id'] ?? null;
            $memberIndex = $payload['member_index'] ?? null;
            $memberBase = $payload['member_base_sequence'] ?? null;
            $identity = $payload['operation_identity'] ?? null;
            if (!is_string($groupId) || !preg_match('/\Aselect-calls:[1-9][0-9]*:[1-9][0-9]*\z/', $groupId)
                || !is_int($memberIndex) || $memberIndex < 0
                || !is_int($memberBase) || $memberBase < 1
                || !is_string($identity) || $identity === '') {
                throw new NonDeterministicWorkflow(
                    'SelectionResolved history contains invalid durable winner identity.',
                    $memberBase,
                    'stable selection group, member, and operation identity',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_resolution_metadata_invalid',
                );
            }
            if (isset($resolutions[$groupId])) {
                throw new NonDeterministicWorkflow(
                    "Selection group {$groupId} contains more than one committed winner.",
                    $memberBase,
                    'exactly one SelectionResolved marker',
                    'duplicate SelectionResolved markers',
                    'duplicate_selection_resolution',
                );
            }
            $resolutions[$groupId] = $payload;
            $resolutionOrders[$groupId] = $order;
        }

        return $resolutions;
    }

    /** @param list<array<string, mixed>> $history
     *  @param array<string, int> $cancellationOrders
     *  @return array<string, array<string, mixed>>
     */
    private function selectionCancellations(array $history, array &$cancellationOrders): array
    {
        $cancellations = [];
        foreach ($history as $order => $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SelectionOperationCancelled') {
                continue;
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $groupId = $payload['selection_group_id'] ?? null;
            $memberBase = $payload['member_base_sequence'] ?? null;
            if (!is_string($groupId)
                || !preg_match('/\Aselect-calls:[1-9][0-9]*:[1-9][0-9]*\z/', $groupId)
                || !is_int($memberBase) || $memberBase < 1) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation history contains invalid durable identity.',
                    is_int($memberBase) ? $memberBase : null,
                    'SelectionOperationCancelled with stable group and member base',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_invalid',
                );
            }
            $key = $groupId.':'.$memberBase;
            if (isset($cancellations[$key]) && $cancellations[$key] !== $payload) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation history contains conflicting markers.',
                    $memberBase,
                    'one stable SelectionOperationCancelled marker',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_conflict',
                );
            }
            $cancellations[$key] = $payload;
            $cancellationOrders[$key] ??= $order;
        }

        return $cancellations;
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return array<int, string>
     */
    private function selectionOperationIdentities(array $history): array
    {
        $identities = [];
        $priorities = [];

        foreach ($history as $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $sequence = $this->sequence($payload);
            $descriptor = match ($type) {
                'ActivityScheduled' => ['activity_execution_id', 10],
                'ChildWorkflowScheduled' => ['child_workflow_run_id', 10],
                'TimerScheduled' => ['timer_id', 1],
                'SignalWaitOpened' => ['signal_wait_id', 20],
                'ConditionWaitOpened' => ['condition_wait_id', 20],
                default => null,
            };
            if ($sequence === null || $descriptor === null) {
                continue;
            }

            [$field, $priority] = $descriptor;
            $identity = $this->stringValue($payload[$field] ?? null);
            if ($identity !== null && $identity !== '' && $priority > ($priorities[$sequence] ?? -1)) {
                $identities[$sequence] = $identity;
                $priorities[$sequence] = $priority;
            }
        }

        return $identities;
    }

    /**
     * @param array<string, mixed> $marker
     * @param array<int, array<string, mixed>> $stepsBySequence
     * @param list<array<string, mixed>> $history
     * @param array<int, string> $operationIdentities
     * @return array<string, mixed>
     */
    private function validatedSelectionResolution(
        ParallelWorkflowCommand $group,
        int $baseSequence,
        array $marker,
        array $stepsBySequence,
        array $history,
        array $operationIdentities,
    ): array {
        $groupSize = $group->leafCount();
        $expectedGroupId = "select-calls:{$baseSequence}:{$groupSize}";
        foreach ([
            'selection_group_id' => $expectedGroupId,
            'selection_group_base_sequence' => $baseSequence,
            'selection_group_size' => $groupSize,
        ] as $field => $expected) {
            if (($marker[$field] ?? null) !== $expected) {
                $this->throwSelectionMarkerMismatch(
                    $baseSequence,
                    $marker,
                    "field {$field} does not match the authored selection group",
                );
            }
        }

        $memberIndex = $marker['member_index'] ?? null;
        if (!is_int($memberIndex) || !array_key_exists($memberIndex, $group->operations)) {
            $this->throwSelectionMarkerMismatch(
                $baseSequence,
                $marker,
                'member_index does not name an authored selection member',
            );
        }

        foreach ($group->leafDescriptors($baseSequence) as $descriptor) {
            $sequence = $baseSequence + $descriptor['offset'];
            $step = $stepsBySequence[$sequence] ?? null;
            $identity = $operationIdentities[$sequence] ?? null;
            if (is_array($step) && is_string($identity) && $identity !== '') {
                continue;
            }

            $this->throwSelectionMarkerMismatch(
                $sequence,
                $marker,
                'cannot resolve before every authored member has a durable scheduling identity',
                'selection_resolution_incomplete_group',
            );
        }

        $cursor = 0;
        foreach ($group->operations as $index => $operation) {
            $memberSize = $operation instanceof ParallelWorkflowCommand ? $operation->leafCount() : 1;
            if ($index !== $memberIndex) {
                $cursor += $memberSize;
                continue;
            }

            $memberBase = $baseSequence + $cursor;
            $memberKind = $operation instanceof ParallelWorkflowCommand
                ? 'group'
                : match ($operation->command->historyShape) {
                    'activity' => 'activity',
                    'child_workflow' => 'child',
                    'timer' => 'timer',
                    'condition_wait' => 'condition',
                    default => $operation->command->historyShape,
                };
            $identity = $memberKind === 'group'
                ? "group:{$memberBase}:{$memberSize}"
                : ($operationIdentities[$memberBase] ?? null);
            foreach ([
                'member_key' => $group->keys[$index],
                'member_base_sequence' => $memberBase,
                'member_size' => $memberSize,
                'operation_kind' => $memberKind,
                'operation_identity' => $identity,
            ] as $field => $expected) {
                if ($expected === null || ($marker[$field] ?? null) !== $expected) {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        "field {$field} does not match the authored durable member",
                    );
                }
            }

            $outcome = $marker['outcome'] ?? null;
            if (!in_array($outcome, ['completed', 'failed'], true)) {
                $this->throwSelectionMarkerMismatch($memberBase, $marker, 'outcome must be completed or failed');
            }
            $resolutionId = $marker['resolution_event_id'] ?? null;
            $resolutionType = $marker['resolution_event_type'] ?? null;
            if (!is_string($resolutionId) || $resolutionId === '' || !is_string($resolutionType)) {
                $this->throwSelectionMarkerMismatch(
                    $memberBase,
                    $marker,
                    'resolution_event_id/type must identify the terminal durable event',
                );
            }

            $failureTypes = [
                'ActivityFailed',
                'ActivityCancelled',
                'ActivityTimedOut',
                'ChildRunFailed',
                'ChildRunCancelled',
                'ChildRunTerminated',
            ];
            $successTypes = [
                'ActivityCompleted',
                'ChildRunCompleted',
                'TimerFired',
                'SignalApplied',
                'ConditionWaitSatisfied',
                'ConditionWaitTimedOut',
            ];
            $terminalTypes = $outcome === 'failed' ? $failureTypes : $successTypes;
            $candidates = [];
            foreach ($history as $order => $event) {
                $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
                $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
                $sequence = $this->sequence($payload);
                if (!in_array($type, $terminalTypes, true)
                    || $sequence === null
                    || $sequence < $memberBase
                    || $sequence >= $memberBase + $memberSize) {
                    continue;
                }
                $eventId = $event['id'] ?? $event['event_id'] ?? null;
                if (!is_string($eventId) || $eventId === '') {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        'terminal selection history is missing its durable event id',
                    );
                }
                $candidates[] = [
                    'id' => $eventId,
                    'type' => $type,
                    'sequence' => $sequence,
                    'order' => $order,
                ];
            }
            $resolution = $outcome === 'failed' ? ($candidates[0] ?? null) : ($candidates[array_key_last($candidates)] ?? null);
            if (!is_array($resolution)
                || $resolution['id'] !== $resolutionId
                || $resolution['type'] !== $resolutionType) {
                $this->throwSelectionMarkerMismatch(
                    $memberBase,
                    $marker,
                    'resolution event is not the event that made the authored member terminal',
                );
            }

            if ($outcome === 'completed') {
                for ($sequence = $memberBase; $sequence < $memberBase + $memberSize; ++$sequence) {
                    $step = $stepsBySequence[$sequence] ?? null;
                    if (!is_array($step) || $step['resolved'] !== true || $step['failure'] instanceof Throwable) {
                        $this->throwSelectionMarkerMismatch(
                            $memberBase,
                            $marker,
                            'completed nested winner does not have a fully completed durable barrier',
                        );
                    }
                }
            } else {
                $step = $stepsBySequence[$resolution['sequence']] ?? null;
                if (!is_array($step) || !$step['failure'] instanceof Throwable) {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        'failed winner does not reference its exact durable failure',
                    );
                }
            }

            return [...$marker, '_resolution_sequence' => $resolution['sequence']];
        }

        $this->throwSelectionMarkerMismatch($baseSequence, $marker, 'winner is outside the authored selection group');
    }

    /** @param array<string, mixed> $marker */
    private function throwSelectionMarkerMismatch(
        int $sequence,
        array $marker,
        string $detail,
        string $reason = 'selection_resolution_member_mismatch',
    ): never {
        throw new NonDeterministicWorkflow(
            "SelectionResolved {$detail}.",
            $sequence,
            'SelectionResolved matching the authored member and terminal event',
            json_encode($marker, JSON_THROW_ON_ERROR),
            $reason,
        );
    }

    private function assertDurableHandleExecutionIdentity(
        DurableOperationHandle $handle,
        ?WorkflowContext $context,
    ): void {
        if (!$context instanceof WorkflowContext) {
            throw new LogicException('A durable operation handle resumed before workflow context initialization.');
        }
        if ($handle->workflowId === $context->workflowId && $handle->runId === $context->runId) {
            return;
        }

        throw new NonDeterministicWorkflow(
            'A durable operation handle cannot be used by a different workflow/run execution.',
            $handle->baseSequence,
            json_encode([
                'workflow_id' => $handle->workflowId,
                'run_id' => $handle->runId,
            ], JSON_THROW_ON_ERROR),
            json_encode([
                'workflow_id' => $context->workflowId,
                'run_id' => $context->runId,
            ], JSON_THROW_ON_ERROR),
            'durable_operation_handle_execution_mismatch',
        );
    }

    /**
     * @param array<int, array<string, mixed>> $stepsBySequence
     * @param array<string, array<string, mixed>> $selectionCancellations
     * @param array<string, int> $selectionCancellationOrders
     * @return array{resolved: bool, value: mixed, failure: ?Throwable, resolution_orders: list<int>}
     */
    private function durableHandleResolution(
        DurableOperationHandle $handle,
        array $stepsBySequence,
        array $selectionCancellations,
        array $selectionCancellationOrders,
    ): array {
        if ($this->selectionCancellationForHandle($handle, $selectionCancellations) !== null) {
            return [
                'resolved' => true,
                'value' => null,
                'resolution_orders' => [$selectionCancellationOrders[$handle->selectionGroupId.':'.$handle->baseSequence]],
                'failure' => new DurableOperationCancelled(
                    $handle->selectionGroupId,
                    $handle->key,
                    $handle->index,
                    $handle->kind,
                    $handle->identity,
                ),
            ];
        }

        if ($handle->operation instanceof DeferredWorkflowOperation) {
            $step = $stepsBySequence[$handle->baseSequence] ?? null;

            return is_array($step) && $step['resolved'] === true
                ? ['resolved' => true, 'value' => $step['value'], 'failure' => $step['failure'],
                    'resolution_orders' => $this->blockingResolutionOrders($step)]
                : ['resolved' => false, 'value' => null, 'failure' => null, 'resolution_orders' => []];
        }

        $results = [];
        $pending = false;
        $failures = [];
        $resolvedOrders = [];
        foreach ($handle->operation->leafDescriptors($handle->baseSequence) as $descriptor) {
            $step = $stepsBySequence[$handle->baseSequence + $descriptor['offset']] ?? null;
            if (!is_array($step) || $step['resolved'] !== true) {
                $pending = true;
                continue;
            }
            if ($step['failure'] instanceof Throwable) {
                $failures[] = $step;
                continue;
            }
            foreach ($this->blockingResolutionOrders($step) as $order) {
                $resolvedOrders[] = $order;
            }
            $results[$descriptor['offset']] = $step['value'];
        }
        if ($failures !== []) {
            usort($failures, static fn (array $left, array $right): int =>
                $left['resolution_order'] <=> $right['resolution_order']);

            return ['resolved' => true, 'value' => null, 'failure' => $failures[0]['failure'],
                'resolution_orders' => [...array_values(array_filter($resolvedOrders,
                    static fn (int $order): bool => $order <= $failures[0]['resolution_order'])),
                    ...$this->blockingResolutionOrders($failures[0])]];
        }
        if ($pending) {
            return ['resolved' => false, 'value' => null, 'failure' => null, 'resolution_orders' => []];
        }
        ksort($results);

        return [
            'resolved' => true,
            'value' => $handle->operation->nestedResults(array_values($results)),
            'failure' => null,
            'resolution_orders' => $resolvedOrders,
        ];
    }

    /**
     * @param array{shape: string, resolution_order: int} $step
     * @return list<int>
     */
    private function blockingResolutionOrders(array $step): array
    {
        if (in_array($step['shape'], ['side_effect', 'version_marker', 'memo', 'search_attributes'], true)) {
            return [];
        }

        return [$step['resolution_order']];
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param list<int> $orders
     */
    private function advanceCancellationClock(WorkflowContext $context, array $history, array $orders): void
    {
        if (!$context->hasDeliveredCancellation()) {
            return;
        }
        sort($orders, SORT_NUMERIC);
        foreach ($orders as $order) {
            $context->observeCancellationReplayTime($history[$order] ?? null);
        }
    }

    /**
     * @param Fiber<mixed, mixed, mixed, mixed> $execution
     * @param array<string, mixed>|null $step
     */
    private function resumeExecution(Fiber $execution, BusinessReplayClock $clock, mixed $value, ?array $step = null): mixed
    {
        $clock->consume($step);

        return $clock->invoke(static fn (): mixed => $execution->resume($value));
    }

    /**
     * @param Fiber<mixed, mixed, mixed, mixed> $execution
     * @param array<string, mixed>|null $step
     */
    private function throwIntoExecution(Fiber $execution, BusinessReplayClock $clock, Throwable $failure, ?array $step = null): mixed
    {
        $clock->consume($step);

        return $clock->invoke(static fn (): mixed => $execution->throw($failure));
    }

    /** @param Fiber<mixed, mixed, mixed, mixed> $execution
     * @param list<array{step: array<string, mixed>, path: list<array<string, mixed>>, offset: int}> $members
     */
    private function resumeGroupExecution(Fiber $execution, BusinessReplayClock $clock, mixed $value, array $members, int $size): mixed
    {
        $clock->consumeGroup($members, $size, $this->groupClockPathMatches(...));

        return $clock->invoke(static fn (): mixed => $execution->resume($value));
    }

    /** @param Fiber<mixed, mixed, mixed, mixed> $execution
     * @param list<array{step: array<string, mixed>, path: list<array<string, mixed>>, offset: int}> $members
     */
    private function throwGroupIntoExecution(Fiber $execution, BusinessReplayClock $clock, Throwable $failure, array $members, int $size, int $order): mixed
    {
        $clock->consumeGroup($members, $size, $this->groupClockPathMatches(...), $order);

        return $clock->invoke(static fn (): mixed => $execution->throw($failure));
    }

    /** @param array<string, mixed> $body
     * @param list<array<string, mixed>> $expected
     */
    private function groupClockPathMatches(array $body, array $expected): bool
    {
        $sequence = $body['sequence'] ?? null;

        return is_int($sequence) && $this->parallelPath($body, $sequence) === $expected;
    }

    /**
     * @param array<string, array<string, mixed>> $selectionCancellations
     * @return array<string, mixed>|null
     */
    private function selectionCancellationForHandle(
        DurableOperationHandle $handle,
        array $selectionCancellations,
    ): ?array {
        $payload = $selectionCancellations[$handle->selectionGroupId.':'.$handle->baseSequence] ?? null;
        if (!is_array($payload)) {
            return null;
        }

        foreach ([
            'selection_group_id' => $handle->selectionGroupId,
            'member_key' => $handle->key,
            'member_index' => $handle->index,
            'member_base_sequence' => $handle->baseSequence,
            'member_size' => $handle->size,
            'operation_kind' => $handle->kind,
            'operation_identity' => $handle->identity,
        ] as $field => $expected) {
            if (($payload[$field] ?? null) === $expected) {
                continue;
            }

            throw new NonDeterministicWorkflow(
                "Selection cancellation field {$field} does not match the authored durable member.",
                $handle->baseSequence,
                'SelectionOperationCancelled matching the authored selection handle',
                json_encode($payload, JSON_THROW_ON_ERROR),
                'selection_cancellation_member_mismatch',
            );
        }

        return $payload;
    }

    /**
     * @param array<int|string, DurableOperationHandle> $handles
     * @param array<string, array<string, mixed>> $selectionCancellations
     */
    private function validateSelectionCancellationsForHandles(
        array $handles,
        array $selectionCancellations,
    ): void {
        $first = reset($handles);
        if (!$first instanceof DurableOperationHandle) {
            return;
        }
        $byBase = [];
        foreach ($handles as $handle) {
            $byBase[$handle->baseSequence] = $handle;
        }
        foreach ($selectionCancellations as $payload) {
            if (($payload['selection_group_id'] ?? null) !== $first->selectionGroupId) {
                continue;
            }
            $memberBase = $payload['member_base_sequence'] ?? null;
            $handle = is_int($memberBase) ? ($byBase[$memberBase] ?? null) : null;
            if (!$handle instanceof DurableOperationHandle) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation member base does not name an authored durable member.',
                    is_int($memberBase) ? $memberBase : null,
                    'SelectionOperationCancelled matching an authored selection handle',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_member_mismatch',
                );
            }
            $this->selectionCancellationForHandle($handle, $selectionCancellations);
        }
    }

    /**
     * Decode the inline Avro map envelope returned by Server history.
     *
     * External memo command references are resolved by the runtime before the
     * MemoUpserted event is recorded, so replay always consumes inline bytes.
     *
     * @return array<string, mixed>
     */
    private function decodeMemoHistoryMap(
        mixed $envelope,
        int $sequence,
        string $field,
        bool $requireEntries,
    ): array {
        try {
            if (!is_array($envelope)) {
                throw new LogicException('expected an Avro payload envelope object');
            }

            $keys = array_keys($envelope);
            sort($keys);
            if ($keys !== ['blob', 'codec']) {
                throw new LogicException('expected exactly the public {codec, blob} payload envelope');
            }

            $decoded = $this->codec->decodeEnvelope($envelope);
            if ($decoded instanceof AvroMapValue) {
                $entries = [];
                foreach ($decoded->pairs as [$key, $value]) {
                    $entries[$key] = $value;
                }
                $decoded = $entries;
            }
            if (!is_array($decoded)) {
                throw new LogicException('the Avro payload must decode to a string-keyed map');
            }
            if ($decoded === []) {
                if (!$requireEntries) {
                    return [];
                }

                throw new LogicException('the memo entries map must not be empty');
            }
            if (array_is_list($decoded)) {
                throw new LogicException('the Avro payload must decode to a string-keyed map');
            }

            return WorkflowCommand::canonicalMemoEntries($decoded);
        } catch (NonDeterministicWorkflow $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new NonDeterministicWorkflow(
                "MemoUpserted history at sequence {$sequence} has an invalid {$field} envelope: {$exception->getMessage()}",
                $sequence,
                "Avro memo {$field} payload envelope",
                get_debug_type($envelope),
                $field === 'entries' ? 'memo_entries_invalid' : 'memo_merged_projection_invalid',
            );
        }
    }

    /** @param list<array<string, mixed>> $history */
    private function hasCompletedHistory(array $history): bool
    {
        foreach ($history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) === 'WorkflowCompleted') {
                return true;
            }
        }

        return false;
    }

    /**
     * @param list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }> $steps
     */
    private function assertNoRemainingSteps(array $steps, int $stepCursor, string $terminalCommand): void
    {
        if ($stepCursor >= count($steps)) {
            return;
        }

        $step = $steps[$stepCursor];
        throw new NonDeterministicWorkflow(
            "Workflow reached {$terminalCommand} before consuming its recorded durable history.",
            $step['sequence'],
            $step['shape'],
            $terminalCommand,
        );
    }

    /**
     * @param list<array<string, mixed>> $parallelPath
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }
     */
    private function step(
        int $sequence,
        string $shape,
        ?string $detail = null,
        ?string $conditionKey = null,
        ?string $conditionDefinitionFingerprint = null,
        ?int $timeoutSeconds = null,
        array $parallelPath = [],
        int $resolutionOrder = PHP_INT_MAX,
    ): array {
        return [
            'sequence' => $sequence,
            'shape' => $shape,
            'detail' => $detail,
            'resolved' => false,
            'value' => null,
            'failure' => null,
            'condition_key' => $conditionKey,
            'condition_definition_fingerprint' => $conditionDefinitionFingerprint,
            'timeout_seconds' => $timeoutSeconds,
            'parallel_path' => $parallelPath,
            'resolution_order' => $resolutionOrder,
        ];
    }

    /**
     * @param list<array<string, mixed>> $parallelPath
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }
     */
    private function resolvedStep(
        int $sequence,
        string $shape,
        mixed $value,
        ?Throwable $failure = null,
        ?string $detail = null,
        ?string $conditionKey = null,
        ?string $conditionDefinitionFingerprint = null,
        ?int $timeoutSeconds = null,
        array $parallelPath = [],
        int $resolutionOrder = PHP_INT_MAX,
    ): array
    {
        return [
            'sequence' => $sequence,
            'shape' => $shape,
            'detail' => $detail,
            'resolved' => true,
            'value' => $value,
            'failure' => $failure,
            'condition_key' => $conditionKey,
            'condition_definition_fingerprint' => $conditionDefinitionFingerprint,
            'timeout_seconds' => $timeoutSeconds,
            'parallel_path' => $parallelPath,
            'resolution_order' => $resolutionOrder,
        ];
    }

    /** @param array<string, mixed> $step */
    private function assertCommandMatchesStep(WorkflowCommand $command, array $step): void
    {
        if ($step['value'] instanceof ServiceOperationHistory) {
            $step['value']->assertIntent($command);
        }
        if (($command->attributes['cancellation_cleanup'] ?? null) != ($step['cancellation_cleanup'] ?? null)) {
            throw new NonDeterministicWorkflow('Cleanup timer changed its original delivery or shielding.', $step['sequence'],
                reason: 'cancellation_scope_cleanup_authority_mismatch');
        }
        if (($command->attributes['cancellation_scope_id'] ?? 'root') !== ($step['cancellation_scope_id'] ?? 'root')) {
            throw new NonDeterministicWorkflow('Authored operation cancellation scope changed.', $step['sequence'],
                reason: 'cancellation_scope_membership_changed');
        }
        if ($step['shape'] !== $command->historyShape) {
            throw new NonDeterministicWorkflow(
                "History contains {$step['shape']} but workflow scheduled {$command->historyShape}.",
                $step['sequence'],
                $step['shape'],
                $command->historyShape,
            );
        }

        if ($command->historyShape === 'search_attributes') {
            $authored = $this->canonicalSearchAttributes($command->attributes['attributes'] ?? null, $step['sequence'], 'authored');
            $difference = self::searchAttributesDifference($step['search_attributes'] ?? [], $authored);
            if ($difference !== null) {
                // Diagnostics name the key, never the values.
                throw new NonDeterministicWorkflow(
                    "Recorded search attributes changed during replay: {$difference}.",
                    $step['sequence'],
                    'the recorded search attributes',
                    $difference,
                    'search_attribute_value_mismatch',
                );
            }
            $recordedTypes = $step['search_attribute_types'] ?? null;
            if ($recordedTypes !== null && array_key_exists('attribute_types', $command->attributes)
                && $recordedTypes !== $command->attributes['attribute_types']) {
                throw new NonDeterministicWorkflow(
                    'Declared search-attribute types changed during replay.',
                    $step['sequence'], reason: 'search_attribute_type_mismatch',
                );
            }
        }

        $actualDetail = $this->commandDetail($command);
        if ($command->type !== 'open_condition_wait'
            && $step['detail'] !== null
            && $actualDetail !== null
            && $step['detail'] !== $actualDetail) {
            throw new NonDeterministicWorkflow(
                "Recorded {$step['shape']} detail changed from {$step['detail']} to {$actualDetail}.",
                $step['sequence'],
                $step['detail'],
                $actualDetail,
            );
        }
        if ($command->historyShape === 'activity') {
            $actualPolicy = $command->attributes['cancellation_policy'] ?? CancellationPolicy::TryCancel->value;
            $recordedPolicy = $step['activity_cancellation_policy'] ?? CancellationPolicy::TryCancel->value;
            if ($actualPolicy !== $recordedPolicy) {
                throw new NonDeterministicWorkflow(
                    'Activity cancellation_policy changed during replay.',
                    $step['sequence'],
                    $recordedPolicy,
                    $actualPolicy,
                    'activity_cancellation_policy_changed',
                );
            }
        }
        if ($command->historyShape === 'child_workflow') {
            $actualPolicies = array_merge(
                ['parent_close_policy' => ParentClosePolicy::Abandon->value, 'cancellation_policy' => CancellationPolicy::Abandon->value],
                WorkflowCommand::canonicalChildWorkflowPolicies($command->attributes),
            );
            $recordedPolicies = $step['child_policies'] ?? [
                'parent_close_policy' => ParentClosePolicy::Abandon->value,
                'cancellation_policy' => CancellationPolicy::Abandon->value,
            ];
            foreach ($recordedPolicies as $field => $value) {
                if ($actualPolicies[$field] !== $value) {
                    throw new NonDeterministicWorkflow(
                        "Child workflow {$field} changed during replay.",
                        $step['sequence'],
                        $value,
                        $actualPolicies[$field],
                        'child_workflow_policy_changed',
                    );
                }
            }
        }
    }

    /**
     * @param array<string, mixed> $step
     * @param list<array<string, mixed>> $expectedPath
     */
    private function assertParallelPathMatches(array $step, array $expectedPath): void
    {
        if ($step['parallel_path'] === []) {
            throw new NonDeterministicWorkflow(
                'Recorded parallel member is missing its durable group path.',
                $step['sequence'],
                json_encode($expectedPath, JSON_THROW_ON_ERROR),
                '<missing>',
                'parallel_group_metadata_missing',
            );
        }
        if ($step['parallel_path'] !== $expectedPath) {
            throw new NonDeterministicWorkflow(
                'Recorded parallel-group identity or path changed during replay.',
                $step['sequence'],
                json_encode($step['parallel_path'], JSON_THROW_ON_ERROR),
                json_encode($expectedPath, JSON_THROW_ON_ERROR),
                'parallel_group_shape_mismatch',
            );
        }
    }

    /**
     * @param array<string, mixed> $payload
     * @return list<array<string, mixed>>
     */
    private function parallelPath(array $payload, int $sequence): array
    {
        $fields = [
            'parallel_group_id',
            'parallel_group_kind',
            'parallel_group_base_sequence',
            'parallel_group_size',
            'parallel_group_index',
            'parallel_group_mode',
            'selection_member_key',
            'selection_member_index',
            'selection_member_base_sequence',
            'selection_member_size',
            'selection_member_kind',
        ];
        $hasMetadata = array_key_exists('parallel_group_path', $payload);
        foreach ($fields as $field) {
            $hasMetadata = $hasMetadata || array_key_exists($field, $payload);
        }
        if (!$hasMetadata) {
            return [];
        }

        $topLevel = $this->parallelEntry($payload, $sequence);
        $rawPath = $payload['parallel_group_path'] ?? null;
        if ($rawPath === null) {
            return [$topLevel];
        }
        if (!is_array($rawPath) || !array_is_list($rawPath) || $rawPath === []) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains an invalid group path.',
                $sequence,
                'non-empty parallel_group_path list',
                get_debug_type($rawPath),
                'parallel_group_metadata_invalid',
            );
        }

        $path = [];
        foreach ($rawPath as $entry) {
            if (!is_array($entry)) {
                throw new NonDeterministicWorkflow(
                    'Parallel-group history contains a non-object path entry.',
                    $sequence,
                    'parallel group metadata object',
                    get_debug_type($entry),
                    'parallel_group_metadata_invalid',
                );
            }
            $path[] = $this->parallelEntry($entry, $sequence);
        }

        if ($path[array_key_last($path)] !== $topLevel) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history top-level fields do not match the innermost path entry.',
                $sequence,
                json_encode($path[array_key_last($path)], JSON_THROW_ON_ERROR),
                json_encode($topLevel, JSON_THROW_ON_ERROR),
                'parallel_group_metadata_invalid',
            );
        }

        return $path;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private function parallelEntry(array $payload, int $sequence): array
    {
        $id = $payload['parallel_group_id'] ?? null;
        $kind = $payload['parallel_group_kind'] ?? null;
        $base = $payload['parallel_group_base_sequence'] ?? null;
        $size = $payload['parallel_group_size'] ?? null;
        $index = $payload['parallel_group_index'] ?? null;
        $mode = $payload['parallel_group_mode'] ?? (is_string($id) && str_starts_with($id, 'select-calls:')
            ? 'select'
            : 'all');
        if (!is_string($id) || $id === ''
            || !is_string($kind) || !in_array($kind, ['activity', 'child', 'timer', 'condition', 'mixed'], true)
            || !is_string($mode) || !in_array($mode, ['all', 'select'], true)
            || !is_int($base) || $base < 1
            || !is_int($size) || $size < 1 || $size > WorkflowContext::MAX_PARALLEL_OPERATIONS
            || !is_int($index) || $index < 0 || $index >= $size
            || $sequence !== $base + $index) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains invalid identity, bounds, or path fields.',
                $sequence,
                'valid group, base-sequence, size, index, and path identity',
                json_encode($payload, JSON_THROW_ON_ERROR),
                'parallel_group_metadata_invalid',
            );
        }

        $prefix = $mode === 'select' ? 'select-calls' : match ($kind) {
            'activity' => 'parallel-activities',
            'child' => 'parallel-children',
            'timer' => 'parallel-timers',
            default => 'parallel-calls',
        };
        $expectedId = sprintf('%s:%d:%d', $prefix, $base, $size);
        if ($id !== $expectedId) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains an incompatible stable group ID.',
                $sequence,
                $expectedId,
                $id,
                'parallel_group_metadata_invalid',
            );
        }

        $entry = array_filter([
            'parallel_group_id' => $id,
            'parallel_group_kind' => $kind,
            'parallel_group_mode' => $mode === 'select' ? 'select' : null,
            'parallel_group_base_sequence' => $base,
            'parallel_group_size' => $size,
            'parallel_group_index' => $index,
        ], static fn (mixed $value): bool => $value !== null);

        if ($mode === 'select') {
            $memberKey = $payload['selection_member_key'] ?? null;
            $memberIndex = $payload['selection_member_index'] ?? null;
            $memberBase = $payload['selection_member_base_sequence'] ?? null;
            $memberSize = $payload['selection_member_size'] ?? null;
            $memberKind = $payload['selection_member_kind'] ?? null;
            if (!((is_string($memberKey) && $memberKey !== '') || (is_int($memberKey) && $memberKey >= 0))
                || !is_int($memberIndex) || $memberIndex < 0
                || !is_int($memberBase) || $memberBase < $base
                || !is_int($memberSize) || $memberSize < 1
                || !is_string($memberKind)
                || !in_array($memberKind, ['activity', 'child', 'timer', 'signal', 'condition', 'group'], true)
                || $sequence < $memberBase || $sequence >= $memberBase + $memberSize) {
                throw new NonDeterministicWorkflow(
                    'Selection-group history contains invalid durable member identity.',
                    $sequence,
                    'stable member key, index, base sequence, and size',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_group_metadata_invalid',
                );
            }
            $entry = [
                ...$entry,
                'selection_member_key' => $memberKey,
                'selection_member_index' => $memberIndex,
                'selection_member_base_sequence' => $memberBase,
                'selection_member_size' => $memberSize,
                'selection_member_kind' => $memberKind,
            ];
        }

        return $entry;
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, mixed>|null $previous
     * @return list<array<string, mixed>>
     */
    private function resolutionParallelPath(array $payload, ?array $previous, int $sequence): array
    {
        $current = $this->parallelPath($payload, $sequence);
        $recorded = is_array($previous['parallel_path'] ?? null) ? $previous['parallel_path'] : [];
        if ($current !== [] && $recorded !== [] && $current !== $recorded) {
            throw new NonDeterministicWorkflow(
                'Parallel-group metadata changed between scheduling and resolution history.',
                $sequence,
                json_encode($recorded, JSON_THROW_ON_ERROR),
                json_encode($current, JSON_THROW_ON_ERROR),
                'parallel_group_history_conflict',
            );
        }

        return $current !== [] ? $current : $recorded;
    }

    private function commandDetail(WorkflowCommand $command): ?string
    {
        if ($command->historyShape === 'activity') {
            $activityType = $command->attributes['activity_type'] ?? null;

            return is_string($activityType) && $activityType !== ''
                ? ($command->type === 'record_local_activity' ? 'local:' : '').$activityType
                : null;
        }

        $value = match ($command->historyShape) {
            'timer' => $command->attributes['delay_seconds'] ?? null,
            'child_workflow' => $command->attributes['workflow_type'] ?? null,
            'version_marker' => $command->attributes['change_id'] ?? null,
            'memo' => $this->codec->encode(
                WorkflowCommand::canonicalMemoEntries($command->attributes['entries'] ?? []),
            ),
            'condition_wait' => $this->conditionDetail(
                $this->stringValue($command->attributes['condition_key'] ?? null),
                $this->stringValue($command->attributes['condition_definition_fingerprint'] ?? null),
            ),
            default => null,
        };

        return $value === null ? null : (string) $value;
    }

    private const SEARCH_ATTRIBUTE_MAX_STRING_LENGTH = 2048;

    private const SEARCH_ATTRIBUTE_MAX_KEYWORD_LENGTH = 255;

    /**
     * @param array<string, mixed> $attributes
     * @return array<string, string>
     */
    private function recordedSearchAttributeTypes(array $attributes, mixed $types, int $sequence): array
    {
        try {
            return SearchAttributeTypes::canonical($attributes, $types);
        } catch (\InvalidArgumentException $error) {
            throw new NonDeterministicWorkflow(
                $error->getMessage(), $sequence, reason: 'search_attribute_types_malformed',
            );
        }
    }

    /**
     * The canonical replay identity of a search-attribute upsert's values.
     *
     * History keeps the values as Core's UpsertSearchAttributesCall normalizes
     * them: a non-empty map with 1-64 character URL-safe string keys; each
     * value null, a bool, an int, a finite float, a string of at most 2048
     * characters or a list of strings of at most 255 characters each; strings
     * and list items trimmed, an empty scalar string as a deletion, keys
     * sorted. The authored call is normalized the same way. Anything outside
     * that set, recorded or authored, refuses replay instead of being coerced.
     *
     * Explicitly authored types bind separately against known recorded types.
     * Legacy one-argument calls remain value-only because effective types may
     * come from a namespace registration the worker cannot see. Histories
     * without type metadata also retain value-only compatibility.
     *
     * @return array<string, null|bool|int|float|string|list<string>>
     */
    private function canonicalSearchAttributes(mixed $attributes, int $sequence, string $origin): array
    {
        $refuse = static function (string $detail) use ($sequence, $origin): never {
            throw new NonDeterministicWorkflow(
                "The {$origin} search attributes at sequence {$sequence} are not portable: {$detail}.",
                $sequence,
                'a non-empty map of portable keys to null, bool, int, finite float, string or string-list values',
                $detail,
                'search_attributes_unportable',
            );
        };
        if (!is_array($attributes) || $attributes === []) {
            $refuse('expected a non-empty map');
        }
        $normalized = [];
        foreach ($attributes as $key => $value) {
            if (!is_string($key) || preg_match('/\A[A-Za-z0-9_.:-]{1,64}\z/', $key) !== 1) {
                $refuse(is_string($key) ? 'a key is not a 1-64 character URL-safe string' : 'a key is not a string');
            }
            if (is_string($value)) {
                $value = trim($value);
                $length = self::characterLength($value);
                if ($length === null || $length > self::SEARCH_ATTRIBUTE_MAX_STRING_LENGTH) {
                    $refuse("[{$key}] is not a UTF-8 string of at most ".self::SEARCH_ATTRIBUTE_MAX_STRING_LENGTH.' characters');
                }
                $value = $value === '' ? null : $value;
            } elseif (is_float($value)) {
                if (!is_finite($value)) {
                    $refuse("[{$key}] is not a finite number");
                }
            } elseif (is_array($value)) {
                if (!array_is_list($value)) {
                    $refuse("[{$key}] is a map, not a list");
                }
                foreach ($value as $index => $item) {
                    if (!is_string($item)) {
                        $refuse("[{$key}] list value {$index} is not a string");
                    }
                    $item = trim($item);
                    $length = self::characterLength($item);
                    if ($length === null || $length > self::SEARCH_ATTRIBUTE_MAX_KEYWORD_LENGTH) {
                        $refuse("[{$key}] list value {$index} is not a UTF-8 string of at most ".self::SEARCH_ATTRIBUTE_MAX_KEYWORD_LENGTH.' characters');
                    }
                    $value[$index] = $item;
                }
            } elseif ($value !== null && !is_bool($value) && !is_int($value)) {
                $refuse("[{$key}] is a ".get_debug_type($value));
            }
            $normalized[$key] = $value;
        }
        ksort($normalized, SORT_STRING);

        return $normalized;
    }

    private static function characterLength(string $value): ?int
    {
        $length = preg_match_all('/./su', $value);

        return $length === false ? null : $length;
    }

    /**
     * Compare two canonical maps structurally and type-strictly, without any
     * JSON or php.ini dependent formatting. An int and a float are different
     * values; floats compare numerically, so -0.0 equals 0.0.
     *
     * @param array<string, mixed> $recorded
     * @param array<string, mixed> $authored
     * @return string|null a payload-free description of the first difference
     */
    private static function searchAttributesDifference(array $recorded, array $authored): ?string
    {
        if (array_keys($recorded) !== array_keys($authored)) {
            return 'the key set changed';
        }
        foreach ($recorded as $key => $value) {
            $other = $authored[$key];
            $same = match (true) {
                is_float($value) => is_float($other) && $value == $other,
                is_array($value) => is_array($other) && $value === $other,
                default => $value === $other,
            };
            if (!$same) {
                return "[{$key}] changed";
            }
        }

        return null;
    }

    /** @param array<string, mixed> $payload */
    private function payloadDetail(array $payload, string $shape): ?string
    {
        if ($shape === 'activity') {
            $activityType = $payload['activity_type'] ?? $payload['activity_name'] ?? null;

            return is_string($activityType) && $activityType !== ''
                ? (($payload['execution_mode'] ?? null) === 'local' ? 'local:' : '').$activityType
                : null;
        }

        $value = match ($shape) {
            'timer' => $payload['delay_seconds'] ?? null,
            'child_workflow' => $payload['child_workflow_type'] ?? $payload['workflow_type'] ?? null,
            'version_marker' => $payload['change_id'] ?? null,
            'condition_wait' => $this->conditionDetail(
                $this->stringValue($payload['condition_key'] ?? null),
                $this->stringValue($payload['condition_definition_fingerprint'] ?? null),
            ),
            default => null,
        };

        return $value === null ? null : (string) $value;
    }

    /** @param array<string, mixed> $payload */
    private function decodeResult(array $payload): mixed
    {
        $raw = $payload['result'] ?? $payload['output'] ?? null;
        if (is_array($raw) || is_string($raw)) {
            return $this->codec->decodeEnvelope($raw);
        }

        return $raw;
    }

    /** @param array<string, mixed> $payload */
    private function sequence(array $payload): ?int
    {
        $value = $payload['sequence'] ?? $payload['workflow_sequence'] ?? null;

        return is_numeric($value) ? (int) $value : null;
    }

    /** @param array<string, mixed> $payload */
    private function memoSequence(array $payload): int
    {
        $sequence = $payload['sequence'] ?? $payload['workflow_sequence'] ?? null;
        if ($sequence === null) {
            throw new NonDeterministicWorkflow(
                'MemoUpserted history is missing its replay sequence.',
                null,
                'positive integer sequence',
                'missing sequence',
                'memo_sequence_missing',
            );
        }
        if (!is_int($sequence) || $sequence <= 0) {
            throw new NonDeterministicWorkflow(
                'MemoUpserted history replay sequence must be a positive integer.',
                is_int($sequence) ? $sequence : null,
                'positive integer sequence',
                is_scalar($sequence) ? (string) $sequence : get_debug_type($sequence),
                'memo_sequence_invalid',
            );
        }

        return $sequence;
    }

    /**
     * The sequences a cancellation delivery reserves: its interrupted call's
     * sequence through sequence + sequence_span - 1, inclusive, as Core counts
     * them. A span that is present must be a positive integer.
     *
     * @param array<string, mixed> $payload
     * @return array{int, int}
     */
    private function deliveryRange(string $type, array $payload): array
    {
        $sequence = $payload['sequence'] ?? null;
        $span = $payload['sequence_span'] ?? 1;
        if (!is_int($sequence) || $sequence <= 0 || !is_int($span) || $span <= 0 || $sequence > PHP_INT_MAX - $span) {
            throw new NonDeterministicWorkflow(
                "{$type} history must name a positive workflow sequence and span.",
                is_int($sequence) ? $sequence : null,
                'positive integer sequence and sequence_span',
                'invalid delivery position',
                'durable_command_sequence_invalid',
            );
        }

        return [$sequence, $sequence + $span - 1];
    }

    private function refuseUpsertCollision(int $sequence, string $type, string $existing, string $incoming): never
    {
        throw new NonDeterministicWorkflow(
            "Workflow sequence {$sequence} records {$type} after a {$existing} command.",
            $sequence,
            'one durable command per workflow sequence',
            "{$existing} and {$incoming}",
            'durable_command_sequence_collision',
        );
    }

    /**
     * The command kind that owns a durable history row, or null for a row that
     * only points at a step. A wait's internal timeout timer belongs to that
     * wait, not to a workflow timer.
     *
     * @param array<string, mixed> $payload
     */
    private function durableCommandKind(string $type, array $payload): ?string
    {
        $kind = self::DURABLE_COMMAND_KINDS[$type] ?? null;
        if ($kind !== 'timer') {
            return $kind;
        }
        $timerKind = $payload['timer_kind'] ?? null;
        if ($timerKind === 'condition_timeout' || $this->stringValue($payload['condition_wait_id'] ?? null) !== null) {
            return 'condition';
        }
        if ($timerKind === 'signal_timeout' || $this->stringValue($payload['signal_wait_id'] ?? null) !== null) {
            return 'signal';
        }

        return 'timer';
    }

    /**
     * A recorded search-attribute upsert must name its own workflow step;
     * replay never synthesizes one. The reasons match sdk-rust.
     *
     * @param array<string, mixed> $payload
     */
    private function searchAttributeSequence(array $payload): int
    {
        if (!array_key_exists('sequence', $payload) || $payload['sequence'] === null) {
            throw new NonDeterministicWorkflow(
                'SearchAttributesUpserted history has no workflow sequence.',
                null,
                'positive integer sequence',
                'missing sequence',
                'durable_command_sequence_missing',
            );
        }
        $sequence = $payload['sequence'];
        if (!is_int($sequence) || $sequence <= 0) {
            throw new NonDeterministicWorkflow(
                'SearchAttributesUpserted history uses an invalid workflow sequence.',
                is_int($sequence) ? $sequence : null,
                'positive integer sequence',
                is_scalar($sequence) ? (string) $sequence : get_debug_type($sequence),
                'durable_command_sequence_invalid',
            );
        }

        return $sequence;
    }

    /** @param array<string, mixed> $payload */
    private function versionMarkerSequence(array $payload): int
    {
        $sequence = $payload['sequence'] ?? null;
        if (!is_int($sequence) || $sequence <= 0) {
            throw new NonDeterministicWorkflow(
                'Version-marker history is missing a positive integer workflow sequence.',
                is_int($sequence) ? $sequence : null,
                'positive integer sequence',
                is_scalar($sequence) ? (string) $sequence : get_debug_type($sequence),
                'version_marker_sequence_invalid',
            );
        }

        return $sequence;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array{string, int}
     */
    private function versionMarker(array $payload, int $sequence): array
    {
        $changeId = $payload['change_id'] ?? null;
        if (!is_string($changeId) || trim($changeId) === '') {
            throw new NonDeterministicWorkflow(
                "Version-marker history at workflow sequence {$sequence} is missing its stable change ID.",
                $sequence,
                'non-empty change_id',
                is_scalar($changeId) ? (string) $changeId : get_debug_type($changeId),
                'version_marker_field_missing',
            );
        }

        $version = $this->versionMarkerInteger($payload, 'version', $sequence);
        $minSupported = $this->versionMarkerInteger($payload, 'min_supported', $sequence);
        $maxSupported = $this->versionMarkerInteger($payload, 'max_supported', $sequence);
        if ($minSupported > $maxSupported || $version < $minSupported || $version > $maxSupported) {
            throw new NonDeterministicWorkflow(
                "Version marker {$changeId} at workflow sequence {$sequence} contains an incompatible recorded range.",
                $sequence,
                'min_supported <= version <= max_supported',
                "{$minSupported} <= {$version} <= {$maxSupported}",
                'version_marker_history_range_invalid',
            );
        }

        return [$changeId, $version];
    }

    /** @param array<string, mixed> $payload */
    private function versionMarkerInteger(array $payload, string $field, int $sequence): int
    {
        $value = $payload[$field] ?? null;
        if (!is_int($value) || $value < self::MIN_VERSION || $value > self::MAX_VERSION) {
            throw new NonDeterministicWorkflow(
                "Version-marker history at workflow sequence {$sequence} has a missing or invalid {$field} field.",
                $sequence,
                "32-bit integer {$field}",
                is_scalar($value) ? (string) $value : get_debug_type($value),
                'version_marker_field_missing',
            );
        }

        return $value;
    }

    /** @param array<string, mixed> $payload */
    private function historyEventShape(string $type, array $payload): ?string
    {
        if (in_array($type, ['ServiceCallStarted', 'ServiceCallCompleted', 'ServiceCallFailed', 'ServiceCallCancelled'], true)) {
            return 'service_operation';
        }
        if (str_starts_with($type, 'Activity')) {
            return 'activity';
        }
        if (in_array($type, ['TimerScheduled', 'TimerFired'], true)) {
            return in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)
                ? null
                : 'timer';
        }
        if ($type === 'ChildWorkflowScheduled' || str_starts_with($type, 'ChildRun')) {
            return 'child_workflow';
        }

        return match ($type) {
            'CancellationScopeOpened' => 'cancellation_scope',
            'SideEffectRecorded' => 'side_effect',
            'VersionMarkerRecorded' => 'version_marker',
            'MemoUpserted' => 'memo',
            'SearchAttributesUpserted' => 'search_attributes',
            'ConditionWaitOpened', 'ConditionWaitSatisfied', 'ConditionWaitTimedOut' => 'condition_wait',
            default => null,
        };
    }

    /**
     * @param array{version: int, sequence: ?int} $decision
     */
    private function assertVersionDecisionCompatible(
        string $changeId,
        array $decision,
        WorkflowCommand $command,
    ): void {
        $this->assertVersionSupported(
            $changeId,
            $decision['version'],
            (int) $command->attributes['min_supported'],
            (int) $command->attributes['max_supported'],
            $decision['sequence'],
        );
    }

    private function assertVersionSupported(
        string $changeId,
        int $version,
        int $minSupported,
        int $maxSupported,
        ?int $sequence,
    ): void {
        if ($version >= $minSupported && $version <= $maxSupported) {
            return;
        }

        throw new NonDeterministicWorkflow(
            "Recorded version {$version} for change ID {$changeId} is outside the supported range {$minSupported}..{$maxSupported}.",
            $sequence,
            "{$minSupported}..{$maxSupported}",
            (string) $version,
            'version_marker_incompatible_range',
        );
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, string> $conditionStepsByWaitId
     */
    private function conditionStepKey(array $payload, array $conditionStepsByWaitId): ?string
    {
        $waitId = $this->stringValue($payload['condition_wait_id'] ?? null);
        if ($waitId !== null && isset($conditionStepsByWaitId[$waitId])) {
            return $conditionStepsByWaitId[$waitId];
        }

        $sequence = $this->sequence($payload);

        return $sequence === null ? null : (string) $sequence;
    }

    /**
     * @param list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int,
     *     physical_condition_steps?: list<array<string, mixed>>
     * }> $steps
     * @return list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int,
     *     physical_condition_steps?: list<array<string, mixed>>
     * }>
     */
    private function collapseConditionReopens(array $steps): array
    {
        $collapsed = [];
        foreach ($steps as $step) {
            $last = array_key_last($collapsed);
            if ($last !== null
                && $step['shape'] === 'condition_wait'
                && $collapsed[$last]['shape'] === 'condition_wait'
                && $step['condition_key'] === $collapsed[$last]['condition_key']
                && $step['condition_definition_fingerprint'] === $collapsed[$last]['condition_definition_fingerprint']
                && $step['timeout_seconds'] === $collapsed[$last]['timeout_seconds']
                && $this->conditionWaitStillLive($collapsed[$last], $step)
                && ($this->stepOccurrence($collapsed[$last]) === null || $this->stepOccurrence($step) === null
                    || $this->stepOccurrence($collapsed[$last]) === $this->stepOccurrence($step))
                && $step['parallel_path'] === [] && $collapsed[$last]['parallel_path'] === []) {
                $step['sequence'] = $collapsed[$last]['sequence'];
                $step['opened_at'] = $collapsed[$last]['opened_at'] ?? null;
                $step['occurrence_id'] = $this->stepOccurrence($collapsed[$last]);
                $step['physical_condition_steps'] = [
                    ...($collapsed[$last]['physical_condition_steps'] ?? []),
                    ...($step['physical_condition_steps'] ?? []),
                ];
                $collapsed[$last] = $step;
                continue;
            }

            $collapsed[] = $step;
        }

        return $collapsed;
    }

    /**
     * @param array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int
     * } $step
     */
    private function assertConditionWaitCompatible(array $step, WorkflowCommand $command): void
    {
        $currentKey = $this->stringValue($command->attributes['condition_key'] ?? null);
        $currentFingerprint = $this->stringValue(
            $command->attributes['condition_definition_fingerprint'] ?? null,
        );
        $currentTimeout = $this->intValue($command->attributes['timeout_seconds'] ?? null);

        if ($step['condition_key'] !== $currentKey) {
            throw new NonDeterministicWorkflow(
                sprintf(
                    'Condition wait key changed from %s to %s during replay.',
                    $step['condition_key'] ?? '<none>',
                    $currentKey ?? '<none>',
                ),
                $step['sequence'],
                $step['condition_key'],
                $currentKey,
            );
        }
        if ($step['condition_definition_fingerprint'] !== null
            && !$command->conditionDefinitionMatches($step['condition_definition_fingerprint'])) {
            throw new NonDeterministicWorkflow(
                'Condition wait predicate fingerprint changed during replay.',
                $step['sequence'],
                $step['condition_definition_fingerprint'],
                $currentFingerprint,
            );
        }
        $recordedOccurrence = $this->stepOccurrence($step);
        $currentOccurrence = $this->stringValue($command->attributes['condition_wait_occurrence_id'] ?? null);
        // Only ids in this SDK's own format (php:condition-wait:N, #274) are
        // compared. A history without an id, or with an id in another format,
        // is not checked against the call.
        if ($recordedOccurrence !== null && $currentOccurrence !== null
            && str_starts_with($recordedOccurrence, 'php:condition-wait:') && $recordedOccurrence !== $currentOccurrence) {
            throw new NonDeterministicWorkflow(
                sprintf('Condition wait occurrence changed from %s to %s during replay.', $recordedOccurrence, $currentOccurrence),
                $step['sequence'],
                $recordedOccurrence,
                $currentOccurrence,
                'condition_wait_occurrence_mismatch',
            );
        }
        if ($step['timeout_seconds'] !== $currentTimeout) {
            throw new NonDeterministicWorkflow(
                sprintf(
                    'Condition wait timeout changed from %s to %s during replay.',
                    $step['timeout_seconds'] === null ? '<none>' : (string) $step['timeout_seconds'],
                    $currentTimeout === null ? '<none>' : (string) $currentTimeout,
                ),
                $step['sequence'],
                $step['timeout_seconds'] === null ? null : (string) $step['timeout_seconds'],
                $currentTimeout === null ? null : (string) $currentTimeout,
            );
        }
    }

    private function conditionDetail(?string $conditionKey, ?string $definitionFingerprint): string
    {
        return sprintf(
            'key=%s;predicate=%s',
            $conditionKey ?? '<none>',
            $definitionFingerprint ?? '<none>',
        );
    }

    private function stringValue(mixed $value): ?string
    {
        return is_string($value) && $value !== '' ? $value : null;
    }

    private function intValue(mixed $value): ?int
    {
        return is_int($value) ? $value : (is_numeric($value) ? (int) $value : null);
    }

    /** @return array{type: string, result: array{codec: string, blob: string}} */
    private function completeCommand(mixed $result): array
    {
        return ['type' => 'complete_workflow', 'result' => $this->codec->envelope($result)];
    }
}
