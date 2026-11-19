<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * @internal Which received signals workflow code may observe (sdk-php#434).
 *
 * The Server records `SignalReceived` when it accepts a signal, and applies the
 * signal to the run later, in message order with updates, when the workflow
 * task that delivers it completes. Until then the signal can still be rejected,
 * for example when the run closes first (`SignalRejected`). Workflow code sees
 * a signal only once it is applied, as embedded Workflow, Python and Rust do:
 *
 * - a `SignalApplied` event names its signal ID or command ID;
 * - its `SignalReceived` carries a `workflow_sequence`, which the Server adds
 *   from the signal record once a wait has consumed it;
 * - it is the signal this workflow task delivers (`resume_source_kind:
 *   workflow_signal` with its `workflow_signal_id` or `workflow_command_id`),
 *   which the task's completion applies; or
 * - its `SignalReceived` names neither a signal ID nor a command ID, so the
 *   history cannot say whether it was applied (imported or synthetic
 *   histories, and test fixtures). These remain visible as before.
 *
 * Histories recorded before this rule let earlier tasks see received but
 * unapplied signals. The Replayer keeps them replayable: when hiding such a
 * signal contradicts the recorded history, it replays again with
 * withRecordedObservations(), which also shows every signal received before a
 * later workflow-task decision was recorded. An earlier SDK showed those
 * signals to the task that recorded that decision. A signal received after the
 * last recorded decision was seen by no earlier task and stays hidden until it
 * is applied (see Replayer::replay()).
 */
final class AppliedSignals
{
    /** Events only a workflow-task completion records: decisions taken by workflow code. */
    private const DECISION_EVENTS = [
        'ActivityScheduled', 'TimerScheduled', 'ChildWorkflowScheduled', 'ConditionWaitOpened', 'SignalWaitOpened',
        'SideEffectRecorded', 'VersionMarkerRecorded', 'MemoUpserted', 'SearchAttributesUpserted',
        'ServiceOperationScheduled', 'CancellationScopeOpened', 'UpdateCompleted', 'WorkflowCompleted',
        'WorkflowFailed', 'WorkflowContinuedAsNew',
    ];

    /**
     * @param array<string, true> $signalIds
     * @param array<string, true> $commandIds
     * @param array<int, true> $observedOrders history positions of signals shown for compatibility
     */
    private function __construct(
        private readonly array $signalIds,
        private readonly array $commandIds,
        private readonly array $observedOrders = [],
    ) {
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    public static function fromHistory(array $history, array $task): self
    {
        $signalIds = [];
        $commandIds = [];
        foreach ($history as $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $applied = $type === 'SignalApplied'
                || ($type === 'SignalReceived' && self::recordedSequence($payload['workflow_sequence'] ?? null));
            if (!$applied) {
                continue;
            }
            if (($signalId = self::identity($payload['signal_id'] ?? null)) !== null) {
                $signalIds[$signalId] = true;
            }
            if (($commandId = self::identity($payload['workflow_command_id'] ?? $event['workflow_command_id'] ?? null)) !== null) {
                $commandIds[$commandId] = true;
            }
        }
        if (($task['resume_source_kind'] ?? null) === 'workflow_signal') {
            if (($signalId = self::identity($task['workflow_signal_id'] ?? $task['resume_source_id'] ?? null)) !== null) {
                $signalIds[$signalId] = true;
            }
            if (($commandId = self::identity($task['workflow_command_id'] ?? null)) !== null) {
                $commandIds[$commandId] = true;
            }
        }

        return new self($signalIds, $commandIds);
    }

    /**
     * The compatibility view for a history recorded while every received signal
     * was visible: also show each signal received before a later recorded
     * workflow-task decision.
     *
     * @param list<array<string, mixed>> $history
     */
    public function withRecordedObservations(array $history): self
    {
        $observed = [];
        $pending = [];
        foreach ($history as $order => $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            if ($type === 'SignalReceived') {
                $pending[] = $order;
            } elseif (in_array($type, self::DECISION_EVENTS, true)) {
                foreach ($pending as $received) {
                    $observed[$received] = true;
                }
                $pending = [];
            }
        }

        return new self($this->signalIds, $this->commandIds, $observed);
    }

    /**
     * @param array<string, mixed> $event a `SignalReceived` event
     * @param int|null $order its position in the history
     */
    public function observes(array $event, ?int $order = null): bool
    {
        if ($order !== null && isset($this->observedOrders[$order])) {
            return true;
        }
        $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
        if (self::recordedSequence($payload['workflow_sequence'] ?? null)) {
            return true;
        }
        $signalId = self::identity($payload['signal_id'] ?? null);
        $commandId = self::identity($payload['workflow_command_id'] ?? $event['workflow_command_id'] ?? null);
        if ($signalId === null && $commandId === null) {
            return true;
        }

        return ($signalId !== null && isset($this->signalIds[$signalId]))
            || ($commandId !== null && isset($this->commandIds[$commandId]));
    }

    /**
     * Whether any received signal in the history is hidden from workflow code.
     *
     * @param list<array<string, mixed>> $history
     */
    public function hidesAny(array $history): bool
    {
        foreach ($history as $order => $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) === 'SignalReceived' && !$this->observes($event, $order)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Whether this view shows a received signal the other view hides.
     *
     * @param list<array<string, mixed>> $history
     */
    public function showsMoreThan(self $other, array $history): bool
    {
        foreach ($history as $order => $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) === 'SignalReceived'
                && $this->observes($event, $order) && !$other->observes($event, $order)) {
                return true;
            }
        }

        return false;
    }

    private static function identity(mixed $value): ?string
    {
        return (is_string($value) && $value !== '') || is_int($value) ? (string) $value : null;
    }

    private static function recordedSequence(mixed $value): bool
    {
        return (is_int($value) && $value > 0) || (is_string($value) && preg_match('/\A[1-9][0-9]*\z/', $value) === 1);
    }
}
