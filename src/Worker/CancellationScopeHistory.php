<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\NonDeterministicWorkflow;

/** @internal Canonical authoring tree and immediate operation membership. */
final class CancellationScopeHistory
{
    /** @var array<int, array{scope_id: string, parent_scope_id: string, shield_parent: bool}> */
    public readonly array $openings;

    /** @var array<int, string> */
    public readonly array $memberships;

    /** @param list<array<string, mixed>> $history */
    public function __construct(array $history, string $runId)
    {
        $openings = [];
        $memberships = [];
        $scopes = [];
        $eventIds = [];
        $lastOpening = 0;
        $lastHistorySequence = 0;
        $namespace = null;
        $hasScopes = false;
        foreach ($history as $event) {
            $hasScopes = $hasScopes || ($event['event_type'] ?? $event['type'] ?? null) === 'CancellationScopeOpened';
        }
        foreach ($history as $event) {
            if ($hasScopes) {
                if (!self::identity($event['id'] ?? null) || isset($eventIds[$event['id']])
                    || !is_int($event['sequence'] ?? null) || $event['sequence'] <= $lastHistorySequence
                    || !self::identity($event['namespace'] ?? null)
                    || ($namespace !== null && $namespace !== $event['namespace'])) {
                    throw new NonDeterministicWorkflow('Scope history changed canonical identity, order or namespace.', reason: 'invalid_cancellation_scope_history');
                }
                $eventIds[$event['id']] = true;
                $lastHistorySequence = $event['sequence'];
                $namespace = $event['namespace'];
            }
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            if ($kind === 'CancellationScopeOpened') {
                $sequence = $payload['sequence'] ?? null;
                $scopeId = $payload['scope_id'] ?? null;
                $parent = $payload['parent_scope_id'] ?? null;
                if (($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope/v1'
                    || $runId === '' || ($payload['workflow_run_id'] ?? null) !== $runId
                    || !is_int($sequence) || $sequence <= $lastOpening
                    || !self::identity($scopeId) || $scopeId === 'root' || isset($scopes[$scopeId])
                    || !self::identity($parent) || ($parent !== 'root' && !isset($scopes[$parent]))
                    || !is_bool($payload['shield_parent'] ?? null)) {
                    throw new NonDeterministicWorkflow('Invalid canonical cancellation scope opening.', reason: 'invalid_cancellation_scope_history');
                }
                $lastOpening = $sequence;
                $scopes[$scopeId] = $sequence;
                $openings[$sequence] = ['scope_id' => $scopeId, 'parent_scope_id' => $parent,
                    'shield_parent' => $payload['shield_parent']];
                continue;
            }
            if (!in_array($kind, [
                'ActivityScheduled', 'ActivityStarted', 'ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled',
                'ActivityRetryScheduled', 'TimerScheduled', 'TimerFired', 'TimerCancelled',
                'ChildWorkflowScheduled', 'ChildRunStarted', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated',
                'ConditionWaitOpened', 'ConditionWaitSatisfied', 'ConditionWaitTimedOut', 'ConditionWaitCancelled',
                'SignalWaitOpened', 'SignalWaitReceived', 'SignalWaitTimedOut', 'SignalWaitCancelled',
            ], true)) {
                continue;
            }
            $sequence = $payload['sequence'] ?? null;
            $membership = null;
            foreach ([$payload, $payload['activity'] ?? [], $payload['timer'] ?? [], $payload['child_workflow'] ?? []] as $snapshot) {
                if (!is_array($snapshot) || !array_key_exists('cancellation_scope_id', $snapshot)) {
                    continue;
                }
                $incoming = $snapshot['cancellation_scope_id'];
                if (!self::identity($incoming) || ($membership !== null && $membership !== $incoming)) {
                    throw new NonDeterministicWorkflow('Contradictory cancellation scope membership.', reason: 'invalid_cancellation_scope_history');
                }
                $membership = $incoming;
            }
            $isAdmission = in_array($kind, ['ActivityScheduled', 'TimerScheduled', 'ChildWorkflowScheduled', 'ConditionWaitOpened', 'SignalWaitOpened'], true);
            if ($membership === null && !$isAdmission) {
                continue;
            }
            $membership ??= 'root';
            if ($membership !== 'root' && (!is_int($sequence) || $sequence < 1
                || !isset($scopes[$membership]) || $scopes[$membership] >= $sequence
                || ($event['namespace'] ?? null) !== $namespace)) {
                throw new NonDeterministicWorkflow('Operation scope was not opened before its original admission.', reason: 'invalid_cancellation_scope_history');
            }
            if (!is_int($sequence) || $sequence < 1) {
                continue; // Preserve historical unscoped sequence fallbacks.
            }
            if (isset($memberships[$sequence]) && $memberships[$sequence] !== $membership) {
                throw new NonDeterministicWorkflow('Operation cancellation scope changed in history.', $sequence, reason: 'cancellation_scope_membership_changed');
            }
            $memberships[$sequence] = $membership;
        }
        $this->openings = $openings;
        $this->memberships = $memberships;
    }

    private static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '' && strlen($value) <= 255 && preg_match('//u', $value) === 1;
    }
}
