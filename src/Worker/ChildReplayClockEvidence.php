<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;

/** @internal Correlates a child terminal with its immutable parent lifecycle. */
final class ChildReplayClockEvidence
{
    private const CHILD_TERMINALS = ['ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'];

    private const CHILD_IDENTITY = ['child_call_id', 'child_workflow_instance_id', 'child_workflow_run_id', 'child_workflow_type'];

    /** Only the selected parent terminal supplies child business time.
     * @param list<array<string, mixed>> $history
     * @param list<int> $indices
     * @param (Closure(array<string, mixed>, string): bool)|null $rowMatches
     */
    public static function matches(array $history, array $indices, int $index, int $sequence, ?Closure $rowMatches = null): bool
    {
        $event = $history[$index];
        $payload = $event['payload'] ?? null;
        $identity = is_array($payload) ? self::childIdentity($payload) : null;
        if ($identity === null || ReplayClockHistory::sequence($payload) !== $sequence || !in_array(ReplayClockHistory::kind($event), self::CHILD_TERMINALS, true)) {
            return false;
        }
        $positions = [];
        $allowed = ['ChildWorkflowScheduled', 'ChildRunStarted', ...self::CHILD_TERMINALS];
        foreach ($indices as $position) {
            $row = $history[$position];
            $body = $row['payload'];
            $kind = ReplayClockHistory::kind($row);
            if ($position > $index || ReplayClockHistory::sequence($body) !== $sequence
                || !in_array($kind, $allowed, true) || self::childIdentity($body) !== $identity
                || !($rowMatches === null ? ReplayClockHistory::scalarRow($body, 'child_workflow') : $rowMatches($body, 'child_workflow'))) {
                return false;
            }
            $positions[$kind][] = $position;
        }
        $closures = array_merge(...array_values(array_intersect_key($positions, array_flip(self::CHILD_TERMINALS))));

        $openings = $positions['ChildWorkflowScheduled'] ?? [];
        $starts = $positions['ChildRunStarted'] ?? [];

        return $closures === [$index] && count($openings) === 1 && $openings[0] < $index && count($starts) <= 1
            && ($starts === [] || ($starts[0] > $openings[0] && $starts[0] < $index));
    }

    /** @param array<string, mixed> $payload
     * @return array<string, string>|null
     */
    private static function childIdentity(array $payload): ?array
    {
        $identity = [];
        foreach (self::CHILD_IDENTITY as $field) {
            $value = $payload[$field] ?? null;
            if (!ReplayClockHistory::identity($value)) {
                return null;
            }
            $identity[$field] = $value;
        }
        foreach (['workflow_instance_id' => 'child_workflow_instance_id', 'workflow_run_id' => 'child_workflow_run_id'] as $alias => $field) {
            if (array_key_exists($alias, $payload) && $payload[$alias] !== $identity[$field]) {
                return null;
            }
        }

        return $identity;
    }
}
