<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use InvalidArgumentException;

/** @internal Frozen admission facts, never proof of callback stop or effect authority. */
final class CancellationScopeActivityProjection
{
    /** @return list<array{sequence: int, activity_execution_id: string, descriptor_hash: string}> */
    public static function normalize(mixed $members): array
    {
        if (!is_array($members) || !array_is_list($members)) {
            throw new InvalidArgumentException('Scope Activity projection must be a list.');
        }
        $normalized = [];
        $ids = [];
        $sequences = [];
        foreach ($members as $member) {
            if (!is_array($member) || count($member) !== 3 || !is_int($member['sequence'] ?? null)
                || $member['sequence'] < 1 || !self::identity($member['activity_execution_id'] ?? null)
                || !is_string($member['descriptor_hash'] ?? null)
                || preg_match('/\A[a-f0-9]{64}\z/', $member['descriptor_hash']) !== 1
                || isset($ids[$member['activity_execution_id']]) || isset($sequences[$member['sequence']])) {
                throw new InvalidArgumentException('Scope Activity projection changes an original member address.');
            }
            $ids[$member['activity_execution_id']] = true;
            $sequences[$member['sequence']] = true;
            $normalized[] = ['sequence' => $member['sequence'], 'activity_execution_id' => $member['activity_execution_id'],
                'descriptor_hash' => $member['descriptor_hash']];
        }

        return $normalized;
    }

    /**
     * @param list<array<string, mixed>> $prefix Only events before the original preparation.
     * @return list<array{sequence: int, activity_execution_id: string, descriptor_hash: string}>
     */
    public static function fromHistoryPrefix(array $prefix, string $scopeId): array
    {
        $members = [];
        $ids = [];
        $sequences = [];
        foreach ($prefix as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'ActivityScheduled') {
                continue;
            }
            $payload = $event['payload'];
            $activity = $payload['activity'] ?? null;
            if (!is_array($activity) || array_is_list($activity)) {
                throw new InvalidArgumentException('Scope Activity admission requires its original descriptor.');
            }
            $nested = $activity['cancellation_scope_id'] ?? 'root';
            $address = $payload['cancellation_scope_id'] ?? $nested;
            $id = $payload['activity_execution_id'] ?? null;
            $sequence = $payload['sequence'] ?? null;
            if (!self::identity($id) || ($activity['id'] ?? null) !== $id || !is_int($sequence) || $sequence < 1
                || isset($ids[$id]) || isset($sequences[$sequence]) || $address !== $nested) {
                throw new InvalidArgumentException('Scope Activity history changes identity or membership.');
            }
            $ids[$id] = true;
            $sequences[$sequence] = true;
            if ($address !== $scopeId) {
                continue;
            }
            if (isset($payload['local_preparation']['cancellation_cleanup']['scope_id'])
                || isset($payload['local_group_admission']['cancellation_cleanup']['scope_id'])) {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: nested cleanup projections need their original cleanup proof.');
            }
            $policy = $activity['cancellation_policy'] ?? 'try_cancel';
            if (!in_array($policy, ['try_cancel', 'wait_cancellation_completed', 'abandon'], true)
                || (array_key_exists('local_activity', $payload) && !is_bool($payload['local_activity']))
                || (isset($payload['execution_mode']) && !is_string($payload['execution_mode']))
                || (isset($activity['schedule_to_close_deadline_at']) && !is_string($activity['schedule_to_close_deadline_at']))) {
                throw new InvalidArgumentException('Scope Activity history changes its cancellation descriptor.');
            }
            // Match Native's v5 scalar projection exactly, without application payload bytes.
            $members[] = ['sequence' => $sequence, 'activity_execution_id' => $id,
                'descriptor_hash' => hash('sha256', json_encode([
                    $scopeId, $event['id'], $policy, $payload['local_activity'] ?? false,
                    $payload['execution_mode'] ?? null, $activity['schedule_to_close_deadline_at'] ?? null,
                ], JSON_THROW_ON_ERROR))];
        }

        return $members;
    }

    private static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '' && strlen($value) <= 255 && preg_match('//u', $value) === 1;
    }
}
