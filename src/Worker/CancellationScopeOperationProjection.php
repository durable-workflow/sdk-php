<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;

/** @internal Original admission facts, never operation execution or stop authority. */
final class CancellationScopeOperationProjection
{
    /** @return list<array<string, mixed>> */
    public static function normalize(string $field, mixed $members): array
    {
        $keys = match ($field) {
            'timer_members' => ['sequence', 'timer_id', 'descriptor_hash'],
            'wait_members' => ['kind', 'sequence', 'wait_id', 'timer_id', 'descriptor_hash'],
            'child_members' => ['sequence', 'child_call_id', 'child_workflow_instance_id', 'child_workflow_run_id', 'cancellation_policy', 'descriptor_hash'],
            default => throw new InvalidArgumentException('Unknown scope operation projection.'),
        };
        if (!is_array($members) || !array_is_list($members)) {
            throw new InvalidArgumentException('Scope operation projection must be a list.');
        }
        $result = []; $ids = []; $sequences = [];
        $idKey = match ($field) { 'timer_members' => 'timer_id', 'wait_members' => 'wait_id', default => 'child_call_id' };
        foreach ($members as $member) {
            if (!is_array($member) || count($member) !== count($keys)
                || !is_int($member['sequence'] ?? null) || $member['sequence'] < 1
                || !self::identity($member[$idKey] ?? null) || isset($ids[$member[$idKey]])
                || !is_string($member['descriptor_hash'] ?? null)
                || preg_match('/\A[a-f0-9]{64}\z/', $member['descriptor_hash']) !== 1) {
                throw new InvalidArgumentException('Scope operation projection changes an original member address.');
            }
            if ($field === 'wait_members' && (!in_array($member['kind'] ?? null, ['signal', 'condition'], true)
                || !array_key_exists('timer_id', $member)
                || ($member['timer_id'] !== null && !self::identity($member['timer_id'])))) {
                throw new InvalidArgumentException('Scope wait projection changes its kind or timeout address.');
            }
            if ($field === 'child_members' && (!self::identity($member['child_workflow_instance_id'] ?? null)
                || !self::identity($member['child_workflow_run_id'] ?? null)
                || !in_array($member['cancellation_policy'] ?? null, ['try_cancel', 'wait_cancellation_completed', 'abandon'], true)
                || isset($sequences[$member['sequence']]))) {
                throw new InvalidArgumentException('Scope child projection changes its original target or policy.');
            }
            $normalized = [];
            foreach ($keys as $key) {
                if (!array_key_exists($key, $member)) {
                    throw new InvalidArgumentException('Scope operation projection omits a frozen field.');
                }
                $normalized[$key] = $member[$key];
            }
            $ids[$member[$idKey]] = true; $sequences[$member['sequence']] = true;
            $result[] = $normalized;
        }

        return $result;
    }

    /**
     * @param list<array<string, mixed>> $prefix Events strictly before the original preparation.
     * @return list<array<string, mixed>>
     */
    public static function fromHistoryPrefix(string $field, array $prefix, string $scopeId, string $runId): array
    {
        $members = [];
        $timerSequences = [];
        foreach ($prefix as $index => $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $payload = $event['payload'];
            if ($field === 'timer_members' && $kind === 'TimerScheduled' && self::address($payload, 'timer') === $scopeId) {
                if (CancellationScopeTimerCleanup::fromHistory($event, array_slice($prefix, 0, $index)) !== null) {
                    continue;
                }
                $sequence = $payload['sequence'] ?? null; $id = $payload['timer_id'] ?? null;
                $delay = $payload['delay_seconds'] ?? null; $fireAt = $payload['fire_at'] ?? null;
                $timerKind = $payload['timer_kind'] ?? null;
                if (!is_int($sequence) || $sequence < 1 || !self::identity($id)
                    || !is_int($delay) || $delay < 0
                    || (array_key_exists($sequence, $timerSequences)
                        && (!in_array($timerKind, ['signal_timeout', 'condition_timeout'], true) || $timerSequences[$sequence] !== $timerKind))
                    || self::timestamp($fireAt)->format('Y-m-d\TH:i:s.u\Z') !== $fireAt) {
                    throw new InvalidArgumentException('Scope timer history changes its original descriptor.');
                }
                $timerSequences[$sequence] = $timerKind;
                $members[] = ['sequence' => $sequence, 'timer_id' => $id, 'descriptor_hash' => self::hash([
                    $scopeId, $event['id'], $sequence, $id, $delay, $fireAt, $timerKind,
                    $payload['condition_wait_id'] ?? null, $payload['condition_wait_occurrence_id'] ?? null,
                    $payload['signal_wait_id'] ?? null, self::groupPath($payload),
                ])];
            }
            if ($field === 'wait_members' && in_array($kind, ['SignalWaitOpened', 'ConditionWaitOpened'], true)
                && ($payload['cancellation_scope_id'] ?? 'root') === $scopeId) {
                $waitKind = $kind === 'SignalWaitOpened' ? 'signal' : 'condition';
                $sequence = $payload['sequence'] ?? null; $id = $payload[$waitKind.'_wait_id'] ?? null;
                if (!is_int($sequence) || $sequence < 1 || !self::identity($id)) {
                    throw new InvalidArgumentException('Scope wait history changes its original address.');
                }
                $timers = array_values(array_filter($prefix, static fn (array $row): bool =>
                    ($row['event_type'] ?? $row['type'] ?? null) === 'TimerScheduled'
                    && ($row['payload'][$waitKind.'_wait_id'] ?? null) === $id));
                if (count($timers) > 1) {
                    throw new InvalidArgumentException('Scope wait history duplicates its timeout.');
                }
                $timer = $timers[0] ?? null;
                if ($timer !== null && (($timer['payload']['timer_kind'] ?? null) !== $waitKind.'_timeout'
                    || ($timer['payload']['sequence'] ?? null) !== $sequence || $timer['sequence'] <= $event['sequence']
                    || ($timer['payload']['cancellation_scope_id'] ?? 'root') !== $scopeId
                    || !self::identity($timer['payload']['timer_id'] ?? null))) {
                    throw new InvalidArgumentException('Scope wait history changes its original timeout.');
                }
                $members[] = ['kind' => $waitKind, 'sequence' => $sequence, 'wait_id' => $id,
                    'timer_id' => $timer['payload']['timer_id'] ?? null, 'descriptor_hash' => self::hash([
                        $scopeId, $event['id'], $waitKind, $sequence, $id, $timer['id'] ?? null, $timer['payload']['timer_id'] ?? null,
                        $payload['timeout_seconds'] ?? null, $payload['signal_name'] ?? null,
                        $payload['condition_wait_occurrence_id'] ?? null, $payload['condition_key'] ?? null,
                        $payload['condition_definition_fingerprint'] ?? null, self::groupPath($payload),
                    ])];
            }
            if ($field === 'child_members' && $kind === 'ChildWorkflowScheduled' && self::address($payload, 'child_workflow') === $scopeId) {
                $sequence = $payload['sequence'] ?? null; $callId = $payload['child_call_id'] ?? null;
                $instance = $payload['child_workflow_instance_id'] ?? null; $targetRun = $payload['child_workflow_run_id'] ?? null;
                $policy = $payload['cancellation_policy'] ?? 'abandon';
                if (!is_int($sequence) || $sequence < 1 || !self::identity($callId) || !self::identity($instance)
                    || !self::identity($targetRun) || $targetRun === $runId
                    || !in_array($policy, ['try_cancel', 'wait_cancellation_completed', 'abandon'], true)) {
                    throw new InvalidArgumentException('Scope child history changes its original target or policy.');
                }
                $lastStartedId = null;
                foreach ($prefix as $started) {
                    $next = $started['payload'];
                    if (($started['event_type'] ?? $started['type'] ?? null) !== 'ChildRunStarted' || ($next['sequence'] ?? null) !== $sequence) {
                        continue;
                    }
                    if ($started['sequence'] <= $event['sequence'] || ($next['child_call_id'] ?? null) !== $callId
                        || ($next['child_workflow_instance_id'] ?? null) !== $instance
                        || !self::identity($next['child_workflow_run_id'] ?? null) || $next['child_workflow_run_id'] === $runId
                        || ($next['cancellation_scope_id'] ?? $scopeId) !== $scopeId || ($next['cancellation_policy'] ?? $policy) !== $policy) {
                        throw new InvalidArgumentException('Scope child history changes its original continuation.');
                    }
                    $targetRun = $next['child_workflow_run_id']; $lastStartedId = $started['id'];
                }
                $members[] = ['sequence' => $sequence, 'child_call_id' => $callId, 'child_workflow_instance_id' => $instance,
                    'child_workflow_run_id' => $targetRun, 'cancellation_policy' => $policy, 'descriptor_hash' => self::hash([
                        $scopeId, $event['id'], $sequence, $callId, $instance, $targetRun, $policy,
                        $payload['parent_close_policy'] ?? null, $payload['child_workflow_type'] ?? null, $lastStartedId, self::groupPath($payload),
                    ])];
            }
        }

        return self::normalize($field, $members);
    }

    public static function timestamp(mixed $value): DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Scope projection requires a timestamp with timezone.');
        }
        try { $time = new DateTimeImmutable($value); } catch (\Exception $error) {
            throw new InvalidArgumentException('Scope projection timestamp is invalid.', previous: $error);
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Scope projection timestamp is invalid.');
        }

        return $time->setTimezone(new DateTimeZone('UTC'));
    }

    public static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '' && strlen($value) <= 255 && preg_match('//u', $value) === 1;
    }

    /** @param array<string, mixed> $payload */
    private static function address(array $payload, string $descriptorKey): string
    {
        $descriptor = $payload[$descriptorKey] ?? [];
        if (!is_array($descriptor)) {
            throw new InvalidArgumentException('Scope operation needs its original descriptor.');
        }
        $nested = $descriptor['cancellation_scope_id'] ?? 'root';
        $address = $payload['cancellation_scope_id'] ?? $nested;
        if (!self::identity($address) || (array_key_exists('cancellation_scope_id', $descriptor) && $nested !== $address)) {
            throw new InvalidArgumentException('Scope operation changes its original membership.');
        }

        return $address;
    }

    /** @param list<mixed> $values */
    private static function hash(array $values): string { return hash('sha256', json_encode($values, JSON_THROW_ON_ERROR)); }

    /**
     * @param array<string, mixed> $payload
     * @return list<array<string, mixed>>
     */
    private static function groupPath(array $payload): array
    {
        $path = [];
        foreach (is_array($payload['parallel_group_path'] ?? null) ? $payload['parallel_group_path'] : [] as $entry) {
            if (is_array($entry) && ($metadata = self::groupEntry($entry)) !== null) { $path[] = $metadata; }
        }
        if ($path !== []) { return $path; }
        $metadata = self::groupEntry($payload);

        return $metadata === null ? [] : [$metadata];
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>|null
     */
    private static function groupEntry(array $payload): ?array
    {
        $id = is_string($payload['parallel_group_id'] ?? null) ? $payload['parallel_group_id'] : null;
        $kind = is_string($payload['parallel_group_kind'] ?? null) ? $payload['parallel_group_kind'] : match (true) {
            $id !== null && str_starts_with($id, 'parallel-activities:') => 'activity',
            $id !== null && (str_starts_with($id, 'parallel-calls:') || str_starts_with($id, 'select-calls:')) => 'mixed',
            $id !== null && str_starts_with($id, 'parallel-timers:') => 'timer',
            $id !== null && str_starts_with($id, 'parallel-children:') => 'child', default => null,
        };
        $mode = is_string($payload['parallel_group_mode'] ?? null) && $payload['parallel_group_mode'] !== '' ? $payload['parallel_group_mode']
            : ($id !== null && str_starts_with($id, 'select-calls:') ? 'select' : null);
        $key = $payload['selection_member_key'] ?? null;
        if ($id === null || $kind === null || !is_int($payload['parallel_group_base_sequence'] ?? null)
            || !is_int($payload['parallel_group_size'] ?? null) || $payload['parallel_group_size'] < 1
            || !is_int($payload['parallel_group_index'] ?? null)
            || ($mode === 'select' && !(is_int($key) && $key >= 0) && !(is_string($key) && $key !== ''))) { return null; }

        return array_filter([
            'parallel_group_id' => $id, 'parallel_group_kind' => $kind, 'parallel_group_mode' => $mode === 'select' ? $mode : null,
            'parallel_group_base_sequence' => $payload['parallel_group_base_sequence'], 'parallel_group_size' => $payload['parallel_group_size'],
            'parallel_group_index' => $payload['parallel_group_index'], 'selection_member_key' => $mode === 'select' ? $key : null,
            'selection_member_index' => is_int($payload['selection_member_index'] ?? null) ? $payload['selection_member_index'] : null,
            'selection_member_base_sequence' => is_int($payload['selection_member_base_sequence'] ?? null) ? $payload['selection_member_base_sequence'] : null,
            'selection_member_size' => is_int($payload['selection_member_size'] ?? null) ? $payload['selection_member_size'] : null,
            'selection_member_kind' => is_string($payload['selection_member_kind'] ?? null) && $payload['selection_member_kind'] !== '' ? $payload['selection_member_kind'] : null,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
