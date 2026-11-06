<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;

/** @internal Pure checks on immutable clock evidence, without wall time. */
final class ReplayClockHistory
{
    /** @param array<string, mixed> $body */
    public static function scalarRow(array $body, string $family): bool
    {
        if (!in_array($body['execution_mode'] ?? null, [null, 'remote'], true) || ($body['local_activity'] ?? false) !== false
            || ($body['parallel_group'] ?? null) !== null || !in_array($body['parallel_group_path'] ?? null, [null, []], true)
            || ($body['selection_group'] ?? null) !== null || ($body['selection_group_id'] ?? null) !== null
            || ($body['parallel_group_id'] ?? null) !== null || ($body['parallel_group_base_sequence'] ?? null) !== null
            || ($body['parallel_group_size'] ?? null) !== null || ($body['cancellation_cleanup'] ?? null) !== null
            || !in_array($body['cancellation_scope_id'] ?? null, [null, 'root'], true)) {
            return false;
        }

        return $family !== 'timer' || (in_array($body['timer_kind'] ?? null, [null, 'sleep', 'durable_timer'], true)
            && !array_key_exists('condition_wait_id', $body) && !array_key_exists('signal_wait_id', $body));
    }

    /** @param array<string, mixed> $event */
    public static function kind(array $event): ?string
    {
        if (array_key_exists('event_type', $event) && array_key_exists('type', $event) && $event['event_type'] !== $event['type']) {
            return null;
        }
        $kind = $event['event_type'] ?? $event['type'] ?? null;

        return is_string($kind) && $kind !== '' ? $kind : null;
    }

    /** @param array<string, mixed> $payload */
    public static function sequence(array $payload): ?int
    {
        $value = $payload['sequence'] ?? null;
        if (!is_int($value) || $value < 1 || (array_key_exists('workflow_sequence', $payload) && $payload['workflow_sequence'] !== $value)) {
            return null;
        }

        return $value;
    }

    /** Index every sequence the legacy resolver can claim, solely to refuse ambiguous evidence.
     * @param array<string, mixed> $payload
     */
    public static function legacySequenceClaim(array $payload): ?int
    {
        $value = $payload['sequence'] ?? $payload['workflow_sequence'] ?? null;

        return is_numeric($value) && (int) $value > 0 ? (int) $value : null;
    }

    public static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '';
    }

    /** Public-history timestamp is authoritative when present, including invalid values.
     * @param array<string, mixed> $event
     */
    public static function recordedTime(array $event): ?DateTimeImmutable
    {
        return self::parseTime(array_key_exists('timestamp', $event) ? $event['timestamp'] : ($event['recorded_at'] ?? null));
    }

    public static function parseTime(mixed $value): ?DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z/', $value) !== 1) {
            return null;
        }
        try {
            $time = new DateTimeImmutable($value);
        } catch (\Exception) {
            return null;
        }
        $errors = DateTimeImmutable::getLastErrors();
        return $errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)
            ? null : $time->setTimezone(new DateTimeZone('UTC'));
    }
}
