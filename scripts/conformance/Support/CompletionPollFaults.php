<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DateTimeImmutable;
use DateTimeZone;
use RuntimeException;

/** Select bounded real delays from an actual schema63 grant. Never changes a producer clock. */
final class CompletionPollFaults
{
    public const BUFFER_AGE_SECONDS = 1.25;

    /** @param array<string, mixed> $answer */
    public static function delay(array $answer, string $fault, float $maximumSeconds): float
    {
        $window = self::window($answer);
        // Transit/refence cases must cross the installed SDK's ten-second
        // confirmation reserve while the original lease is still live.
        if ($window < 12 || !is_finite($maximumSeconds) || $maximumSeconds <= 0) {
            throw new RuntimeException('completion_fault_grant_not_targetable');
        }
        $delay = match ($fault) {
            'transit', 'refence' => $window - 8,
            'expiry' => $window + 0.25,
            default => throw new RuntimeException('completion_fault_unknown'),
        };
        if ($delay > $maximumSeconds) {
            throw new RuntimeException('completion_fault_budget');
        }
        return $delay;
    }

    /** @param array<string, mixed> $answer */
    public static function window(array $answer): float
    {
        $clock = self::timestamp($answer['server_time'] ?? null);
        $expiry = self::timestamp($answer['task']['lease_expires_at'] ?? null);
        if (($answer['poll_status'] ?? null) !== 'leased' || $clock === null || $expiry === null || $expiry <= $clock) {
            throw new RuntimeException('completion_fault_original_grant_required');
        }
        return $expiry - $clock;
    }

    /** A replay may refresh its response clock, never these original task fields.
     * @param array<string, mixed> $answer
     */
    public static function identity(array $answer): string
    {
        $task = $answer['task'] ?? null;
        if (!is_array($task) || !is_string($task['task_id'] ?? null) || !is_string($task['lease_owner'] ?? null)
            || !is_string($task['lease_expires_at'] ?? null)) {
            throw new RuntimeException('completion_fault_original_grant_required');
        }
        // Retain only the digest. History, payloads and session documents do
        // not enter the public trace. Schema63's normal replay preserves the
        // whole task, including additional authority fields; only its envelope
        // server_time refreshes. Object key ordering is not an identity change.
        return hash('sha256', json_encode(self::ordered($task), JSON_THROW_ON_ERROR));
    }

    private static function ordered(mixed $value): mixed
    {
        if (!is_array($value)) return $value;
        if (!array_is_list($value)) ksort($value);
        return array_map(self::ordered(...), $value);
    }

    public static function timestamp(mixed $value): ?float
    {
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\z/', $value) !== 1) {
            return null;
        }
        $date = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s.u\Z', $value, new DateTimeZone('UTC'));
        return $date !== false && $date->format('Y-m-d\TH:i:s.u\Z') === $value ? (float) $date->format('U.u') : null;
    }
}
