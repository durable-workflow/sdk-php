<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;

/** Classifies full worker poll response envelopes using protocol fields. */
final class PollResponse
{
    /** @var list<string> */
    public const TERMINAL_POLL_STATUSES = [
        'stale_worker_registration',
        'draining',
        'stopped',
    ];

    /** @var list<string> */
    public const TERMINAL_REASONS = [
        'stale_worker_registration',
        'worker_heartbeat_stale',
        'worker_draining',
        'worker_stopped',
    ];

    /** @param array<string, mixed> $response */
    public static function isTerminal(array $response): bool
    {
        $pollStatus = $response['poll_status'] ?? null;
        $reason = $response['reason'] ?? null;

        return (is_string($pollStatus) && in_array($pollStatus, self::TERMINAL_POLL_STATUSES, true))
            || (is_string($reason) && in_array($reason, self::TERMINAL_REASONS, true));
    }

    /**
     * Identify transport failures that the worker protocol explicitly permits
     * a managed worker to retry. Generic service failures are intentionally
     * excluded even when they use the same HTTP status.
     */
    public static function isTransientFailure(ServerException $exception): bool
    {
        if (!in_array($exception->status, [429, 503], true)) {
            return false;
        }

        $response = $exception->details;
        if ($response === null || array_is_list($response) || !array_key_exists('task', $response)) {
            return false;
        }

        $pollStatus = $response['poll_status'] ?? null;
        $reason = $response['reason'] ?? null;
        if ($response['task'] !== null
            || !is_string($pollStatus)
            || $pollStatus === ''
            || !is_string($reason)
            || $reason === ''
            || $exception->reason !== $reason) {
            return false;
        }

        if (array_key_exists('retry_after_seconds', $response)
            && (!is_int($response['retry_after_seconds']) || $response['retry_after_seconds'] < 0)) {
            return false;
        }

        if ($exception->status === 503
            && $pollStatus === 'backend_lock_pressure'
            && $reason === 'backend_lock_pressure') {
            return isset($response['retry_after_seconds']) && $response['retry_after_seconds'] > 0;
        }

        return ($response['retryable'] ?? null) === true;
    }

    private function __construct()
    {
    }
}
