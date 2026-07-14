<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

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

    private function __construct()
    {
    }
}
