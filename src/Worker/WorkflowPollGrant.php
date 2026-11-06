<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;

/** @internal A short-lived optimization of the original fenced workflow grant. */
final class WorkflowPollGrant
{
    /** @param array<string, mixed> $identity */
    private function __construct(
        private readonly array $identity,
        private readonly float $receivedAt,
        private readonly float $until,
    ) {
    }

    /** @param array<string, mixed> $response */
    public static function observe(array $response, string $workerId, float $startedAt, float $receivedAt): ?self
    {
        $task = $response['task'] ?? null;
        if (!is_array($task) || ($response['poll_status'] ?? null) !== 'leased'
            || !is_string($task['task_id'] ?? null) || $task['task_id'] === ''
            || ($task['lease_owner'] ?? null) !== $workerId
            || !is_int($task['workflow_task_attempt'] ?? null) || $task['workflow_task_attempt'] < 1
            || !is_finite($startedAt) || !is_finite($receivedAt) || $receivedAt < $startedAt) {
            return null;
        }
        $server = self::timestamp($response['server_time'] ?? null);
        $expiry = self::timestamp($task['lease_expires_at'] ?? null);
        if ($server === null || $expiry === null) {
            return null;
        }

        // Charge the whole request, including transit, without trusting the worker's wall clock.
        return new self(array_intersect_key($task, array_flip([
            'task_id', 'lease_owner', 'workflow_task_attempt', 'lease_expires_at',
        ])), $receivedAt, $startedAt + ($expiry - $server));
    }

    /** @param array<string, mixed> $task */
    public function isFreshFor(array $task, float $now): bool
    {
        if (!is_finite($now) || $now < $this->receivedAt || $now - $this->receivedAt >= 1
            || $this->until - $now < 10) {
            return false;
        }
        foreach ($this->identity as $key => $value) {
            if (($task[$key] ?? null) !== $value) {
                return false;
            }
        }

        return true;
    }

    private static function timestamp(mixed $value): ?float
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\z/', $value) !== 1) {
            return null;
        }
        $date = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s.u\Z', $value, new DateTimeZone('UTC'));
        if ($date === false || $date->format('Y-m-d\TH:i:s.u\Z') !== $value) {
            return null;
        }

        return (float) $date->format('U.u');
    }
}
