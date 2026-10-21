<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DateTimeImmutable;
use DateTimeZone;
use Throwable;

/** @internal One original activity grant. Runtime renewal never supplies user progress. */
final class ActivityLeaseAuthority
{
    /** @var array<string, string|int> */
    private array $identity;
    private float $leaseUntil;
    private float $leaseExpiry;
    private float $serverTime;
    private ?float $businessUntil;
    private ?float $heartbeatUntil;
    private ?float $heartbeatTime;
    private ?float $heartbeatDeadline;
    private ?int $heartbeatInterval;
    /** @var array{float, float, float}|null */
    private ?array $lastRenewal = null;
    private ?ActivityLeaseSession $session;
    private ?string $lostReason = null;
    private readonly Closure $clock;

    /**
     * @param array<string, mixed> $task
     * @param (Closure(): float)|null $clock Monotonic seconds, including in tests.
     */
    public function __construct(
        array $task,
        string $namespace,
        string $owner,
        string $queue,
        mixed $serverTime,
        private readonly float $capturedAt,
        ?Closure $clock = null,
    ) {
        $this->clock = $clock ?? static fn (): float => hrtime(true) / 1e9;
        try {
            $this->identity = $this->originalIdentity($task, $namespace, $owner, $queue);
            $this->serverTime = self::timestamp($serverTime);
            $this->leaseExpiry = self::timestamp($task['lease_expires_at'] ?? null);
            $this->leaseUntil = $capturedAt + self::duration($this->leaseExpiry, $this->serverTime);
            $this->captureDeadlines($task);
            $this->session = isset($task['worker_session'])
                ? new ActivityLeaseSession($task['worker_session'], $namespace, $owner, $this->serverTime, $capturedAt)
                : null;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('invalid_activity_claim', 'Activity delivery lacks valid original authority.', previous: $error);
        }
        $this->remaining();
    }

    /** @param array<string, mixed> $task
     * @return array<string, string|int>
     */
    private function originalIdentity(array $task, string $namespace, string $owner, string $queue): array
    {
        foreach (['task_id', 'workflow_id', 'run_id', 'activity_execution_id', 'activity_attempt_id',
            'lease_owner', 'task_queue'] as $key) {
            if (!is_string($task[$key] ?? null) || trim($task[$key]) === '' || strlen($task[$key]) > 255) {
                throw new \UnexpectedValueException('The delivery lacks its original '.$key.'.');
            }
        }
        if ($task['lease_owner'] !== $owner || $task['task_queue'] !== $queue
            || ($task['namespace'] ?? $namespace) !== $namespace
            || !is_int($task['attempt_number'] ?? null) || $task['attempt_number'] < 1
            || !is_finite($this->capturedAt) || $this->capturedAt < 0 || $this->capturedAt > ($this->clock)()) {
            throw new \UnexpectedValueException('The delivery does not belong to this Worker.');
        }

        return ['task_id' => $task['task_id'], 'workflow_task_id' => $task['task_id'],
            'workflow_instance_id' => $task['workflow_id'], 'workflow_run_id' => $task['run_id'],
            'activity_execution_id' => $task['activity_execution_id'],
            'activity_attempt_id' => $task['activity_attempt_id'], 'attempt_number' => $task['attempt_number'],
            'lease_owner' => $owner];
    }

    /** @param array<string, mixed> $task */
    private function captureDeadlines(array $task): void
    {
        $deadlines = $task['deadlines'] ?? [];
        if (!is_array($deadlines)) {
            throw new \UnexpectedValueException('Invalid activity deadlines.');
        }
        $bounds = [];
        foreach (['start_to_close', 'schedule_to_close', 'run', 'execution'] as $key) {
            if (isset($deadlines[$key])) {
                $bounds[] = $this->capturedAt + self::timestamp($deadlines[$key]) - $this->serverTime;
            }
        }
        $this->businessUntil = $bounds === [] ? null : min($bounds);
        $policy = $task['retry_policy'] ?? null;
        $interval = is_array($policy) ? ($policy['heartbeat_timeout'] ?? null) : null;
        $this->heartbeatInterval = is_int($interval) && $interval > 0 ? $interval : null;
        $this->heartbeatDeadline = isset($deadlines['heartbeat']) ? self::timestamp($deadlines['heartbeat']) : null;
        if ($this->heartbeatDeadline !== null && ($this->heartbeatInterval === null
            || $this->heartbeatDeadline - $this->serverTime > $this->heartbeatInterval)) {
            throw new \UnexpectedValueException('Missing original heartbeat policy.');
        }
        $this->heartbeatUntil = $this->heartbeatDeadline === null ? null
            : $this->capturedAt + $this->heartbeatDeadline - $this->serverTime;
        $this->heartbeatTime = $this->heartbeatDeadline === null ? null
            : $this->heartbeatDeadline - $this->heartbeatInterval;
    }

    public function remaining(): float
    {
        return $this->remainingAt(($this->clock)());
    }

    public function fixedBound(): float
    {
        return min($this->businessUntil ?? INF, $this->session?->ttlBound() ?? INF);
    }

    /** @return array<string, string|int> */
    public function identity(): array
    {
        return $this->identity;
    }

    /** @return array<string, mixed>|null */
    public function session(): ?array
    {
        return $this->session?->snapshot();
    }

    /**
     * A successful HTTP response is insufficient: it must acknowledge this
     * original attempt and must arrive before the preceding authority expires.
     * @param array<string, mixed> $reply
     */
    public function acknowledge(array $reply, float $started): void
    {
        $received = ($this->clock)();
        $this->remainingAt($received);
        try {
            $this->assertRoundTrip($started, $received);
            $this->assertRenewalIdentity($reply);
            $server = self::timestamp($reply['server_time'] ?? null);
            $expiry = self::timestamp($reply['lease_expires_at'] ?? null);
            $until = $started + self::duration($expiry, $server);
            if ($until <= $received) {
                throw new \UnexpectedValueException('The renewed lease was consumed by transport.');
            }
            // An application heartbeat can overtake an in-flight runtime ping.
            // That older ping grants nothing and must not undo actual progress.
            if ($this->heartbeatTime !== null && $server < $this->heartbeatTime && $expiry <= $this->leaseExpiry) {
                return;
            }
            if ($server < $this->serverTime || $expiry < $this->leaseExpiry) {
                throw new \UnexpectedValueException('Renewal predates the confirmed activity grant.');
            }
            $confirmation = [$server, $expiry, $started];
            if ($this->lastRenewal !== null && $server <= $this->lastRenewal[0]) {
                if ($confirmation === $this->lastRenewal) {
                    return;
                }
                throw new \UnexpectedValueException('Replayed activity renewal.');
            }
            $this->observeProgress($reply, $server, $started);
            $this->acknowledgeRequiredSession($reply, $server, $started);
            $this->leaseUntil = max($this->leaseUntil, $until);
            $this->serverTime = $server;
            $this->leaseExpiry = $expiry;
            $this->lastRenewal = $confirmation;
        } catch (Throwable $error) {
            $this->lose('invalid_activity_renewal', $error);
        }
        $this->remaining();
    }

    /** @param array<string, mixed> $reply */
    private function assertRenewalIdentity(array $reply): void
    {
        foreach ($this->identity as $key => $value) {
            if (($reply[$key] ?? null) !== $value) {
                throw new \UnexpectedValueException('Activity renewal changed '.$key.'.');
            }
        }
        if (($reply['lease_renewed'] ?? null) !== true || ($reply['heartbeat_recorded'] ?? null) !== false) {
            throw new \UnexpectedValueException('Runtime renewal did not acknowledge a metadata-only grant.');
        }
        $this->assertContinuation($reply);
    }

    /** @param array<string, mixed> $reply */
    private function acknowledgeRequiredSession(array $reply, float $server, float $started): void
    {
        if ($this->session === null) {
            return;
        }
        if (!is_array($reply['worker_session'] ?? null)) {
            throw new \UnexpectedValueException('Required session grant is missing from renewal.');
        }
        $this->session->acknowledge($reply['worker_session'], $server, $started);
    }

    /**
     * Only an actual user heartbeat can move the recorded progress deadline.
     * The receipt instant is retained when the parent relays it over private IPC.
     * @param array<string, mixed> $reply
     */
    public function acknowledgeProgress(array $reply, float $started, ?float $received = null): void
    {
        $received ??= ($this->clock)();
        $this->remainingAt($received);
        try {
            $this->assertRoundTrip($started, $received);
            foreach (['task_id', 'activity_attempt_id', 'lease_owner'] as $key) {
                if (($reply[$key] ?? null) !== $this->identity[$key]) {
                    throw new \UnexpectedValueException('Heartbeat identifies another activity.');
                }
            }
            $this->assertContinuation($reply);
            if (($reply['heartbeat_recorded'] ?? null) !== true) {
                throw new \UnexpectedValueException('No application heartbeat was recorded.');
            }
            $progress = self::timestamp($reply['last_heartbeat_at'] ?? null);
            $expiry = self::timestamp($reply['lease_expires_at'] ?? null);
            $until = $started + self::duration($expiry, $progress);
            if ($until <= $received) {
                throw new \UnexpectedValueException('The heartbeat grant was consumed by transport.');
            }
            if ($this->heartbeatTime !== null && $progress <= $this->heartbeatTime) {
                return;
            }
            if ($this->heartbeatInterval !== null) {
                $deadline = self::timestamp($reply['heartbeat_deadline_at'] ?? null);
                if (abs($deadline - $progress - $this->heartbeatInterval) > 0.000001) {
                    throw new \UnexpectedValueException('Heartbeat changed the recorded progress interval.');
                }
                $this->heartbeatUntil = max($this->heartbeatUntil ?? 0, $started + $this->heartbeatInterval);
                $this->heartbeatDeadline = $deadline;
            }
            $this->heartbeatTime = $progress;
            $this->leaseUntil = max($this->leaseUntil, $until);
            $this->leaseExpiry = max($this->leaseExpiry, $expiry);
        } catch (Throwable $error) {
            $this->lose('invalid_activity_heartbeat', $error);
        }
        $this->remaining();
    }

    /** @param array<string, mixed> $reply */
    public function acknowledgeSession(array $reply, float $started): void
    {
        $this->remaining();
        try {
            $this->assertRoundTrip($started, ($this->clock)());
            if ($this->session === null) {
                throw new \UnexpectedValueException('No original session was required.');
            }
            $this->session->acknowledge($reply, self::timestamp($reply['server_time'] ?? null), $started);
        } catch (Throwable $error) {
            $this->lose('invalid_activity_session', $error);
        }
        $this->remaining();
    }

    private function remainingAt(float $now): float
    {
        if ($this->lostReason === null) {
            $remaining = min($this->leaseUntil, $this->businessUntil ?? INF,
                $this->heartbeatUntil ?? INF, $this->session?->bound() ?? INF) - $now;
            if ($remaining > 0) {
                return $remaining;
            }
            $this->lostReason = 'activity_execution_budget_expired';
        }
        throw new WorkflowClaimRevoked($this->lostReason, 'The original activity execution authority is unavailable.',
            taskId: (string) $this->identity['task_id']);
    }

    /** @param array<string, mixed> $reply */
    private function assertContinuation(array $reply): void
    {
        if (($reply['can_continue'] ?? null) !== true || ($reply['cancel_requested'] ?? null) !== false
            || ($reply['reason'] ?? null) !== null) {
            throw new \UnexpectedValueException('The Server refused the original activity claim.');
        }
    }

    private function assertRoundTrip(float $started, float $received): void
    {
        if (!is_finite($started) || !is_finite($received) || $started < $this->capturedAt
            || $started > $received || $received > ($this->clock)()) {
            throw new \UnexpectedValueException('Invalid monotonic activity request timing.');
        }
    }

    /** @param array<string, mixed> $reply */
    private function observeProgress(array $reply, float $server, float $started): void
    {
        if ($this->heartbeatInterval === null || $this->heartbeatUntil === null) {
            return;
        }
        $progress = self::timestamp($reply['last_heartbeat_at'] ?? null);
        $deadline = self::timestamp($reply['heartbeat_deadline_at'] ?? null);
        if ($progress > $server || abs($deadline - $progress - $this->heartbeatInterval) > 0.000001) {
            throw new \UnexpectedValueException('Runtime renewal changed application progress policy.');
        }
        $bound = $started + $deadline - $server;
        $this->heartbeatUntil = $progress > $this->heartbeatTime
            ? max($this->heartbeatUntil, $bound) : min($this->heartbeatUntil, $bound);
        $this->heartbeatTime = max($this->heartbeatTime ?? 0, $progress);
        $this->heartbeatDeadline = $deadline;
    }

    private function lose(string $reason, Throwable $error): never
    {
        $this->lostReason = $reason;
        throw new WorkflowClaimRevoked($reason, 'Activity continuation could not be confirmed.', previous: $error,
            taskId: (string) $this->identity['task_id']);
    }

    /** @internal Canonical UTC6 timestamps cannot normalize an invalid calendar date. */
    public static function timestamp(mixed $value): float
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\z/', $value) !== 1) {
            throw new \UnexpectedValueException('Activity timing must be UTC with six fractional digits.');
        }
        $date = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s.u\Z', $value, new DateTimeZone('UTC'));
        if ($date === false || $date->format('Y-m-d\TH:i:s.u\Z') !== $value) {
            throw new \UnexpectedValueException('Activity timing contains an invalid calendar date.');
        }

        return (float) $date->format('U.u');
    }

    private static function duration(float $expiry, float $server): float
    {
        $duration = $expiry - $server;
        if ($duration <= 0 || $duration > 86400) {
            throw new \UnexpectedValueException('Activity lease duration is invalid.');
        }

        return $duration;
    }
}
