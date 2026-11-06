<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Auth\StaticHeadersAuthentication;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\RequestBudget;
use Throwable;

/** @internal Private metadata-only helper. Never dispatches an activity callback. */
final class ActivityLeaseMaintainer
{
    private string $buffer = '';
    private ?string $lostReason = null;
    private float $nextRenewal = 0;
    private float $nextPresence = 0;
    private readonly ActivityLeaseAuthority $authority;
    private readonly Client $client;

    /** @param array<string, mixed> $start */
    private function __construct(private readonly array $start, ?Client $client = null)
    {
        $configuration = $start['client'];
        if (!is_array($configuration) || !is_array($start['task'] ?? null)
            || !is_string($start['owner'] ?? null) || !is_string($start['queue'] ?? null)
            || !is_numeric($start['captured_at'] ?? null) || !is_int($start['parent_pid'] ?? null)) {
            throw new \UnexpectedValueException('Malformed private activity maintenance configuration.');
        }
        $this->client = $client ?? self::client($configuration)->withBoundedWorkerRequests();
        $this->authority = new ActivityLeaseAuthority($start['task'], $this->client->namespace,
            $start['owner'], $start['queue'], $start['server_time'] ?? null, (float) $start['captured_at']);
        $due = $start['presence_due_in'] ?? 0;
        $this->nextPresence = (float) $start['captured_at']
            + (is_int($due) || is_float($due) ? max(0, min(10, $due)) : 0);
    }

    public static function run(): void
    {
        stream_set_blocking(STDIN, false);
        $parent = null;
        $configuration = null;
        $client = null;
        $proven = false;
        while (true) {
            $start = [];
            try {
                $start = self::awaitStart($parent);
                $key = json_encode($start['client'] ?? null, JSON_THROW_ON_ERROR);
                if ($start === null || ($start['operation'] ?? null) !== 'start'
                    || ($configuration !== null && $key !== $configuration)
                    || ($parent !== null && ($start['parent_pid'] ?? null) !== $parent)) {
                    return;
                }
                $maintainer = new self($start, $client);
                $parent = $start['parent_pid'];
                $configuration = $key;
                $client = $maintainer->client;
                $maintainer->maintainInitialGrant($proven);
                $proven = true;
                $maintainer->respond($start);
                if (!$maintainer->loop()) {
                    return;
                }
            } catch (Throwable) {
                // Bootstrap exceptions can contain application secrets. Only a
                // bounded protocol reason is ever written to the parent pipe.
                self::write(['id' => $start['id'] ?? 0,
                    'can_continue' => false, 'reason' => 'activity_maintenance_bootstrap_failed']);
                return;
            }
        }
    }

    /** @return array<string, mixed>|null */
    private static function awaitStart(?int $parent): ?array
    {
        $buffer = '';
        while (!str_contains($buffer, "\n")) {
            if ($parent !== null && function_exists('posix_getppid') && posix_getppid() !== $parent) {
                return null;
            }
            $read = [STDIN];
            $write = $except = null;
            if (@stream_select($read, $write, $except, 1) === false) {
                // Windows process pipes are not selectable; keep the existing
                // bounded nonblocking-read path on that platform and on EINTR.
                usleep(10000);
            }
            $part = stream_get_contents(STDIN, 65537 - strlen($buffer));
            if ($part === false || strlen($part) + strlen($buffer) > 65536 || feof(STDIN)) {
                return null;
            }
            $buffer .= $part;
        }
        [$line, $remaining] = explode("\n", $buffer, 2);
        $command = $remaining === '' ? json_decode($line, true, 32, JSON_THROW_ON_ERROR) : null;

        return is_array($command) ? $command : null;
    }

    /** @param array<string, mixed> $configuration */
    private static function client(array $configuration): Client
    {
        if (!is_string($configuration['base_uri'] ?? null) || !is_string($configuration['namespace'] ?? null)
            || !is_string($configuration['worker_protocol'] ?? null)) {
            throw new \UnexpectedValueException('Missing maintenance runtime identity.');
        }
        $bootstrap = $configuration['bootstrap'] ?? null;
        if ($bootstrap !== null) {
            if (!is_string($bootstrap)) {
                throw new \UnexpectedValueException('Malformed activity bootstrap.');
            }
            ob_start();
            try {
                $client = require $bootstrap;
            } finally {
                $output = ob_get_clean();
            }
            if ($output !== '' || !$client instanceof Client
                || $client->activityLeaseEndpoint() !== $configuration['base_uri']
                || $client->namespace !== $configuration['namespace']
                || $client->workerProtocolVersion !== $configuration['worker_protocol']) {
                throw new \UnexpectedValueException('Bootstrap changed the maintenance runtime identity.');
            }

            return $client;
        }
        $headers = $configuration['worker_headers'] ?? null;
        if (!is_array($headers)) {
            throw new \UnexpectedValueException('Missing original worker authentication.');
        }
        foreach ($headers as $key => $value) {
            if (!is_string($key) || !is_string($value)) {
                throw new \UnexpectedValueException('Malformed original worker authentication.');
            }
        }

        $requestTimeout = $configuration['request_timeout_seconds'] ?? null;
        if ($requestTimeout !== null && !is_int($requestTimeout) && !is_float($requestTimeout)) {
            throw new \UnexpectedValueException('Malformed original control request timeout.');
        }
        $workerRequestTimeout = $configuration['worker_request_timeout_seconds'] ?? null;
        if ($workerRequestTimeout !== null && !is_int($workerRequestTimeout) && !is_float($workerRequestTimeout)) {
            throw new \UnexpectedValueException('Malformed original worker request timeout.');
        }

        return new Client($configuration['base_uri'], new StaticHeadersAuthentication($headers),
            namespace: $configuration['namespace'], workerProtocolVersion: $configuration['worker_protocol'],
            requestTimeoutSeconds: $requestTimeout,
            workerRequestTimeoutSeconds: $workerRequestTimeout === null ? null : (float) $workerRequestTimeout);
    }

    private function loop(): bool
    {
        while (!feof(STDIN) && $this->parentPresent()) {
            $started = hrtime(true) / 1e9;
            $reply = $this->renewWhenDue($started);
            // Actual parent progress can overtake the in-flight runtime reply.
            // Drain those receipts before evaluating the old heartbeat ceiling.
            $commands = $this->readCommands();
            $this->applyProgressCommands($commands);
            $this->applyRenewal($reply, $started);
            foreach ($commands as $index => $command) {
                if (($command['operation'] ?? null) === 'finish') {
                    if ($index !== array_key_last($commands) || $this->buffer !== '') {
                        return false;
                    }
                    self::write(['id' => $command['id'] ?? 0, 'finished' => true]);
                    return true;
                }
                if (!in_array($command['operation'] ?? null, ['check', 'progress'], true)) {
                    $this->lostReason = 'activity_maintenance_invalid_command';
                }
                $this->respond($command);
            }
            $this->waitForCommand();
        }

        return false;
    }

    private function waitForCommand(): void
    {
        $due = ($this->start['registered'] ?? false) === true
            ? min($this->nextRenewal, $this->nextPresence) : $this->nextRenewal;
        $delay = $this->lostReason === null ? max(0, min(1, $due - hrtime(true) / 1e9)) : 1;
        $read = [STDIN];
        $write = $except = null;
        if (@stream_select($read, $write, $except, (int) $delay, (int) (($delay - (int) $delay) * 1e6)) === false) {
            usleep((int) min(10000, $delay * 1e6));
        }
    }

    /** @return array<string, mixed>|null */
    private function renewWhenDue(float $started): ?array
    {
        $due = ($this->start['registered'] ?? false) === true
            ? min($this->nextRenewal, $this->nextPresence) : $this->nextRenewal;
        if ($this->lostReason !== null || $started < $due) {
            return null;
        }
        try {
            return $this->requestRenewal();
        } catch (Throwable $error) {
            $this->recordMaintenanceError($error);

            return null;
        }
    }

    /** @param list<array<string, mixed>> $commands */
    private function applyProgressCommands(array $commands): void
    {
        foreach ($commands as $command) {
            if (($command['operation'] ?? null) !== 'progress' || $this->lostReason !== null) {
                continue;
            }
            try {
                $this->authority->acknowledgeProgress($command['reply'], $command['started'], $command['received']);
            } catch (Throwable $error) {
                $this->recordMaintenanceError($error);
            }
        }
    }

    /** @param array<string, mixed>|null $reply */
    private function applyRenewal(?array $reply, float $started): void
    {
        if ($reply === null || $this->lostReason !== null) {
            return;
        }
        try {
            $this->authority->acknowledge($reply, $started);
            $this->scheduleRenewal();
        } catch (Throwable $error) {
            $this->recordMaintenanceError($error);
        }
    }

    private function maintainInitialGrant(bool $proven): void
    {
        $age = hrtime(true) / 1e9 - (float) $this->start['captured_at'];
        if ($proven && $age >= 0 && $age < 1 && $this->authority->remaining() >= 3) {
            $this->scheduleRenewal();
            return;
        }
        $started = hrtime(true) / 1e9;
        $this->authority->acknowledge($this->requestRenewal(), $started);
        $this->scheduleRenewal();
    }

    /** @return array<string, mixed> */
    private function requestRenewal(): array
    {
        if (!$this->parentPresent()) {
            throw new WorkflowClaimRevoked('activity_parent_stopped', 'Original activity process has stopped.');
        }
        $identity = $this->authority->identity();
        if (($this->start['registered'] ?? false) === true && hrtime(true) / 1e9 >= $this->nextPresence) {
            $ack = $this->client->heartbeatWorker((string) $identity['lease_owner'],
                ['workflow_available' => 0, 'activity_available' => 0, 'session_available' => 0], $this->budget(),
                $this->start['parent_pid']);
            if (($ack['worker_id'] ?? null) !== $identity['lease_owner'] || ($ack['acknowledged'] ?? null) !== true) {
                throw new WorkflowClaimRevoked('activity_worker_unconfirmed', 'Original Worker presence was not acknowledged.');
            }
            $interval = $this->start['heartbeat_interval'] ?? 10;
            $this->nextPresence = hrtime(true) / 1e9 + max(1, min(10, is_int($interval) ? $interval : 10));
        }

        return $this->client->renewActivityTaskLease((string) $identity['task_id'],
            (string) $identity['activity_attempt_id'], (string) $identity['lease_owner'], $this->budget());
    }

    private function budget(): RequestBudget
    {
        return new RequestBudget(5, hrtime(true) / 1e9 + $this->authority->remaining());
    }

    private function scheduleRenewal(): void
    {
        $this->nextRenewal = hrtime(true) / 1e9 + min(10, $this->authority->remaining() / 3);
    }

    private function recordMaintenanceError(Throwable $error): void
    {
        if ($error instanceof ServerException && ($error->isTransientConnectionFailure()
            || $error->isTransientUpstreamFailure() || $error->isStorageAdmissionFailure())) {
            // Unavailability adds no time. Retry only within the preceding grant.
            $this->nextRenewal = hrtime(true) / 1e9 + 0.1;
            $this->nextPresence = max($this->nextPresence, $this->nextRenewal);

            return;
        }
        $this->lostReason = $error instanceof WorkflowClaimRevoked ? $error->reason : 'activity_maintenance_refused';
    }

    /** @return list<array<string, mixed>> */
    private function readCommands(): array
    {
        $part = stream_get_contents(STDIN, 65537 - strlen($this->buffer));
        if ($part === false || strlen($part) + strlen($this->buffer) > 65536) {
            throw new \UnexpectedValueException('Activity maintenance input exceeded its private bound.');
        }
        $this->buffer .= $part;
        $commands = [];
        while (str_contains($this->buffer, "\n")) {
            [$line, $this->buffer] = explode("\n", $this->buffer, 2);
            $command = json_decode($line, true, 32, JSON_THROW_ON_ERROR);
            if (!is_array($command)) {
                throw new \UnexpectedValueException('Invalid private activity command.');
            }
            $commands[] = $command;
        }

        return $commands;
    }

    /** @param array<string, mixed> $command */
    private function respond(array $command): void
    {
        try {
            if ($this->lostReason !== null) {
                throw new WorkflowClaimRevoked($this->lostReason, 'Original activity grant is unavailable.');
            }
            $now = hrtime(true) / 1e9;
            $remaining = $this->authority->remaining();
            self::write(['id' => $command['id'] ?? 0, 'can_continue' => true,
                'until' => $now + $remaining, 'monotonic' => $now]);
        } catch (Throwable $error) {
            $this->recordMaintenanceError($error);
            self::write(['id' => $command['id'] ?? 0, 'can_continue' => false, 'reason' => $this->lostReason]);
        }
    }

    private function parentPresent(): bool
    {
        return !function_exists('posix_getppid') || posix_getppid() === $this->start['parent_pid'];
    }

    /** @param array<string, mixed> $message */
    private static function write(array $message): void
    {
        $line = json_encode($message, JSON_THROW_ON_ERROR)."\n";
        if (fwrite(STDOUT, $line) !== strlen($line)) {
            throw new ActivityLeaseMaintenanceException('Original activity process is unavailable.');
        }
        fflush(STDOUT);
    }
}
