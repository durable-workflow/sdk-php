<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Composer\Autoload\ClassLoader;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\RequestBudget;
use Throwable;

/** @internal A fresh PHP process maintains leases while the original callback stays inline. */
final class ActivityLeaseProcess
{
    public static function available(): bool
    {
        return PHP_SAPI === 'cli' && function_exists('proc_open');
    }

    /** @var resource|null */
    private $process = null;
    /** @var array<int, resource> */
    private array $pipes = [];
    private string $buffer = '';
    private int $sequence = 0;
    private int $parentPid = 0;
    private float $lastTransferCheck = 0;
    private ?string $lostReason = null;
    private float $until;
    private readonly float $fixedUntil;
    /** @var array<string, string|int> */
    private readonly array $identity;

    /** @param array<string, mixed> $task */
    public function __construct(Client $client, array $task, string $owner, string $queue,
        mixed $serverTime, float $captured, bool $registered, int $heartbeatInterval)
    {
        $this->parentPid = getmypid();
        $authority = new ActivityLeaseAuthority($task, $client->namespace, $owner, $queue, $serverTime, $captured);
        $this->identity = $authority->identity();
        $this->until = hrtime(true) / 1e9 + $authority->remaining();
        $this->fixedUntil = $authority->fixedBound();
        if (!self::available()) {
            throw new \RuntimeException('Automatic activity lease maintenance requires PHP CLI proc_open.');
        }
        $configuration = $client->activityLeaseProcessConfiguration();
        $autoload = null;
        foreach (ClassLoader::getRegisteredLoaders() as $directory => $loader) {
            if (realpath((string) $loader->findFile(Client::class)) === realpath(__DIR__.'/../Client.php')) {
                $autoload = $directory.'/autoload.php';
                break;
            }
        }
        if ($autoload === null) {
            throw new \RuntimeException('Activity maintenance requires this SDK\'s Composer autoloader.');
        }
        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'],
            2 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'w']];
        $this->process = proc_open([PHP_BINARY, '-d', 'auto_prepend_file=', '-d', 'auto_append_file=',
            __DIR__.'/activity-lease-maintainer.php', $autoload],
            $descriptors, $this->pipes, options: ['bypass_shell' => true]);
        if (!is_resource($this->process)) {
            throw new \RuntimeException('Could not start independent activity lease maintenance.');
        }
        foreach ($this->pipes as $pipe) {
            stream_set_blocking($pipe, false);
        }
        try {
            $this->exchange('start', ['client' => $configuration, 'task' => array_intersect_key($task,
                array_flip(['task_id', 'workflow_id', 'run_id', 'activity_execution_id', 'activity_attempt_id',
                    'attempt_number', 'lease_owner', 'task_queue', 'namespace', 'lease_expires_at',
                    'deadlines', 'retry_policy', 'worker_session'])),
                'server_time' => $serverTime, 'captured_at' => $captured, 'owner' => $owner, 'queue' => $queue,
                'parent_pid' => getmypid(), 'registered' => $registered, 'heartbeat_interval' => $heartbeatInterval]);
        } catch (Throwable $error) {
            $this->stop();
            throw $error;
        }
    }

    public function governs(string $path): bool
    {
        return in_array($path, array_map(fn (string $action): string => '/worker/activity-tasks/'
            .rawurlencode((string) $this->identity['task_id']).'/'.$action,
            ['complete', 'fail', 'heartbeat', 'renew-lease']), true);
    }

    /** @param array<string, mixed>|null $body */
    public function checkRequest(string $path, ?array $body): void
    {
        if (($body['activity_attempt_id'] ?? null) !== $this->identity['activity_attempt_id']
            || ($body['lease_owner'] ?? null) !== $this->identity['lease_owner']) {
            throw new WorkflowClaimRevoked('activity_identity_mismatch', 'Activity publication changed its original claim.');
        }
        $this->check();
    }

    public function check(): void
    {
        $this->exchange('check');
    }

    public function budget(): RequestBudget
    {
        return new RequestBudget(30, is_finite($this->fixedUntil) ? $this->fixedUntil : null);
    }

    public function publicationDeadline(): float
    {
        return min($this->until, $this->fixedUntil);
    }

    public function checkTransfer(): void
    {
        $now = hrtime(true) / 1e9;
        if ($now >= $this->fixedUntil) {
            $this->refuse('activity_execution_budget_expired');
        }
        // Streaming checks bound staleness without an IPC round trip per chunk.
        // Callback dispatch and terminal POSTs always obtain fresh proof.
        if ($this->lostReason !== null || $now >= $this->until || $now - $this->lastTransferCheck >= 1) {
            $this->check();
            $this->lastTransferCheck = $now;
        }
    }

    /** @param array<string, mixed> $reply */
    public function progress(array $reply, float $started, float $received): void
    {
        $this->exchange('progress', ['reply' => $reply, 'started' => $started, 'received' => $received]);
    }

    public function stop(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];
        if ($this->parentPid !== 0 && $this->parentPid !== getmypid()) {
            // An explicitly isolated callback may inherit descriptors, but may
            // never terminate or converse with its parent's maintenance helper.
            $this->process = null;

            return;
        }
        if (is_resource($this->process)) {
            $until = hrtime(true) / 1e9 + 0.25;
            while ($this->running() && hrtime(true) / 1e9 < $until) {
                usleep(5000);
            }
            if ($this->running()) {
                // This stops maintenance only. It is never callback-stop proof.
                proc_terminate($this->process);
                $until = hrtime(true) / 1e9 + 0.25;
                while ($this->running() && hrtime(true) / 1e9 < $until) {
                    usleep(5000);
                }
                if ($this->running()) {
                    proc_terminate($this->process, 9);
                }
            }
            proc_close($this->process);
        }
        $this->process = null;
    }

    /** @phpstan-impure Process status changes independently of this PHP process. */
    private function running(): bool
    {
        return is_resource($this->process) && proc_get_status($this->process)['running'];
    }

    public function __destruct()
    {
        $this->stop();
    }

    /** @param array<string, mixed> $body */
    private function exchange(string $operation, array $body = []): void
    {
        if ($this->parentPid !== getmypid()) {
            throw new WorkflowClaimRevoked('activity_supervisor_process_mismatch',
                'Only the original Worker process can use its activity maintenance channel.');
        }
        if ($this->lostReason !== null) {
            $this->refuse($this->lostReason);
        }
        $started = hrtime(true) / 1e9;
        if ($started >= $this->fixedUntil || ($operation === 'start' && $started >= $this->until)) {
            $this->refuse('activity_execution_budget_expired');
        }
        // The helper can renew a runtime grant while the inline callback is
        // blocked. Obtain current proof; immutable business/TTL bounds remain.
        $deadline = min($this->fixedUntil, $started + 5, $operation === 'start' ? $this->until : INF);
        $id = ++$this->sequence;
        $line = json_encode(['id' => $id, 'operation' => $operation] + $body, JSON_THROW_ON_ERROR)."\n";
        if (strlen($line) > 65536) {
            $this->refuse('activity_maintenance_message_too_large');
        }
        try {
            $offset = 0;
            while ($offset < strlen($line)) {
                $written = fwrite($this->pipes[0], substr($line, $offset));
                if ($written === false || hrtime(true) / 1e9 >= $deadline) {
                    $this->refuse('activity_maintenance_unavailable');
                }
                $offset += $written;
                if ($written === 0) {
                    usleep(1000);
                }
            }
            while (!str_contains($this->buffer, "\n")) {
                $part = stream_get_contents($this->pipes[1], 65537 - strlen($this->buffer));
                if ($part === false || feof($this->pipes[1]) || strlen($this->buffer) + strlen($part) > 65536
                    || hrtime(true) / 1e9 >= $deadline) {
                    $this->refuse('activity_maintenance_unavailable');
                }
                $this->buffer .= $part;
                usleep(1000);
            }
            [$raw, $this->buffer] = explode("\n", $this->buffer, 2);
            $reply = json_decode($raw, true, 32, JSON_THROW_ON_ERROR);
            $now = hrtime(true) / 1e9;
            if (!is_array($reply) || ($reply['id'] ?? null) !== $id || ($reply['can_continue'] ?? null) !== true
                || !is_numeric($reply['until'] ?? null) || !is_numeric($reply['monotonic'] ?? null)
                || $reply['monotonic'] < $started || $reply['monotonic'] > $now
                || $reply['until'] <= $now || $now >= $deadline) {
                $reason = is_array($reply) && is_string($reply['reason'] ?? null)
                    ? $reply['reason'] : 'activity_maintenance_unconfirmed';
                $this->refuse($reason);
            }
            $this->until = (float) $reply['until'];
        } catch (WorkflowClaimRevoked $error) {
            throw $error;
        } catch (Throwable $error) {
            $this->lostReason = 'activity_maintenance_unavailable';
            throw new WorkflowClaimRevoked($this->lostReason, 'Independent activity maintenance became unavailable.', previous: $error);
        }
    }

    private function refuse(string $reason): never
    {
        $this->lostReason = $reason;
        throw new WorkflowClaimRevoked($reason, 'The original activity lease is unavailable.',
            taskId: (string) $this->identity['task_id']);
    }
}
