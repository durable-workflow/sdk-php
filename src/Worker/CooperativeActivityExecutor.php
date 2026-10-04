<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\InvalidLocalActivityReport;
use DurableWorkflow\Exception\LocalActivityTimedOut;
use DurableWorkflow\Exception\ServerException;
use InvalidArgumentException;
use Throwable;

/**
 * @internal Keep the owning worker available while a fork-safe activity blocks.
 *
 * The owning worker alone observes the Server, renews leases and permits result
 * encoding. A relay watches its socket and kills the callback if the owner dies.
 * Forked processes stop without running inherited PHP shutdown/destructor hooks.
 */
final class CooperativeActivityExecutor
{
    private const RELAY_BUFFER_BYTES = 65536;
    private const CHECK_INTERVAL_SECONDS = 0.1;
    private readonly int $maxFrameBytes;

    public function __construct(private readonly PayloadCodec $codec, int $maxPayloadBytes = 67108864)
    {
        if ($maxPayloadBytes < 1 || $maxPayloadBytes > intdiv(min(PHP_INT_MAX, 4294967295) - 65536, 2)) {
            throw new InvalidArgumentException('Activity IPC payload bound must be positive and finite.');
        }
        $this->maxFrameBytes = $maxPayloadBytes * 2 + 65536;
    }

    public static function available(): bool
    {
        return PHP_SAPI === 'cli'
            && function_exists('pcntl_fork') && function_exists('pcntl_waitpid')
            && function_exists('pcntl_signal') && function_exists('posix_kill')
            && function_exists('posix_setpgid') && function_exists('stream_socket_pair');
    }

    /**
     * The check must use bounded I/O and throw when the real task cannot continue.
     * Handler-owned connections must be opened in the callback process.
     *
     * @param Closure(Closure(array<array-key, mixed>): mixed): mixed $callback
     * @param Closure(array<array-key, mixed>): mixed $heartbeat
     * @param Closure(bool): void $check Force an actual lease check at publication boundaries.
     * @param (Closure(int, int): void)|null $started Receives relay and callback PIDs.
     * @param (Closure(): void)|null $stopped Called only after the callback is gone and the relay is joined.
     */
    public function execute(Closure $callback, Closure $heartbeat, Closure $check, ?Closure $started = null, ?Closure $stopped = null): mixed
    {
        if (!self::available()) {
            throw new InvalidArgumentException('Cooperative activity execution requires Unix CLI with pcntl and posix.');
        }
        $check(true);
        [$owner, $relay] = $this->socketPair();
        $relayPid = pcntl_fork();
        if ($relayPid === -1) {
            fclose($owner);
            fclose($relay);
            throw new WorkflowClaimAborted('Could not start the activity relay.');
        }
        if ($relayPid === 0) {
            fclose($owner);
            $this->relay($relay, $callback);
        }
        fclose($relay);
        stream_set_blocking($owner, false);
        $buffer = '';
        $callbackPid = null;
        $nextCheck = hrtime(true) / 1e9;
        try {
            while (true) {
                $now = hrtime(true) / 1e9;
                if ($now >= $nextCheck) {
                    $check(false);
                    $nextCheck = hrtime(true) / 1e9 + self::CHECK_INTERVAL_SECONDS;
                }
                $message = $this->takeFrame($buffer);
                if ($message === null) {
                    $read = [$owner];
                    $write = $except = [];
                    $ready = @stream_select($read, $write, $except, 0, 50000);
                    if ($ready === false) {
                        continue; // A managed signal may interrupt select.
                    }
                    if ($ready === 0) {
                        continue;
                    }
                    $chunk = fread($owner, 8192);
                    if ($chunk === false || ($chunk === '' && feof($owner))) {
                        throw new WorkflowClaimAborted('Activity IPC closed before a fenced result.');
                    }
                    $buffer .= $chunk;
                    if (strlen($buffer) > $this->maxFrameBytes + 4) {
                        throw new WorkflowClaimAborted('Activity IPC exceeded its finite frame bound.');
                    }
                    continue;
                }
                switch ($message['kind']) {
                    case 'started':
                        if (!is_int($message['callback_pid'] ?? null) || $message['callback_pid'] < 1) {
                            throw new WorkflowClaimAborted('Activity IPC returned an invalid callback PID.');
                        }
                        $callbackPid = $message['callback_pid'];
                        $started?->__invoke($relayPid, $callbackPid);
                        $check(true);
                        $this->writeFrame($owner, ['kind' => 'begin']);
                        break;
                    case 'heartbeat':
                        $details = $this->decode($message);
                        if (!is_array($details)) {
                            throw new WorkflowClaimAborted('Activity IPC heartbeat details are not an array.');
                        }
                        $check(true);
                        $reply = $heartbeat($details);
                        $check(true);
                        $this->writeFrame($owner, ['kind' => 'heartbeat_reply', 'value' => $this->codec->envelope($reply)]);
                        break;
                    case 'ready':
                        // No result is encoded in the callback before this real lease check.
                        $check(true);
                        $this->writeFrame($owner, ['kind' => 'encode']);
                        break;
                    case 'result':
                        $check(true);

                        return $this->decode($message);
                    case 'failure':
                        $check(true);
                        if (!is_string($message['message'] ?? null) || !is_string($message['type'] ?? null)
                            || !is_bool($message['encoding'] ?? null)
                            || !is_bool($message['cancelled'] ?? null)
                            || !is_bool($message['invalid_report'] ?? null)
                            || !is_bool($message['storage_admission_failure'] ?? null)
                            || !array_key_exists('timeout_kind', $message)
                            || ($message['timeout_kind'] !== null && !in_array($message['timeout_kind'],
                                ['heartbeat', 'start_to_close', 'schedule_to_close'], true))) {
                            throw new WorkflowClaimAborted('Activity IPC returned malformed failure metadata.');
                        }
                        throw new ActivityExecutionFailure($message['message'], $message['type'], $message['encoding'],
                            $message['cancelled'], $message['invalid_report'], $message['timeout_kind'],
                            $message['storage_admission_failure']);
                    default:
                        throw new WorkflowClaimAborted('Activity IPC returned an unexpected message.');
                }
            }
        } finally {
            // Closing this socket also works when the owner is killed with SIGKILL.
            fclose($owner);
            $joined = $this->reap($relayPid);
            if (!$joined) {
                throw new WorkflowClaimAborted('Activity relay stop could not be confirmed.');
            }
            if ($callbackPid !== null) {
                if (posix_kill($callbackPid, 0)) {
                    throw new WorkflowClaimAborted('Activity callback stop could not be confirmed.');
                }
                $stopped?->__invoke();
            }
        }
    }

    /**
     * All callbacks are admitted before this method. Only this owning worker
     * handles Server I/O. Settle each result after its physical process join so
     * completed work can become durable while other members are still running.
     * A scoped stop joins only that member. Return its observation after all
     * surviving members settle, preserving their hosting claim and results.
     *
     * @param list<array{
     *     callback: Closure, heartbeat: Closure, check: Closure,
     *     started: Closure, stopped: Closure
     * }> $operations
     * @param Closure(int, mixed, ?ActivityExecutionFailure): void $settled
     */
    public function executeConcurrent(array $operations, Closure $settled): void
    {
        if (!self::available() || $operations === [] || count($operations) > 100) {
            throw new InvalidArgumentException('Concurrent cooperative execution requires Unix CLI and 1 to 100 admitted callbacks.');
        }
        /** @var array<int, CooperativeActivityProcess> $processes */
        $processes = [];
        $failure = null;
        $scopedCancellation = null;
        $stopMember = function (int $index, ScopedActivityCancellationObserved $error) use (
            &$processes, &$scopedCancellation, $operations,
        ): void {
            if (isset($processes[$index])) {
                $this->joinProcess($processes[$index]);
                unset($processes[$index]);
            }
            // The stop receipt must follow the physical join, including a
            // callback fenced at its publication boundary or before its fork.
            $operations[$index]['stopped']();
            $scopedCancellation ??= $error;
        };
        try {
            foreach ($operations as $index => $operation) {
                try {
                    $operation['check'](true);
                } catch (ScopedActivityCancellationObserved $error) {
                    $stopMember($index, $error);
                    continue;
                }
                [$owner, $relay] = $this->socketPair();
                $relayPid = pcntl_fork();
                if ($relayPid === -1) {
                    fclose($owner);
                    fclose($relay);
                    throw new WorkflowClaimAborted('Could not start a group activity relay.');
                }
                if ($relayPid === 0) {
                    // Another relay must never keep a sibling's owner socket
                    // alive after the real owning worker receives SIGKILL.
                    foreach ($processes as $sibling) {
                        fclose($sibling->socket);
                    }
                    fclose($owner);
                    $this->relay($relay, $operation['callback']);
                }
                fclose($relay);
                stream_set_blocking($owner, false);
                $processes[$index] = new CooperativeActivityProcess($owner, $relayPid);
            }
            while ($processes !== []) {
                $read = [];
                foreach ($processes as $index => $process) {
                    if (hrtime(true) / 1e9 >= $process->nextCheck) {
                        try {
                            $operations[$index]['check'](false);
                        } catch (ScopedActivityCancellationObserved $error) {
                            $stopMember($index, $error);
                            continue;
                        }
                        $process->nextCheck = hrtime(true) / 1e9 + self::CHECK_INTERVAL_SECONDS;
                    }
                    $read[] = $process->socket;
                }
                if ($read === []) {
                    break;
                }
                $write = $except = [];
                $ready = @stream_select($read, $write, $except, 0, 50000);
                if ($ready === false) {
                    continue;
                }
                foreach ($processes as $index => $process) {
                    if (in_array($process->socket, $read, true)) {
                        $chunk = fread($process->socket, 8192);
                        if ($chunk === false || ($chunk === '' && feof($process->socket))) {
                            throw new WorkflowClaimAborted('Group activity IPC closed before a fenced result.');
                        }
                        $process->buffer .= $chunk;
                        if (strlen($process->buffer) > $this->maxFrameBytes + 4) {
                            throw new WorkflowClaimAborted('Group activity IPC exceeded its finite frame bound.');
                        }
                    }
                    try {
                        while (($message = $this->takeFrame($process->buffer)) !== null) {
                            $operation = $operations[$index];
                            $kind = $message['kind'];
                            if ($kind === 'started') {
                                if ($process->callbackPid !== null || !is_int($message['callback_pid'] ?? null) || $message['callback_pid'] < 1) {
                                    throw new WorkflowClaimAborted('Group activity IPC returned an invalid callback PID.');
                                }
                                $process->callbackPid = $message['callback_pid'];
                                $operation['started']($process->relayPid, $process->callbackPid);
                                $operation['check'](true);
                                $this->writeFrame($process->socket, ['kind' => 'begin']);
                                continue;
                            }
                            if ($process->callbackPid === null) {
                                throw new WorkflowClaimAborted('Group activity IPC returned data before its start handshake.');
                            }
                            $operation['check'](true);
                            if ($kind === 'heartbeat') {
                                $details = $this->decode($message);
                                if (!is_array($details)) {
                                    throw new WorkflowClaimAborted('Group activity IPC heartbeat details are not an array.');
                                }
                                $reply = $operation['heartbeat']($details);
                                $operation['check'](true);
                                $this->writeFrame($process->socket, ['kind' => 'heartbeat_reply', 'value' => $this->codec->envelope($reply)]);
                            } elseif ($kind === 'ready') {
                                $this->writeFrame($process->socket, ['kind' => 'encode']);
                            } elseif (in_array($kind, ['result', 'failure'], true)) {
                                $result = null;
                                $applicationFailure = null;
                                if ($kind === 'result') {
                                    $result = $this->decode($message);
                                } else {
                                    $applicationFailure = $this->failure($message);
                                }
                                $this->joinProcess($process);
                                unset($processes[$index]);
                                $operation['stopped']();
                                $settled($index, $result, $applicationFailure);
                                break;
                            } else {
                                throw new WorkflowClaimAborted('Group activity IPC returned an unexpected message.');
                            }
                        }
                    } catch (ScopedActivityCancellationObserved $error) {
                        $stopMember($index, $error);
                    }
                }
            }
            if ($scopedCancellation !== null) {
                throw $scopedCancellation;
            }
        } catch (Throwable $error) {
            $failure = $error;
            throw $error;
        } finally {
            // Close every owner socket before waiting on any relay. Each relay
            // then stops its own callback even when another member lost authority.
            foreach ($processes as $process) {
                if (is_resource($process->socket)) {
                    fclose($process->socket);
                }
            }
            $cleanupError = null;
            foreach ($processes as $process) {
                try { $this->joinProcess($process); } catch (Throwable $error) { $cleanupError ??= $error; }
            }
            if ($cleanupError !== null) {
                throw $cleanupError;
            }
            foreach ($processes as $index => $process) {
                if ($failure instanceof CooperativeCancellationObserved) {
                    // Every joined original attempt needs its own canonical
                    // cancellation observation before reporting physical stop.
                    try { $operations[$index]['check'](true); }
                    catch (CooperativeCancellationObserved) {}
                    catch (Throwable $error) { $cleanupError ??= $error; }
                }
                try { $operations[$index]['stopped'](); } catch (Throwable $error) { $cleanupError ??= $error; }
            }
            if ($cleanupError !== null) {
                throw $cleanupError;
            }
        }
    }

    private function joinProcess(CooperativeActivityProcess $process): void
    {
        if (is_resource($process->socket)) {
            fclose($process->socket);
        }
        if (!$this->reap($process->relayPid)
            || ($process->callbackPid !== null && posix_kill($process->callbackPid, 0))) {
            throw new WorkflowClaimAborted('Group activity callback stop could not be confirmed.');
        }
    }

    /** @param array<string, mixed> $message */
    private function failure(array $message): ActivityExecutionFailure
    {
        if (!is_string($message['message'] ?? null) || !is_string($message['type'] ?? null)
            || !is_bool($message['encoding'] ?? null) || !is_bool($message['cancelled'] ?? null)
            || !is_bool($message['invalid_report'] ?? null) || !is_bool($message['storage_admission_failure'] ?? null)
            || !array_key_exists('timeout_kind', $message)
            || ($message['timeout_kind'] !== null && !in_array($message['timeout_kind'],
                ['heartbeat', 'start_to_close', 'schedule_to_close'], true))) {
            throw new WorkflowClaimAborted('Group activity IPC returned malformed failure metadata.');
        }

        return new ActivityExecutionFailure($message['message'], $message['type'], $message['encoding'],
            $message['cancelled'], $message['invalid_report'], $message['timeout_kind'], $message['storage_admission_failure']);
    }

    /** @param resource $owner
     *  @param Closure(Closure(array<array-key, mixed>): mixed): mixed $callback
     */
    private function relay($owner, Closure $callback): never
    {
        $this->resetSignals();
        $callbackPid = null;
        $task = null;
        try {
            [$task, $child] = $this->socketPair();
            $callbackPid = pcntl_fork();
            if ($callbackPid === -1) {
                fclose($child);
                throw new WorkflowClaimAborted('Could not start the isolated activity callback.');
            }
            if ($callbackPid === 0) {
                fclose($owner);
                fclose($task);
                $this->callback($child, $callback);
            }
            fclose($child);
            if (!posix_setpgid($callbackPid, $callbackPid)) {
                throw new WorkflowClaimAborted('Could not isolate the activity process group.');
            }
            $this->writeFrame($owner, ['kind' => 'started', 'callback_pid' => $callbackPid]);
            stream_set_blocking($owner, false);
            stream_set_blocking($task, false);
            $toOwner = $toTask = '';
            $taskClosed = false;
            while (true) {
                if ($taskClosed && $toOwner === '') {
                    break;
                }
                $read = $write = $except = [];
                if (strlen($toTask) < self::RELAY_BUFFER_BYTES) {
                    $read[] = $owner;
                }
                if (!$taskClosed && strlen($toOwner) < self::RELAY_BUFFER_BYTES) {
                    $read[] = $task;
                }
                if ($toOwner !== '') {
                    $write[] = $owner;
                }
                if (!$taskClosed && $toTask !== '') {
                    $write[] = $task;
                }
                if (@stream_select($read, $write, $except, 0, 50000) === false) {
                    continue;
                }
                foreach ($read as $stream) {
                    $chunk = fread($stream, 8192);
                    if ($chunk === false || ($chunk === '' && feof($stream))) {
                        if ($stream === $owner) {
                            break 2; // The worker is gone. Never leave its callback running.
                        }
                        $taskClosed = true;
                        continue;
                    }
                    if ($stream === $owner) {
                        $toTask .= $chunk;
                    } else {
                        $toOwner .= $chunk;
                    }
                }
                foreach ($write as $stream) {
                    $pending = $stream === $owner ? $toOwner : $toTask;
                    $written = @fwrite($stream, $pending);
                    if ($written === false) {
                        break 2;
                    }
                    if ($stream === $owner) {
                        $toOwner = substr($toOwner, $written);
                    } else {
                        $toTask = substr($toTask, $written);
                    }
                }
            }
        } catch (Throwable) {
            // EOF is an unsafe claim, never an invented callback success.
        } finally {
            if (is_int($callbackPid) && $callbackPid > 0) {
                @posix_kill(-$callbackPid, SIGKILL);
                @posix_kill($callbackPid, SIGKILL);
                $this->reap($callbackPid);
            }
            if (is_resource($task)) {
                fclose($task);
            }
            fclose($owner);
        }
        $this->stopFork();
    }

    /** @param resource $socket
     *  @param Closure(Closure(array<array-key, mixed>): mixed): mixed $callback
     */
    private function callback($socket, Closure $callback): never
    {
        $this->resetSignals();
        $encoding = false;
        try {
            $this->requireMessage($socket, 'begin');
            $result = $callback(function (array $details) use ($socket): mixed {
                try {
                    $value = $this->codec->envelope($details);
                } catch (Throwable $error) {
                    throw new InvalidLocalActivityReport('Activity heartbeat details could not be encoded.', previous: $error);
                }
                $this->writeFrame($socket, ['kind' => 'heartbeat', 'value' => $value]);

                return $this->decode($this->requireMessage($socket, 'heartbeat_reply'));
            });
            $this->writeFrame($socket, ['kind' => 'ready']);
            $this->requireMessage($socket, 'encode');
            $encoding = true;
            $this->writeFrame($socket, ['kind' => 'result', 'value' => $this->codec->envelope($result)]);
        } catch (Throwable $error) {
            try {
                $message = $error->getMessage();
                $type = $error::class;
                $invalid = $error instanceof InvalidLocalActivityReport;
                try {
                    json_encode(['message' => $message, 'type' => $type], JSON_THROW_ON_ERROR);
                } catch (Throwable) {
                    $invalid = true;
                    $message = 'Activity failure metadata could not be encoded for the HTTP JSON wire boundary.';
                }
                if (strlen($type) > 255) {
                    $invalid = true;
                    $message = 'Activity failure metadata exceeded the published exception type limit.';
                }
                $this->writeFrame($socket, ['kind' => 'failure', 'message' => $message,
                    'type' => $invalid ? InvalidLocalActivityReport::class : $type, 'encoding' => $encoding,
                    'cancelled' => !$invalid && $error instanceof ActivityCancelled,
                    'invalid_report' => $invalid,
                    'timeout_kind' => !$invalid && $error instanceof LocalActivityTimedOut ? $error->timeoutKind : null,
                    'storage_admission_failure' => $error instanceof ServerException && $error->isStorageAdmissionFailure()]);
            } catch (Throwable) {
                // The owning worker has abandoned this claim or the frame is invalid.
            }
        } finally {
            fclose($socket);
        }
        $this->stopFork();
    }

    private function resetSignals(): void
    {
        foreach ([SIGINT, SIGTERM, SIGHUP, SIGPIPE, SIGCHLD] as $signal) {
            pcntl_signal($signal, $signal === SIGPIPE ? SIG_IGN : SIG_DFL);
        }
    }

    private function stopFork(): never
    {
        // exit() could run inherited application destructors and close its DB session.
        posix_kill(getmypid(), SIGKILL);
        throw new \LogicException('Could not stop the forked activity process.');
    }

    private function reap(int $pid): bool
    {
        for ($phase = 0; $phase < 2; ++$phase) {
            $deadline = hrtime(true) / 1e9 + 2;
            do {
                $result = pcntl_waitpid($pid, $status, WNOHANG);
                if ($result === $pid || ($result === -1 && !posix_kill($pid, 0))) {
                    return true;
                }
                if ($result === -1 && pcntl_get_last_error() !== PCNTL_EINTR) {
                    return false;
                }
                usleep(10000);
            } while (hrtime(true) / 1e9 < $deadline);
            @posix_kill($pid, SIGKILL);
        }

        return false;
    }

    /** @return array{resource, resource} */
    private function socketPair(): array
    {
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($sockets === false) {
            throw new WorkflowClaimAborted('Could not open the activity IPC socket.');
        }

        return $sockets;
    }

    /** @param resource $socket
     *  @param array<string, mixed> $message
     */
    private function writeFrame($socket, array $message): void
    {
        $body = json_encode($message, JSON_THROW_ON_ERROR);
        if (strlen($body) > $this->maxFrameBytes) {
            throw new WorkflowClaimAborted('Activity IPC exceeded its finite frame bound.');
        }
        $frame = pack('N', strlen($body)).$body;
        $offset = 0;
        while ($offset < strlen($frame)) {
            $written = @fwrite($socket, substr($frame, $offset));
            if ($written === false || ($written === 0 && feof($socket))) {
                throw new WorkflowClaimAborted('Activity IPC write failed.');
            }
            if ($written === 0) {
                $read = $except = [];
                $write = [$socket];
                @stream_select($read, $write, $except, 0, 50000);
            }
            $offset += $written;
        }
    }

    /** @param resource $socket
     *  @return array<string, mixed>
     */
    private function requireMessage($socket, string $kind): array
    {
        $buffer = '';
        while (($message = $this->takeFrame($buffer)) === null) {
            $chunk = fread($socket, 8192);
            if ($chunk === false || ($chunk === '' && feof($socket))) {
                throw new WorkflowClaimAborted('Activity IPC owner closed the attempt.');
            }
            $buffer .= $chunk;
        }
        if ($message['kind'] !== $kind || $buffer !== '') {
            throw new WorkflowClaimAborted('Activity IPC received an unexpected command.');
        }

        return $message;
    }

    /** @return array<string, mixed>|null */
    private function takeFrame(string &$buffer): ?array
    {
        if (strlen($buffer) < 4) {
            return null;
        }
        $prefix = unpack('Nlength', substr($buffer, 0, 4));
        $length = $prefix['length'] ?? 0;
        if (!is_int($length) || $length < 1 || $length > $this->maxFrameBytes) {
            throw new WorkflowClaimAborted('Activity IPC received an invalid frame length.');
        }
        if (strlen($buffer) < $length + 4) {
            return null;
        }
        try {
            $message = json_decode(substr($buffer, 4, $length), true, flags: JSON_THROW_ON_ERROR);
        } catch (Throwable $error) {
            throw new WorkflowClaimAborted('Activity IPC received invalid JSON.', previous: $error);
        }
        $buffer = substr($buffer, $length + 4);
        if (!is_array($message) || array_is_list($message) || !is_string($message['kind'] ?? null)) {
            throw new WorkflowClaimAborted('Activity IPC received a malformed message.');
        }

        return $message;
    }

    /** @param array<string, mixed> $message */
    private function decode(array $message): mixed
    {
        if (!is_array($message['value'] ?? null) || array_is_list($message['value'])) {
            throw new WorkflowClaimAborted('Activity IPC returned an invalid typed envelope.');
        }

        try {
            return $this->codec->decodeEnvelope($message['value']);
        } catch (Throwable $error) {
            throw new WorkflowClaimAborted('Activity IPC returned invalid typed bytes.', previous: $error);
        }
    }
}
