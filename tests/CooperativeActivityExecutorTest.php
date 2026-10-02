<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\ActivityExecutionFailure;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\CooperativeCancellationObserved;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

final class CooperativeActivityExecutorTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        if (!CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Real Unix CLI process control is required.');
        }
        $this->directory = sys_get_temp_dir().'/dw-activity-lifetime-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
    }

    protected function tearDown(): void
    {
        if (!isset($this->directory)) {
            return;
        }
        foreach (glob($this->directory.'/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->directory);
    }

    public function testTypedResultsAndHeartbeatReceiptsCrossTheProcessBoundary(): void
    {
        $ownerPid = getmypid();
        $checks = 0;
        $pids = [];
        $heartbeats = [];
        $stopped = false;
        $result = $this->executor()->execute(
            static function (Closure $heartbeat): array {
                $reply = $heartbeat(['bytes' => AvroBinaryValue::fromBytes("\xFF\x00"), 'progress' => 1]);

                return ['pid' => getmypid(), 'reply' => $reply, 'double' => 7.0, 'long' => 7];
            },
            static function (array $details) use (&$heartbeats, $ownerPid): array {
                self::assertSame($ownerPid, getmypid(), 'Only the owning worker handles Server heartbeats.');
                $heartbeats[] = $details;

                return ['can_continue' => true, 'original_request_id' => 'request-1'];
            },
            static function () use (&$checks, $ownerPid): void {
                self::assertSame($ownerPid, getmypid());
                ++$checks;
            },
            static function (int $relay, int $callback) use (&$pids): void {
                $pids = [$relay, $callback];
            },
            static function () use (&$pids, &$stopped): void {
                foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
                $stopped = true;
            },
        );
        self::assertTrue($stopped);
        self::assertIsArray($result);
        self::assertNotSame($ownerPid, $result['pid']);
        self::assertSame($pids[1], $result['pid']);
        self::assertSame(['can_continue' => true, 'original_request_id' => 'request-1'], $result['reply']);
        self::assertIsFloat($result['double']);
        self::assertSame(7.0, $result['double']);
        self::assertIsInt($result['long']);
        self::assertSame(7, $result['long']);
        self::assertCount(1, $heartbeats);
        self::assertInstanceOf(AvroBinaryValue::class, $heartbeats[0]['bytes']);
        self::assertSame("\xFF\x00", $heartbeats[0]['bytes']->bytes);
        self::assertGreaterThanOrEqual(5, $checks);
        $this->assertProcessStops($pids[0]);
        $this->assertProcessStops($pids[1]);
    }

    public function testCancellationStopsARealBlockingCallbackWithoutUserHeartbeats(): void
    {
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $pids = [];
        $stopped = false;
        $startedAt = microtime(true);
        try {
            $this->executor()->execute(
                static function (Closure $heartbeat) use ($entered, $late): string {
                    file_put_contents($entered, 'entered');
                    sleep(60);
                    file_put_contents($late, 'late');

                    return 'late';
                },
                static function (array $details): never {
                    throw new RuntimeException('The callback must not need a user heartbeat.');
                },
                static function () use ($entered): void {
                    if (is_file($entered)) {
                        throw new CooperativeCancellationObserved('original request observed');
                    }
                },
                static function (int $relay, int $callback) use (&$pids): void {
                    $pids = [$relay, $callback];
                },
                static function () use (&$pids, &$stopped): void {
                    foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
                    $stopped = true;
                },
            );
            self::fail('The blocked callback produced an accepted result.');
        } catch (CooperativeCancellationObserved $error) {
            self::assertSame('original request observed', $error->getMessage());
        }
        self::assertLessThan(2, microtime(true) - $startedAt);
        self::assertTrue($stopped);
        self::assertFileDoesNotExist($late);
        $this->assertProcessStops($pids[0]);
        $this->assertProcessStops($pids[1]);
        self::assertSame('next task', $this->executor()->execute(
            static fn (Closure $heartbeat): string => 'next task',
            static fn (array $details): mixed => null,
            static function (): void {},
        ));
    }

    public function testRefusalBeforeCallbackStartCannotReportAJoinedCallback(): void
    {
        $stopped = false;
        try {
            $this->executor()->execute(
                static fn (): string => 'unsafe',
                static fn (): mixed => null,
                static function (): never { throw new WorkflowClaimAborted('Already fenced.'); },
                stopped: static function () use (&$stopped): void { $stopped = true; },
            );
            self::fail('Expected refused callback.');
        } catch (WorkflowClaimAborted $error) { self::assertSame('Already fenced.', $error->getMessage()); }
        self::assertFalse($stopped);
    }

    public function testRelayDeathAloneCannotConfirmAStillRunningCallback(): void
    {
        $entered = $this->directory.'/entered';
        $pids = [];
        $stopped = false;
        try {
            $this->executor()->execute(
                static function () use ($entered): never {
                    file_put_contents($entered, 'entered');
                    sleep(60);
                    throw new RuntimeException('Test callback escaped its stop fixture.');
                },
                static fn (): mixed => null,
                static function () use ($entered, &$pids): void {
                    if (is_file($entered)) {
                        self::assertTrue(posix_kill($pids[0], SIGKILL));
                        throw new CooperativeCancellationObserved('Stop after relay death.');
                    }
                },
                static function (int $relay, int $callback) use (&$pids): void { $pids = [$relay, $callback]; },
                static function () use (&$stopped): void { $stopped = true; },
            );
            self::fail('A live callback was reported as stopped.');
        } catch (WorkflowClaimAborted $error) {
            self::assertSame('Activity callback stop could not be confirmed.', $error->getMessage());
            self::assertFalse($stopped);
            self::assertTrue(posix_kill($pids[1], 0));
        } finally {
            // This injected relay failure leaves the known fixture callback alive.
            if (isset($pids[1])) { posix_kill(-$pids[1], SIGKILL); }
        }
        $this->assertProcessStops($pids[0]);
        $this->assertProcessStops($pids[1]);
    }

    public function testReturnedUnencodableValueNeedsOwnerPermissionBeforeEncoding(): void
    {
        $returned = $this->directory.'/returned';
        try {
            $this->executor()->execute(
                static function (Closure $heartbeat) use ($returned): object {
                    file_put_contents($returned, 'returned');

                    return new \stdClass();
                },
                static fn (array $details): mixed => null,
                static function (bool $force) use ($returned): void {
                    if ($force && is_file($returned)) {
                        throw new CooperativeCancellationObserved('request wins before result encoding');
                    }
                },
            );
            self::fail('An unsafe result was accepted.');
        } catch (CooperativeCancellationObserved $error) {
            self::assertSame('request wins before result encoding', $error->getMessage());
        }
    }

    public function testCallbackFailureRetainsItsOriginalTypeAndMessage(): void
    {
        try {
            $this->executor()->execute(
                static function (Closure $heartbeat): never {
                    throw new RuntimeException('business failure');
                },
                static fn (array $details): mixed => null,
                static function (): void {},
            );
            self::fail('Callback failure was accepted as success.');
        } catch (ActivityExecutionFailure $error) {
            self::assertSame('business failure', $error->getMessage());
            self::assertSame(RuntimeException::class, $error->originalType);
            self::assertFalse($error->duringEncoding);
        }
    }

    public function testResultEncodingFailureIsDistinctFromCallbackFailure(): void
    {
        try {
            $this->executor()->execute(
                static fn (Closure $heartbeat): object => new \stdClass(),
                static fn (array $details): mixed => null,
                static function (): void {},
            );
            self::fail('An unsupported payload was accepted.');
        } catch (ActivityExecutionFailure $error) {
            self::assertTrue($error->duringEncoding);
            self::assertStringContainsString('stdClass', $error->getMessage());
        }
    }

    public function testLargeTypedResultCrossesPartialSocketWrites(): void
    {
        $value = str_repeat('abcd', 262144);
        self::assertSame($value, $this->executor()->execute(
            static fn (Closure $heartbeat): string => $value,
            static fn (array $details): mixed => null,
            static function (): void {},
        ));
    }

    public function testForksDoNotRunInheritedApplicationDestructors(): void
    {
        $marker = $this->directory.'/destructor';
        $guard = new ActivityParentResourceGuard($marker);
        self::assertSame('done', $this->executor()->execute(
            static function (Closure $heartbeat) use ($guard): string {
                return 'done';
            },
            static fn (array $details): mixed => null,
            static function (): void {},
        ));
        self::assertFileDoesNotExist($marker, 'Fork exit must not close inherited parent application resources.');
        unset($guard);
        self::assertFileExists($marker, 'The owning process still owns its normal destructor.');
    }

    public function testSigkillOfOwningWorkerStopsItsBlockedCallback(): void
    {
        $entered = $this->directory.'/entered';
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        self::assertNotFalse($sockets);
        [$parent, $child] = $sockets;
        $workerPid = pcntl_fork();
        self::assertGreaterThanOrEqual(0, $workerPid);
        if ($workerPid === 0) {
            fclose($parent);
            try {
                $this->executor()->execute(
                    static function (Closure $heartbeat) use ($entered): never {
                        file_put_contents($entered, 'entered');
                        while (true) {
                            sleep(60);
                        }
                    },
                    static fn (array $details): mixed => null,
                    static function (): void {},
                    static function (int $relay, int $callback) use ($child): void {
                        fwrite($child, json_encode([$relay, $callback], JSON_THROW_ON_ERROR)."\n");
                        fflush($child);
                    },
                );
            } catch (Throwable) {
                posix_kill(getmypid(), SIGKILL);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        fclose($child);
        $workerReaped = false;
        try {
            stream_set_timeout($parent, 3);
            $line = fgets($parent);
            self::assertIsString($line);
            $pids = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
            self::assertIsArray($pids);
            $deadline = microtime(true) + 3;
            while (!is_file($entered) && microtime(true) < $deadline) {
                usleep(10000);
            }
            self::assertFileExists($entered, 'Kill the owner while the callback is actually blocked.');
            self::assertTrue(posix_kill($workerPid, SIGKILL));
            self::assertSame($workerPid, pcntl_waitpid($workerPid, $status));
            $workerReaped = true;
            self::assertTrue(pcntl_wifsignaled($status));
            self::assertSame(SIGKILL, pcntl_wtermsig($status));
            $this->assertProcessStops($pids[1]);
            $this->assertProcessStops($pids[0]);
        } finally {
            fclose($parent);
            if (!$workerReaped) {
                @posix_kill($workerPid, SIGKILL);
                pcntl_waitpid($workerPid, $status);
            }
        }
    }

    private function executor(): CooperativeActivityExecutor
    {
        return new CooperativeActivityExecutor(new AvroPayloadCodec());
    }

    private function assertProcessStops(int $pid): void
    {
        $deadline = microtime(true) + 3;
        do {
            if (!posix_kill($pid, 0)) {
                self::assertFalse(posix_kill($pid, 0));

                return;
            }
            usleep(10000);
        } while (microtime(true) < $deadline);
        self::fail("Activity process {$pid} survived its owning task.");
    }
}

final class ActivityParentResourceGuard
{
    public function __construct(private readonly string $marker) {}

    public function __destruct()
    {
        file_put_contents($this->marker, 'destructor');
    }
}
