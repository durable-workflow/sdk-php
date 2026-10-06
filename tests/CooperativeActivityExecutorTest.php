<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\ActivityExecutionFailure;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\CooperativeCancellationObserved;
use DurableWorkflow\Worker\ScopedActivityCancellationObserved;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use PHPUnit\Framework\Attributes\DataProvider;
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

    public function test_concurrent_callbacks_overlap_and_settle_after_their_own_process_join(): void
    {
        $entered = $this->directory.'/second-entered';
        $committed = $this->directory.'/first-committed';
        $owner = getmypid();
        $pids = $stopped = $results = [];
        $operations = [];
        foreach ([0, 1] as $index) {
            $operations[] = [
                'callback' => static function (Closure $heartbeat) use ($index, $entered, $committed): string {
                    if ($index === 1) { file_put_contents($entered, 'entered'); }
                    $wait = $index === 0 ? $entered : $committed;
                    $deadline = hrtime(true) / 1e9 + 3;
                    while (!is_file($wait) && hrtime(true) / 1e9 < $deadline) { usleep(10000); }
                    if (!is_file($wait)) { throw new RuntimeException('Group callback was serialized or its result was not settled.'); }
                    return 'member-'.$index;
                },
                'heartbeat' => static function (): never { self::fail('Application heartbeat was not requested.'); },
                'check' => static function () use ($owner): void { self::assertSame($owner, getmypid()); },
                'started' => static function (int $relay, int $callback) use ($index, &$pids): void { $pids[$index] = [$relay, $callback]; },
                'stopped' => static function () use ($index, &$pids, &$stopped): void {
                    foreach ($pids[$index] as $pid) { self::assertFalse(posix_kill($pid, 0)); }
                    $stopped[$index] = true;
                },
            ];
        }
        $this->executor()->executeConcurrent($operations,
            static function (int $index, mixed $value, ?ActivityExecutionFailure $failure) use (&$results, &$stopped, $committed): void {
                self::assertNull($failure);
                self::assertTrue($stopped[$index]);
                $results[$index] = $value;
                if ($index === 0) { file_put_contents($committed, 'canonical outcome committed'); }
            });
        self::assertSame([0 => 'member-0', 1 => 'member-1'], $results);
        self::assertCount(2, $stopped);
    }

    public function test_concurrent_blocked_callbacks_all_join_before_cancellation_returns_without_application_heartbeats(): void
    {
        $pids = $stopped = $observed = [];
        $operations = [];
        foreach ([0, 1] as $index) {
            $entered = $this->directory.'/entered-'.$index;
            $late = $this->directory.'/late-'.$index;
            $first = $this->directory.'/entered-0';
            $second = $this->directory.'/entered-1';
            $operations[] = [
                'callback' => static function () use ($entered, $late): string {
                    file_put_contents($entered, (string) getmypid());
                    sleep(60);
                    file_put_contents($late, 'unsafe');
                    return 'unsafe';
                },
                'heartbeat' => static function (): never { self::fail('Cancellation must not require an application heartbeat.'); },
                'check' => static function () use ($index, $first, $second, &$observed): void {
                    if (is_file($first) && is_file($second)) {
                        $observed[$index] = true;
                        throw new CooperativeCancellationObserved('one root request');
                    }
                },
                'started' => static function (int $relay, int $callback) use ($index, &$pids): void { $pids[$index] = [$relay, $callback]; },
                'stopped' => static function () use ($index, &$pids, &$observed, &$stopped): void {
                    self::assertTrue($observed[$index]);
                    foreach ($pids[$index] as $pid) { self::assertFalse(posix_kill($pid, 0)); }
                    $stopped[$index] = true;
                },
            ];
        }
        $start = hrtime(true) / 1e9;
        try {
            $this->executor()->executeConcurrent($operations, static function (): never { self::fail('Cancelled callback published a result.'); });
            self::fail('Expected group cancellation.');
        } catch (CooperativeCancellationObserved) {
            self::assertLessThan(3, hrtime(true) / 1e9 - $start);
            self::assertCount(2, $observed);
            self::assertCount(2, $stopped);
            self::assertFileDoesNotExist($this->directory.'/late-0');
            self::assertFileDoesNotExist($this->directory.'/late-1');
        }
    }

    public function test_sigkill_of_the_group_owner_stops_every_blocked_callback(): void
    {
        $directory = $this->directory;
        $owner = pcntl_fork();
        self::assertNotSame(-1, $owner);
        if ($owner === 0) {
            $operations = [];
            foreach ([0, 1] as $index) {
                $operations[] = [
                    'callback' => static function () use ($directory, $index): never {
                        file_put_contents($directory.'/running-'.$index, (string) getmypid());
                        sleep(60);
                        file_put_contents($directory.'/late-'.$index, 'unsafe');
                        throw new RuntimeException('A dead owner left its callback running.');
                    },
                    'heartbeat' => static fn (): array => [], 'check' => static function (): void {},
                    'started' => static function (int $relay, int $callback) use ($directory, $index): void {
                        file_put_contents($directory.'/pids-'.$index, json_encode([$relay, $callback], JSON_THROW_ON_ERROR));
                    },
                    'stopped' => static function (): void {},
                ];
            }
            $this->executor()->executeConcurrent($operations, static function (): never { throw new RuntimeException('Unexpected result.'); });
            posix_kill(getmypid(), SIGKILL);
        }
        try {
            $deadline = hrtime(true) / 1e9 + 3;
            while ((!is_file($directory.'/running-0') || !is_file($directory.'/running-1')) && hrtime(true) / 1e9 < $deadline) { usleep(10000); }
            self::assertFileExists($directory.'/running-0');
            self::assertFileExists($directory.'/running-1');
            self::assertTrue(posix_kill($owner, SIGKILL));
            pcntl_waitpid($owner, $status);
            foreach ([0, 1] as $index) {
                foreach (json_decode((string) file_get_contents($directory.'/pids-'.$index), true, flags: JSON_THROW_ON_ERROR) as $pid) {
                    $this->assertProcessStops($pid);
                }
                self::assertFileDoesNotExist($directory.'/late-'.$index);
            }
        } finally {
            @posix_kill($owner, SIGKILL);
            pcntl_waitpid($owner, $status, WNOHANG);
        }
    }

    #[DataProvider('scopedStopBoundaries')]
    public function test_scoped_stop_preserves_the_live_sibling_and_its_result(string $boundary): void
    {
        $pids = $stopped = $results = [];
        $request = $this->scopeRequest();
        $operations = $this->selectiveOperations($boundary, $request, $pids, $stopped);
        $started = hrtime(true) / 1e9;
        try {
            $this->executor()->executeConcurrent($operations,
                static function (int $index, mixed $value, ?ActivityExecutionFailure $failure) use (&$results, &$stopped): void {
                    self::assertSame(1, $index, 'The cancelled attempt published an outcome.');
                    self::assertNull($failure);
                    self::assertTrue($stopped[0]);
                    self::assertTrue($stopped[1]);
                    $results[$index] = $value;
                });
            self::fail('Expected the original scoped observation after sibling settlement.');
        } catch (ScopedActivityCancellationObserved $error) {
            self::assertSame($request, $error->request);
            self::assertSame('inner-request', $error->request->requestId);
            self::assertSame('2026-10-04T00:00:20.123456Z', $error->request->deadline()->format('Y-m-d\TH:i:s.u\Z'));
            self::assertSame([1 => 'surviving-result'], $results);
            self::assertCount(2, $stopped);
            self::assertFileExists($this->directory.'/sibling-entered');
            if (in_array($boundary, ['before_fork', 'before_begin'], true)) {
                self::assertFileDoesNotExist($this->directory.'/target-entered');
            }
            self::assertLessThan(3, hrtime(true) / 1e9 - $started);
        }
    }

    public static function scopedStopBoundaries(): iterable
    {
        foreach (['before_fork', 'before_begin', 'running', 'publication'] as $boundary) {
            yield $boundary => [$boundary];
        }
    }

    public function test_failed_scoped_stop_receipt_joins_the_remaining_sibling_without_publication(): void
    {
        $pids = $stopped = [];
        $operations = $this->selectiveOperations('running', $this->scopeRequest(), $pids, $stopped);
        $targetStopped = $operations[0]['stopped'];
        $operations[0]['stopped'] = static function () use ($targetStopped): void {
            $targetStopped();
            throw new WorkflowClaimAborted('Stop receipt could not be recorded.');
        };
        try {
            $this->executor()->executeConcurrent($operations, static function (): never {
                self::fail('Unknown scoped stop authority allowed a sibling publication.');
            });
            self::fail('Expected stop receipt failure.');
        } catch (WorkflowClaimAborted $error) {
            self::assertSame('Stop receipt could not be recorded.', $error->getMessage());
            foreach ($pids as $pair) {
                foreach ($pair as $pid) { self::assertFalse(posix_kill($pid, 0)); }
            }
        }
    }

    public function test_scoped_stops_can_join_every_running_member_without_a_survivor(): void
    {
        $pids = $stopped = [];
        $request = $this->scopeRequest();
        $directory = $this->directory;
        $operations = $this->selectiveOperations('running', $request, $pids, $stopped);
        $operations[0]['stopped'] = static function () use (&$pids, &$stopped): void {
            foreach ($pids[0] as $pid) { self::assertFalse(posix_kill($pid, 0)); }
            $stopped[0] = true;
        };
        $operations[1]['callback'] = static function () use ($directory): never {
            file_put_contents($directory.'/sibling-entered', (string) getmypid());
            sleep(60);
            throw new RuntimeException('Cancelled sibling escaped its callback fence.');
        };
        $operations[1]['check'] = static function () use ($directory, $request): void {
            if (is_file($directory.'/target-entered') && is_file($directory.'/sibling-entered')) {
                throw new ScopedActivityCancellationObserved($request);
            }
        };
        try {
            $this->executor()->executeConcurrent($operations, static function (): never { self::fail('A scoped member published after its fence.'); });
            self::fail('Expected scoped cancellation.');
        } catch (ScopedActivityCancellationObserved $error) {
            self::assertSame($request, $error->request);
            self::assertCount(2, $stopped);
            foreach ($pids as $pair) {
                foreach ($pair as $pid) { self::assertFalse(posix_kill($pid, 0)); }
            }
        }
    }

    public function test_sigkill_after_partial_scope_stop_stops_the_survivor_without_inventing_a_stop_receipt(): void
    {
        $directory = $this->directory;
        $owner = pcntl_fork();
        self::assertNotSame(-1, $owner);
        if ($owner === 0) {
            $pids = $stopped = [];
            $operations = $this->selectiveOperations('running', $this->scopeRequest(), $pids, $stopped);
            $operations[1]['callback'] = static function () use ($directory): never {
                file_put_contents($directory.'/sibling-entered', (string) getmypid());
                sleep(60);
                file_put_contents($directory.'/late-sibling', 'unsafe');
                throw new RuntimeException('Owner loss left a scoped survivor running.');
            };
            foreach ([0, 1] as $index) {
                $started = $operations[$index]['started'];
                $operations[$index]['started'] = static function (int $relay, int $callback) use ($index, $directory, $started): void {
                    $started($relay, $callback);
                    file_put_contents($directory.'/partial-pids-'.$index, json_encode([$relay, $callback], JSON_THROW_ON_ERROR));
                };
            }
            $survivorStopped = $operations[1]['stopped'];
            $operations[1]['stopped'] = static function () use ($directory, $survivorStopped): void {
                $survivorStopped();
                file_put_contents($directory.'/survivor-stop-receipt', 'owner joined survivor');
            };
            $this->executor()->executeConcurrent($operations, static function (): never { throw new RuntimeException('Killed owner published a result.'); });
            posix_kill(getmypid(), SIGKILL);
        }
        try {
            $deadline = hrtime(true) / 1e9 + 3;
            while (!is_file($directory.'/target-stopped') && hrtime(true) / 1e9 < $deadline) { usleep(10000); }
            self::assertFileExists($directory.'/target-stopped');
            $survivor = json_decode((string) file_get_contents($directory.'/partial-pids-1'), true, flags: JSON_THROW_ON_ERROR);
            self::assertTrue(posix_kill($survivor[1], 0));
            self::assertTrue(posix_kill($owner, SIGKILL));
            pcntl_waitpid($owner, $status);
            self::assertSame(SIGKILL, pcntl_wtermsig($status));
            foreach ([0, 1] as $index) {
                foreach (json_decode((string) file_get_contents($directory.'/partial-pids-'.$index), true, flags: JSON_THROW_ON_ERROR) as $pid) {
                    $this->assertProcessStops($pid);
                }
            }
            self::assertFileDoesNotExist($directory.'/late-sibling');
            self::assertFileDoesNotExist($directory.'/survivor-stop-receipt');
        } finally {
            @posix_kill($owner, SIGKILL);
            pcntl_waitpid($owner, $status, WNOHANG);
        }
    }

    public function test_run_cancellation_stops_a_survivor_after_a_partial_scoped_stop(): void
    {
        $pids = $stopped = [];
        $operations = $this->selectiveOperations('running', $this->scopeRequest(), $pids, $stopped);
        $acknowledged = $this->directory.'/target-stopped';
        $operations[1]['check'] = static function () use ($acknowledged): void {
            if (is_file($acknowledged)) { throw new CooperativeCancellationObserved('enclosing run request'); }
        };
        try {
            $this->executor()->executeConcurrent($operations, static function (): never {
                self::fail('Run cancellation allowed a sibling publication.');
            });
            self::fail('Expected run cancellation to supersede the partial wait.');
        } catch (CooperativeCancellationObserved $error) {
            self::assertSame('enclosing run request', $error->getMessage());
            self::assertCount(2, $stopped);
            foreach ($pids as $pair) {
                foreach ($pair as $pid) { self::assertFalse(posix_kill($pid, 0)); }
            }
        }
    }

    private function scopeRequest(): ScopedCancellationContext
    {
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/scoped-run-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
        return ScopedCancellationContext::fromArray($fixture['child']['scope_origin']);
    }

    /** @param array<int, array{int, int}> $pids
     * @param array<int, bool> $stopped
     * @return list<array{callback: Closure, heartbeat: Closure, check: Closure, started: Closure, stopped: Closure}>
     */
    private function selectiveOperations(string $boundary, ScopedCancellationContext $request, array &$pids, array &$stopped): array
    {
        $directory = $this->directory;
        $owner = getmypid();
        $operations = [];
        foreach ([0, 1] as $index) {
            $operations[] = [
                'callback' => static function () use ($directory, $index, $boundary): string {
                    file_put_contents($directory.($index === 0 ? '/target-entered' : '/sibling-entered'), (string) getmypid());
                    if ($index === 0 && $boundary === 'running') { sleep(60); return 'unsafe'; }
                    $wait = $directory.($index === 0 ? '/sibling-entered' : '/target-stopped');
                    $deadline = hrtime(true) / 1e9 + 3;
                    while (!is_file($wait) && hrtime(true) / 1e9 < $deadline) { usleep(10000); }
                    if (!is_file($wait)) { throw new RuntimeException('Surviving callback lost concurrent supervision or the target was never stopped.'); }
                    return $index === 0 ? 'unsafe' : 'surviving-result';
                },
                'heartbeat' => static function (): never { self::fail('Scoped stopping must not require application heartbeats.'); },
                'check' => static function (bool $force) use ($index, $directory, $boundary, $request, $owner, &$pids): void {
                    self::assertSame($owner, getmypid());
                    if ($index !== 0) { return; }
                    if ($boundary === 'before_fork'
                        || ($boundary === 'before_begin' && isset($pids[0]))
                        || ($boundary === 'running' && !$force && is_file($directory.'/target-entered') && is_file($directory.'/sibling-entered'))
                        || ($boundary === 'publication' && $force && is_file($directory.'/target-entered') && is_file($directory.'/sibling-entered'))) {
                        throw new ScopedActivityCancellationObserved($request);
                    }
                },
                'started' => static function (int $relay, int $callback) use ($index, &$pids): void { $pids[$index] = [$relay, $callback]; },
                'stopped' => static function () use ($index, $directory, $boundary, &$pids, &$stopped): void {
                    foreach ($pids[$index] ?? [] as $pid) { self::assertFalse(posix_kill($pid, 0)); }
                    if ($index === 0) {
                        if (in_array($boundary, ['running', 'publication'], true)) {
                            self::assertTrue(posix_kill($pids[1][1], 0), 'Partial cancellation killed the unrelated callback.');
                        }
                        file_put_contents($directory.'/target-stopped', 'original owner joined target');
                    }
                    $stopped[$index] = true;
                },
            ];
        }
        return $operations;
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
