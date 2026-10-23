<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerClaimedTaskTransportRecoveryTest extends TestCase
{
    #[DataProvider('transientFailures')]
    public function testSameManagedWorkerRecoversOriginalClaimAndProcessesLaterWork(
        string $operation, int $status, bool $committedFirst,
    ): void {
        $this->withServer($operation, $status, $committedFirst, '<html>Upstream unavailable</html>',
            static function (string $url, string $directory) use ($operation): void {
                $calls = 0;
                $events = [];
                $now = 0.0;
                $worker = new Worker(new Client($url), 'orders', workerId: 'worker-1',
                    clock: static function () use (&$now): float { return $now; },
                    sleeper: static function (int $us) use (&$now): void { $now += $us / 1_000_000; },
                    diagnosticListener: static function (string $event) use (&$events): void { $events[] = $event; });
                $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context) use (&$calls): string {
                    ++$calls;
                    return 'workflow result';
                });
                $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): string {
                    ++$calls;
                    return 'activity result';
                });

                $worker->run(0);

                $state = json_decode(file_get_contents($directory.'/state.json'), true, flags: JSON_THROW_ON_ERROR);
                $kind = $operation === 'activity_complete' ? 'activity' : 'workflow';
                $target = '/api/worker/'.$kind.'-tasks/'.$kind.'-1/'
                    .($operation === 'workflow_heartbeat' ? 'heartbeat' : 'complete');
                $requests = array_values(array_filter($state['requests'], static fn (array $r): bool => $r['path'] === $target));
                self::assertCount(2, $requests);
                self::assertSame($requests[0], $requests[1], 'Reuse the original fence and saved command/result bytes.');
                self::assertSame('worker-1', $requests[0]['body']['lease_owner']);
                self::assertSame($kind === 'workflow' ? 3 : 'attempt-1',
                    $requests[0]['body'][$kind === 'workflow' ? 'workflow_task_attempt' : 'activity_attempt_id']);
                self::assertSame([$kind.'-1', $kind.'-2'], array_keys($state['outcomes']));
                self::assertSame(2, $calls, 'One handler invocation per task, including later work.');
                self::assertContains('worker.retrying', $events);
                self::assertContains('worker.stopped', $events);
                self::assertNotContains('worker.failed', $events);
                self::assertEqualsWithDelta(0.1, $now, 0.00001);
                self::assertSame([], array_filter($state['requests'], static fn (array $r): bool => str_ends_with($r['path'], '/fail')));
            });
    }

    public static function transientFailures(): iterable
    {
        foreach (['workflow_heartbeat', 'workflow_complete', 'activity_complete'] as $operation) {
            foreach ([502, 503, 504, 0] as $status) {
                foreach ($operation === 'workflow_heartbeat' ? [false] : [false, true] as $committedFirst) {
                    yield $operation.' '.$status.' committed='.(int) $committedFirst => [$operation, $status, $committedFirst];
                }
            }
        }
    }

    #[DataProvider('terminalFailures')]
    public function testTerminalFailureIsNotRetriedOrReportedAsAHandlerFailure(
        string $operation, int $status, string $body,
    ): void {
        $this->withServer($operation, $status, false, $body,
            static function (string $url, string $directory) use ($operation): void {
                $calls = 0;
                $worker = new Worker(new Client($url), 'orders', workerId: 'worker-1');
                $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context) use (&$calls): string {
                    ++$calls;
                    return 'result';
                });
                $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): string {
                    ++$calls;
                    return 'result';
                });
                try {
                    $worker->tick(0);
                    self::fail('A terminal response must remain visible to the caller.');
                } catch (ServerException $error) {
                    self::assertFalse($error->isTransientConnectionFailure());
                    self::assertFalse($error->isTransientUpstreamFailure());
                }
                $state = json_decode(file_get_contents($directory.'/state.json'), true, flags: JSON_THROW_ON_ERROR);
                $kind = $operation === 'activity_complete' ? 'activity' : 'workflow';
                $target = '/api/worker/'.$kind.'-tasks/'.$kind.'-1/'
                    .($operation === 'workflow_heartbeat' ? 'heartbeat' : 'complete');
                self::assertCount(1, array_filter($state['requests'], static fn (array $r): bool => $r['path'] === $target));
                $expectedCalls = $operation === 'workflow_heartbeat' ? 0 : 1;
                self::assertSame($expectedCalls, $calls);
                self::assertSame([], $state['outcomes']);
                self::assertSame([], array_filter($state['requests'], static fn (array $r): bool => str_ends_with($r['path'], '/fail')));
            });
    }

    public static function terminalFailures(): iterable
    {
        foreach (['workflow_heartbeat', 'workflow_complete', 'activity_complete'] as $operation) {
            foreach ([[401, '<html>Unauthorized</html>'], [403, '<html>Forbidden</html>'],
                [200, '<html>Malformed success</html>'],
                [503, '{"reason":"service_unavailable","retryable":false}']] as [$status, $body]) {
                yield $operation.' '.$status => [$operation, $status, $body];
            }
        }
    }

    private function withServer(string $operation, int $status, bool $committedFirst, string $body, \Closure $test): void
    {
        if (!extension_loaded('curl') || !function_exists('proc_open')) {
            self::markTestSkipped('Real cURL and proc_open are required.');
        }
        $directory = sys_get_temp_dir().'/dw-claimed-task-'.bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        $process = proc_open([PHP_BINARY, __DIR__.'/Support/claimed-task-server.php', $directory,
            $operation, (string) $status, $committedFirst ? '1' : '0', $body],
            [0 => ['pipe', 'r'], 1 => ['file', $directory.'/stdout', 'w'], 2 => ['file', $directory.'/stderr', 'w']], $pipes);
        self::assertIsResource($process);
        fclose($pipes[0]);
        try {
            $deadline = microtime(true) + 5;
            while (!is_file($directory.'/address') && microtime(true) < $deadline && proc_get_status($process)['running']) {
                usleep(10000);
            }
            self::assertFileExists($directory.'/address', (string) file_get_contents($directory.'/stderr'));
            $test('http://'.trim(file_get_contents($directory.'/address')), $directory);
        } finally {
            proc_terminate($process);
            proc_close($process);
            foreach (glob($directory.'/*') as $file) {
                unlink($file);
            }
            rmdir($directory);
        }
    }
}
