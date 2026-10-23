<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ControlRequestTimeoutTest extends TestCase
{
    #[DataProvider('stalledRequests')]
    public function testRealControlDeadlineIncludesHeadersAndBodyWithoutRepeatingTheMutation(string $mode, string $operation, bool $native): void
    {
        $this->withPeer($mode, static function (string $url, string $directory) use ($operation, $native): void {
            $command = [PHP_BINARY];
            if ($native) {
                array_push($command, '-d', 'disable_functions=curl_exec,curl_multi_exec');
            }
            array_push($command, __DIR__.'/Support/control-timeout-client.php', dirname(__DIR__).'/vendor/autoload.php', $url, $operation, '0.2');
            $process = proc_open($command, [0 => ['file', '/dev/null', 'r'],
                1 => ['file', $directory.'/stdout', 'w'], 2 => ['file', $directory.'/stderr', 'w']], $pipes);
            self::assertIsResource($process);
            try {
                $until = microtime(true) + 2;
                while (proc_get_status($process)['running'] && microtime(true) < $until) {
                    usleep(10000);
                }
                self::assertFalse(proc_get_status($process)['running'], 'The configured total deadline was not enforced.');
                $result = json_decode(file_get_contents($directory.'/stdout'), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame('error', $result['outcome']);
                self::assertSame(0, $result['status']);
                self::assertSame('request_timeout', $result['reason']);
                self::assertGreaterThanOrEqual(0.15, $result['elapsed']);
                self::assertLessThan(1.0, $result['elapsed']);
                self::assertSame('', file_get_contents($directory.'/stderr'));
                $requests = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
                    file($directory.'/requests', FILE_IGNORE_NEW_LINES));
                $actual = array_values(array_filter($requests, static fn (array $r): bool => !str_starts_with($r['path'], '/api/cluster/info')));
                self::assertCount(1, $actual, 'A timeout cannot authorize resubmission of an ambiguous mutation.');
            } finally {
                proc_terminate($process, 9);
                proc_close($process);
            }
        });
    }

    public static function stalledRequests(): iterable
    {
        foreach ([false, true] as $native) {
            foreach (['stall', 'slow_headers', 'slow_body'] as $mode) {
                foreach (['list', 'start', 'signal', 'update'] as $operation) {
                    yield $mode.' '.$operation.' native='.(int) $native => [$mode, $operation, $native];
                }
            }
        }
    }

    #[DataProvider('invalidTimeouts')]
    public function testInvalidTimeoutCannotDisableTheControlBudget(float $seconds): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Client('https://server.example', requestTimeoutSeconds: $seconds);
    }

    public static function invalidTimeouts(): iterable
    {
        foreach ([0.0, -1.0, 0.0001, INF, NAN, (float) PHP_INT_MAX] as $seconds) {
            yield [$seconds];
        }
    }

    public function testInjectedTransportCannotBeSilentlyRebuiltToApplyAnSdkTimeout(): void
    {
        $transport = new FakeTransport();
        $this->expectException(\InvalidArgumentException::class);
        try {
            new Client('https://server.example', transport: $transport, requestTimeoutSeconds: 1);
        } finally {
            self::assertSame([], $transport->requests);
        }
    }

    public function testDefaultAndConfiguredBudgetsSurviveNamespaceAndMaintenanceConfiguration(): void
    {
        $default = new Client('https://server.example');
        self::assertSame(60.0, $default->activityLeaseProcessConfiguration()['request_timeout_seconds']);
        $configured = (new Client('https://server.example', requestTimeoutSeconds: 0.25))->withNamespace('billing');
        self::assertSame(0.25, $configured->activityLeaseProcessConfiguration()['request_timeout_seconds']);
    }

    private function withPeer(string $mode, \Closure $test): void
    {
        if (!function_exists('proc_open')) {
            self::markTestSkipped('The real deadline fixture requires proc_open.');
        }
        $directory = sys_get_temp_dir().'/dw-control-timeout-'.bin2hex(random_bytes(8));
        mkdir($directory, 0700);
        $process = proc_open([PHP_BINARY, __DIR__.'/Support/control-timeout-peer.php', $directory, $mode],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
        self::assertIsResource($process);
        try {
            $until = microtime(true) + 3;
            while (!is_file($directory.'/address') && microtime(true) < $until) {
                usleep(10000);
            }
            self::assertFileExists($directory.'/address');
            $test('http://'.trim(file_get_contents($directory.'/address')), $directory);
        } finally {
            proc_terminate($process, 9);
            proc_close($process);
            foreach (glob($directory.'/*') as $file) {
                unlink($file);
            }
            rmdir($directory);
        }
    }
}
