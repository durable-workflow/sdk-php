<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestBudget;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

final class RequestBudgetRecoveryTest extends TestCase
{
    public function testARealReplyAfterTheOriginalIoBudgetCanBeRetried(): void
    {
        if (!extension_loaded('curl')) self::markTestSkipped('The bounded HTTP transport requires cURL.');
        $listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertIsResource($listener);
        $address = stream_socket_get_name($listener, false);
        fclose($listener);
        $log = tempnam(sys_get_temp_dir(), 'dw-budget-');
        $serverLog = tempnam(sys_get_temp_dir(), 'dw-budget-server-');
        $server = proc_open([PHP_BINARY, '-S', $address, __DIR__.'/fixtures/request-budget-router.php'],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', $serverLog, 'a'], 2 => ['file', $serverLog, 'a']],
            $pipes, env_vars: ['DW_BUDGET_RECEIPTS' => $log]);
        self::assertIsResource($server);
        try {
            $readyUntil = microtime(true) + 3;
            $ready = false;
            do {
                $connection = @stream_socket_client('tcp://'.$address, $errno, $error, .1);
                if (is_resource($connection)) { $ready = true; fclose($connection); break; }
                usleep(10000);
            } while (microtime(true) < $readyUntil);
            self::assertTrue($ready, file_get_contents($serverLog));
            $client = (new Client('http://'.$address))->withBoundedWorkerRequests();
            $budget = new RequestBudget(1);
            usleep(850000);
            try {
                $client->heartbeatWorker('worker-1', budget: $budget);
                self::fail('A successful HTTP reply must still respect the original monotonic budget.');
            } catch (ServerException $error) {
                self::assertStringContainsString('request time budget expired', $error->getMessage());
                self::assertTrue($error->isTransientConnectionFailure());
            }
            $client->heartbeatWorker('worker-1');
            $receipts = array_map(static fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR),
                file($log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES));
            self::assertCount(2, $receipts, 'Both heartbeats must execute real HTTP requests.');
            self::assertSame(['worker-1', 'worker-1'], array_column($receipts, 'worker_id'));
        } finally {
            proc_terminate($server, 15);
            proc_close($server);
            unlink($log);
            unlink($serverLog);
        }
    }

    public function testExpiredIdleRequestIsRetryableWithoutSendingAnotherRequestUnderTheOldBudget(): void
    {
        $requests = 0;
        $http = new GuzzleClient(['handler' => static function () use (&$requests) {
            ++$requests;
            return Create::promiseFor(new Response(200, [], '{"acknowledged":true}'));
        }]);
        $client = (new Client('https://server.example', transport: new Psr18Transport($http)))
            ->withBoundedWorkerRequests();
        $budget = new RequestBudget(1);
        usleep(1_100_000);
        try {
            $client->heartbeatWorker('worker-1', budget: $budget);
            self::fail('An expired original budget must refuse I/O.');
        } catch (ServerException $error) {
            self::assertSame(0, $error->status);
            self::assertTrue($error->isTransientConnectionFailure());
            self::assertSame(0, $requests);
        }
        $client->heartbeatWorker('worker-1');
        self::assertSame(1, $requests, 'The next idle attempt gets its own bounded I/O budget.');
    }

    public function testExpiredAuthorityIsNotReclassifiedAsAnOrdinaryIdleTimeout(): void
    {
        $budget = new RequestBudget(5, hrtime(true) / 1e9 - 1);
        try {
            $budget->remainingSeconds();
            self::fail('Expired authority must refuse another request.');
        } catch (TransportException $error) {
            self::assertFalse($error->transientConnectionFailure);
        }
    }

    public function testAuthorityLearnedDuringIoKeepsItsOriginalCeiling(): void
    {
        $budget = new RequestBudget(5);
        $budget->restrictWallAuthorityDeadline(new \DateTimeImmutable('-1 second'));
        try {
            $budget->remainingSeconds();
            self::fail('A learned authority deadline must not become a fresh I/O attempt.');
        } catch (TransportException $error) {
            self::assertFalse($error->transientConnectionFailure);
        }
    }
}
