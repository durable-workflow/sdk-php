<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerConnectionRecoveryTest extends TestCase
{
    #[DataProvider('pollKinds')]
    public function testEveryPollRecoversWithoutRepeatingEarlierPolls(int $position): void
    {
        $empty = ['task' => null, 'poll_status' => 'empty'];
        $responses = [$empty, $empty, $empty];
        array_splice($responses, $position, 0, [self::disconnected(), self::disconnected()]);
        $transport = new FakeTransport($responses);
        $now = 0.0;
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void { $now += $us / 1_000_000; },
        );

        self::assertFalse($worker->tick(0));
        self::assertCount(5, $transport->requests);
        self::assertSame($transport->requests[$position], $transport->requests[$position + 1]);
        self::assertSame($transport->requests[$position], $transport->requests[$position + 2]);
        self::assertEqualsWithDelta(0.1, $now, 0.00001);
    }

    /** @return iterable<string, array{int}> */
    public static function pollKinds(): iterable
    {
        yield 'workflow' => [0];
        yield 'activity' => [1];
        yield 'query' => [2];
    }

    public function testHeartbeatOutageDuringPollBackoffRecoversWithoutRecursiveWaiting(): void
    {
        $now = 0.0;
        $heartbeats = [];
        $retries = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$now, &$heartbeats): array {
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                $heartbeats[] = $now;
                if (count($heartbeats) < 4) {
                    throw self::disconnected();
                }

                return ['acknowledged' => true];
            }
            if (count($heartbeats) < 5) {
                throw self::disconnected();
            }

            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void {
                $now += $us / 1_000_000;
                self::assertLessThan(15, $now, 'Recovery must not hang.');
            },
            transientPollRetryObserver: static function (string $phase, int $attempt, float $delay) use (&$retries): void {
                if ($phase === 'heartbeat') {
                    $retries[] = [$attempt, $delay];
                }
            },
        );

        $worker->run(0);

        self::assertSame([[1, 0.1], [2, 0.2], [3, 0.4]], $retries);
        foreach ([1.0, 1.1, 1.3, 1.7, 2.7] as $index => $expected) {
            self::assertEqualsWithDelta($expected, $heartbeats[$index], 0.00001);
        }
        self::assertSame(1, count(array_filter($transport->requests, static fn (array $r): bool => str_ends_with($r['uri'], '/register'))));
    }

    public function testShutdownInterruptsAnOutageWithoutAnotherPollOrHeartbeat(): void
    {
        $now = 0.0;
        $worker = null;
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 1],
            self::disconnected(),
            self::disconnected(),
            ['deregistered' => true],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now, &$worker): void {
                $now += $us / 1_000_000;
                $worker?->requestShutdown();
            },
        );

        $worker->run(0);

        self::assertCount(4, $transport->requests);
        self::assertSame('DELETE', $transport->requests[3]['method']);
        self::assertEqualsWithDelta(0.1, $now, 0.00001);
    }

    #[DataProvider('terminalFailures')]
    public function testHeartbeatErrorsOtherThanClassifiedConnectionLossRemainFatal(TransportException $failure): void
    {
        $now = 0.0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$now, $failure): array {
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                throw $failure;
            }
            ++$now;

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (): void { self::fail('A terminal heartbeat must not retry.'); },
        );

        try {
            $worker->run(0);
            self::fail('The worker must surface the terminal error.');
        } catch (ServerException $exception) {
            self::assertSame($failure, $exception->getPrevious());
        }
    }

    /** @return iterable<string, array{TransportException}> */
    public static function terminalFailures(): iterable
    {
        yield 'opaque network error' => [new TransportException('Connection error without classification.')];
        yield 'malformed JSON' => [new TransportException('Invalid JSON.')];
        foreach ([401, 403, 409, 503] as $status) {
            yield "HTTP {$status}" => [TransportException::fromResponse($status, ['reason' => 'refused'], '')];
        }
    }

    public function testAmbiguousRegistrationIsNotRepeated(): void
    {
        $transport = new FakeTransport([self::disconnected()]);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders');

        try {
            $worker->run(0);
            self::fail('Registration requires explicit reconciliation, not blind retry.');
        } catch (ServerException $exception) {
            self::assertTrue($exception->isTransientConnectionFailure());
        }
        self::assertCount(1, $transport->requests);
    }

    private static function disconnected(): TransportException
    {
        return new TransportException('Connection refused.', transientConnectionFailure: true);
    }
}
