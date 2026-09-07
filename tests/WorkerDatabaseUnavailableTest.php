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

final class WorkerDatabaseUnavailableTest extends TestCase
{
    #[DataProvider('pollKinds')]
    public function testPollRetryKeepsTheIdentityOfAnUncertainClaim(string $kind): void
    {
        $now = 0.0;
        $polls = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($kind, &$polls): array {
            if (str_ends_with($uri, "/{$kind}-tasks/poll")) {
                $polls[] = $body;
                if (count($polls) <= 2) {
                    throw self::unavailable("poll_{$kind}_task", $body);
                }
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = self::worker($transport, $now);

        self::assertFalse($worker->tick(0));

        self::assertCount(3, $polls);
        self::assertSame($polls[0], $polls[1]);
        self::assertSame($polls[0], $polls[2]);
        self::assertEqualsWithDelta(2.0, $now, 0.00001);
    }

    public static function pollKinds(): array
    {
        return [['workflow'], ['activity'], ['query']];
    }

    public function testRegistrationRetriesWithoutChangingItsIdentity(): void
    {
        $now = 0.0;
        $registrations = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$registrations): array {
            if (str_ends_with($uri, '/register')) {
                $registrations[] = $body;
                if (count($registrations) === 1) {
                    throw self::unavailable('register_worker', $body);
                }

                return ['registered' => true];
            }

            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });

        self::worker($transport, $now)->run(0);

        self::assertCount(2, $registrations);
        self::assertSame($registrations[0], $registrations[1]);
        self::assertEqualsWithDelta(1.0, $now, 0.00001);
    }

    public function testHeartbeatRecoversDuringPollBackoffWithoutRecursiveRetry(): void
    {
        $now = 0.0;
        $heartbeatTimes = [];
        $pollIds = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$now, &$heartbeatTimes, &$pollIds): array {
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                $heartbeatTimes[] = $now;
                if (count($heartbeatTimes) === 1) {
                    throw self::unavailable('heartbeat_worker', $body);
                }

                return ['acknowledged' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $pollIds[] = $body['poll_request_id'];
                if (count($pollIds) <= 3) {
                    throw self::unavailable('poll_workflow_task', $body);
                }
            }

            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });

        self::worker($transport, $now)->run(0);

        self::assertCount(3, $heartbeatTimes);
        foreach ($heartbeatTimes as $index => $time) {
            self::assertEqualsWithDelta($index + 1.0, $time, 0.00001);
        }
        self::assertCount(4, $pollIds);
        self::assertCount(1, array_unique($pollIds));
    }

    #[DataProvider('invalidPollResponses')]
    public function testMalformedOutageResponseDoesNotRetry(array $overrides): void
    {
        $now = 0.0;
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($overrides): array {
            throw self::unavailable('poll_workflow_task', $body, $overrides);
        });

        try {
            self::worker($transport, $now)->tick(0);
            self::fail('An invalid retry contract must remain terminal.');
        } catch (ServerException) {
            self::assertCount(1, $transport->requests);
            self::assertSame(0.0, $now);
        }
    }

    public static function invalidPollResponses(): array
    {
        return [
            'worker mismatch' => [['worker_id' => 'another-worker']],
            'queue mismatch' => [['task_queue' => 'another-queue']],
            'poll mismatch' => [['poll_request_id' => 'another-poll']],
            'operation mismatch' => [['operation' => 'poll_activity_task']],
            'outcome missing' => [['outcome' => null]],
            'identity reuse missing' => [['retry_same_poll_request_id' => null]],
            'retry disabled' => [['retryable' => false]],
            'invalid delay' => [['retry_after_seconds' => 'soon']],
            'task is not empty' => [['task' => ['task_id' => 'uncertain-task']]],
        ];
    }

    private static function worker(FakeTransport $transport, float &$now): Worker
    {
        return new Worker(
            new Client('https://server.example', transport: $transport),
            'orders', workerId: 'database-worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );
    }

    private static function unavailable(string $operation, array $request, array $overrides = []): TransportException
    {
        $response = [
            'reason' => 'backend_unavailable', 'operation' => $operation,
            'outcome' => 'unknown', 'worker_id' => $request['worker_id'],
            'task_queue' => $request['task_queue'] ?? null,
            'retryable' => true, 'retry_after_seconds' => 1,
        ];
        if (str_starts_with($operation, 'poll_')) {
            $response += [
                'task' => null, 'poll_status' => 'backend_unavailable',
                'poll_request_id' => $request['poll_request_id'],
                'retry_same_poll_request_id' => true,
            ];
        }
        $response = array_replace($response, $overrides);

        return TransportException::fromResponse(503, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }
}
