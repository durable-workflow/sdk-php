<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerTransientPollRetryTest extends TestCase
{
    #[DataProvider('pollKindProvider')]
    public function testEveryPollKindRetriesTypedBackendLockPressure(
        string $taskKind,
        array $responses,
        array $expectedPollKinds,
    ): void {
        $now = 0.0;
        $observedRetries = [];
        $transport = new FakeTransport($responses);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
            transientPollRetryObserver: static function (
                string $observedTaskKind,
                int $attempt,
                float $delaySeconds,
                ServerException $exception,
            ) use (&$observedRetries): void {
                $observedRetries[] = [
                    'task_kind' => $observedTaskKind,
                    'attempt' => $attempt,
                    'delay_seconds' => $delaySeconds,
                    'reason' => $exception->reason,
                ];
            },
        );

        self::assertFalse($worker->tick(0));

        self::assertSame([[
            'task_kind' => $taskKind,
            'attempt' => 1,
            'delay_seconds' => 1.0,
            'reason' => 'backend_lock_pressure',
        ]], $observedRetries);
        self::assertEqualsWithDelta(1.0, $now, 0.000_001);
        self::assertSame(
            $expectedPollKinds,
            array_map(
                static fn (array $request): string => (string) preg_replace(
                    '#^.*/worker/(workflow|activity|query)-tasks/poll$#',
                    '$1',
                    $request['uri'],
                ),
                $transport->requests,
            ),
        );
        foreach ($transport->requests as $request) {
            self::assertSame('worker-1', $request['body']['worker_id'] ?? null);
            self::assertSame('orders', $request['body']['task_queue'] ?? null);
        }
    }

    /** @return iterable<string, array{string, list<array<string, mixed>|TransportException>, list<string>}> */
    public static function pollKindProvider(): iterable
    {
        $empty = ['task' => null, 'poll_status' => 'empty'];

        yield 'workflow' => [
            'workflow',
            [self::lockPressure(), $empty, $empty, $empty],
            ['workflow', 'workflow', 'activity', 'query'],
        ];
        yield 'activity' => [
            'activity',
            [$empty, self::lockPressure(), $empty, $empty],
            ['workflow', 'activity', 'activity', 'query'],
        ];
        yield 'query' => [
            'query',
            [$empty, $empty, self::lockPressure(), $empty],
            ['workflow', 'activity', 'query', 'query'],
        ];
    }

    public function testExplicitlyRetryablePollRefusalUsesBoundedFallbackBackoff(): void
    {
        $now = 0.0;
        $observedDelay = null;
        $response = [
            'task' => null,
            'poll_status' => 'service_temporarily_unavailable',
            'reason' => 'service_temporarily_unavailable',
            'retryable' => true,
        ];
        $transport = new FakeTransport([
            self::failure(503, $response),
            ['task' => null, 'poll_status' => 'empty'],
            ['task' => null, 'poll_status' => 'empty'],
            ['task' => null, 'poll_status' => 'empty'],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
            transientPollRetryObserver: static function (
                string $taskKind,
                int $attempt,
                float $delaySeconds,
                ServerException $exception,
            ) use (&$observedDelay): void {
                $observedDelay = $delaySeconds;
            },
        );

        self::assertFalse($worker->tick(0));

        self::assertSame(0.1, $observedDelay);
        self::assertEqualsWithDelta(0.1, $now, 0.000_001);
    }

    public function testTransientRefusalThenAvailableTaskIsCompletedOnceByTheSameManagedWorker(): void
    {
        $now = 0.0;
        $activityPolls = [];
        $handlerCalls = 0;
        $completions = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$activityPolls, &$handlerCalls, &$completions): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                $activityPolls[] = $body;
                if (count($activityPolls) === 1) {
                    throw self::lockPressure();
                }
                if (count($activityPolls) === 2) {
                    return [
                        'poll_status' => 'leased',
                        'task' => [
                            'task_id' => 'activity-1',
                            'activity_attempt_id' => 'attempt-1',
                            'lease_owner' => 'worker-1',
                            'activity_type' => 'orders.charge',
                        ],
                    ];
                }

                self::fail('The activity task must not be polled a third time.');
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/complete')) {
                ++$completions;
                self::assertSame('worker-1', $body['lease_owner'] ?? null);

                return ['completed' => true];
            }

            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );
        $worker->registerActivity(
            'orders.charge',
            static function (ActivityContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'charged';
            },
        );

        $worker->run(0);

        self::assertSame(1, $handlerCalls);
        self::assertSame(1, $completions);
        self::assertCount(2, $activityPolls);
        foreach ($activityPolls as $poll) {
            self::assertSame('worker-1', $poll['worker_id'] ?? null);
            self::assertSame('orders', $poll['task_queue'] ?? null);
        }
    }

    public function testManagedWorkerHeartbeatsThroughoutTransientPollBackoff(): void
    {
        $now = 0.0;
        $workflowPollTimes = [];
        $heartbeatTimes = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$now, &$workflowPollTimes, &$heartbeatTimes): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }

            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                $heartbeatTimes[] = $now;
                self::assertSame('worker-1', $body['worker_id'] ?? null);

                return ['acknowledged' => true, 'heartbeat_interval_seconds' => 1];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $workflowPollTimes[] = $now;

                return count($workflowPollTimes) === 1
                    ? throw self::lockPressure(3)
                    : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );

        $worker->run(0);

        self::assertCount(2, $workflowPollTimes);
        self::assertEqualsWithDelta(0.0, $workflowPollTimes[0], 0.000_01);
        self::assertEqualsWithDelta(3.0, $workflowPollTimes[1], 0.000_01);
        self::assertGreaterThanOrEqual(2, count($heartbeatTimes));
        self::assertLessThanOrEqual(1.000_01, $heartbeatTimes[0]);
        foreach (array_slice($heartbeatTimes, 1) as $index => $heartbeatTime) {
            self::assertLessThanOrEqual(1.000_01, $heartbeatTime - $heartbeatTimes[$index]);
        }
        self::assertLessThanOrEqual(1.000_01, $workflowPollTimes[1] - end($heartbeatTimes));
    }

    public function testShutdownInterruptsTransientPollBackoffWithoutAnotherPoll(): void
    {
        $now = 0.0;
        $sleepCalls = 0;
        $worker = null;
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 1],
            self::lockPressure(5),
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, &$sleepCalls, &$worker): void {
                $now += $microseconds / 1_000_000;
                ++$sleepCalls;
                $worker?->requestShutdown();
            },
        );

        $worker->run(0);

        self::assertSame(1, $sleepCalls);
        self::assertCount(2, $transport->requests);
        self::assertStringEndsWith('/api/worker/workflow-tasks/poll', $transport->requests[1]['uri']);
    }

    public function testRepeatedTransientRefusalsUseObservableCappedBackoff(): void
    {
        $now = 0.0;
        $attempts = [];
        $delays = [];
        $responses = array_fill(0, 8, self::lockPressure());
        $responses[] = ['task' => null, 'poll_status' => 'empty'];
        $responses[] = ['task' => null, 'poll_status' => 'empty'];
        $responses[] = ['task' => null, 'poll_status' => 'empty'];
        $transport = new FakeTransport($responses);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
            transientPollRetryObserver: static function (
                string $taskKind,
                int $attempt,
                float $delaySeconds,
                ServerException $exception,
            ) use (&$attempts, &$delays): void {
                self::assertSame('workflow', $taskKind);
                $attempts[] = $attempt;
                $delays[] = $delaySeconds;
            },
        );

        self::assertFalse($worker->tick(0));

        self::assertSame(range(1, 8), $attempts);
        self::assertSame([1.0, 1.0, 1.0, 1.0, 1.6, 3.2, 5.0, 5.0], $delays);
        self::assertEqualsWithDelta(18.8, $now, 0.000_1);
        self::assertCount(11, $transport->requests);
    }

    #[DataProvider('fatalPollFailureProvider')]
    public function testAuthenticationMalformedAndNonRetryableFailuresRemainFatal(
        TransportException $failure,
        int $expectedStatus,
    ): void {
        $sleepCalls = 0;
        $observedRetries = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport([$failure])),
            'orders',
            sleeper: static function (int $microseconds) use (&$sleepCalls): void {
                ++$sleepCalls;
            },
            transientPollRetryObserver: static function (
                string $taskKind,
                int $attempt,
                float $delaySeconds,
                ServerException $exception,
            ) use (&$observedRetries): void {
                ++$observedRetries;
            },
        );

        try {
            $worker->tick(0);
            self::fail('The poll failure should remain fatal.');
        } catch (ServerException $exception) {
            self::assertSame($expectedStatus, $exception->status);
        }

        self::assertSame(0, $sleepCalls);
        self::assertSame(0, $observedRetries);
    }

    /** @return iterable<string, array{TransportException, int}> */
    public static function fatalPollFailureProvider(): iterable
    {
        yield 'authentication failure' => [self::failure(401, [
            'message' => 'Invalid worker token.',
            'reason' => 'authentication_failed',
        ]), 401];

        yield 'non-retryable service failure' => [self::failure(503, [
            'task' => null,
            'poll_status' => 'service_unavailable',
            'reason' => 'service_unavailable',
            'retryable' => false,
            'retry_after_seconds' => 1,
        ]), 503];

        yield 'malformed retry delay' => [self::failure(503, [
            'task' => null,
            'poll_status' => 'backend_lock_pressure',
            'reason' => 'backend_lock_pressure',
            'retry_after_seconds' => 'soon',
        ]), 503];

        yield 'malformed task envelope' => [self::failure(503, [
            'poll_status' => 'backend_lock_pressure',
            'reason' => 'backend_lock_pressure',
            'retry_after_seconds' => 1,
        ]), 503];
    }

    public function testTypedTerminalRegistrationOutcomeStillStopsWithoutRetry(): void
    {
        $response = [
            'task' => null,
            'poll_status' => 'draining',
            'reason' => 'worker_draining',
        ];
        $retries = 0;
        $transport = new FakeTransport([self::failure(409, $response)]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            transientPollRetryObserver: static function (
                string $taskKind,
                int $attempt,
                float $delaySeconds,
                ServerException $exception,
            ) use (&$retries): void {
                ++$retries;
            },
        );

        self::assertFalse($worker->tick(0));
        self::assertFalse($worker->tick(0));
        self::assertSame(0, $retries);
        self::assertCount(1, $transport->requests);
    }

    private static function lockPressure(int $retryAfterSeconds = 1): TransportException
    {
        return self::failure(503, [
            'task' => null,
            'poll_status' => 'backend_lock_pressure',
            'reason' => 'backend_lock_pressure',
            'retry_after_seconds' => $retryAfterSeconds,
            'task_kind' => 'workflow_task',
            'namespace' => 'default',
            'task_queue' => 'orders',
            'backend' => ['driver' => 'sqlite', 'lock_pressure' => true],
        ]);
    }

    /** @param array<string, mixed> $response */
    private static function failure(int $status, array $response): TransportException
    {
        return TransportException::fromResponse(
            $status,
            $response,
            json_encode($response, JSON_THROW_ON_ERROR),
        );
    }
}
