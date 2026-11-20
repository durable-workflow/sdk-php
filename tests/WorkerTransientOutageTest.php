<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\TransientFailure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** A managed Worker outlives an untyped Server or network outage and stops only on fatal refusals. */
final class WorkerTransientOutageTest extends TestCase
{
    /** Native Server's answer while its database is unavailable (for example warming up). */
    private static function storageUnavailable(): TransportException
    {
        $body = ['reason' => 'storage_unavailable', 'message' => 'storage_unavailable'];

        return TransportException::fromResponse(503, $body, json_encode($body, JSON_THROW_ON_ERROR));
    }

    /** @return array{0: Worker, 1: list<string>, 2: object} */
    private function worker(\Closure $handler): array
    {
        $now = 0.0;
        $events = [];
        $state = new class { public float $slept = 0.0; public int $calls = 0; };
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport(handler: $handler)),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, $state): void {
                $now += $microseconds / 1_000_000;
                $state->slept += $microseconds / 1_000_000;
            },
            diagnosticListener: static function (string $event, array $context) use (&$events): void {
                $events[] = $event;
            },
        );
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use ($state): string {
            ++$state->calls;

            return 'charged';
        });

        return [$worker, $events, $state];
    }

    public function testStorageUnavailablePollsAreRetriedWithJitterAndTheWorkerKeepsRunning(): void
    {
        $workflowPolls = 0;
        $completions = 0;
        $events = [];
        $state = null;
        $handler = static function (string $method, string $uri, array $headers, ?array $body) use (&$workflowPolls, &$completions): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                if (++$workflowPolls <= 3) {
                    throw self::storageUnavailable();
                }

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge', 'payload_codec' => 'avro']];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/complete')) {
                ++$completions;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        [$worker, , $state] = $this->worker($handler);

        $worker->run(0);

        self::assertSame(4, $workflowPolls, 'Three 503 storage_unavailable answers, then a poll that succeeds.');
        self::assertSame(1, $state->calls);
        self::assertSame(1, $completions);
        // Jittered: each wait is at most 0.25, 0.5 and 1 s.
        self::assertLessThanOrEqual(1.75, $state->slept);
    }

    public function testATransientFailureThatEscapesATickDoesNotStopTheWorker(): void
    {
        // The activity's completion and its failure report both meet the
        // outage, so the error leaves the tick; the Worker waits and polls on.
        $workflowPolls = 0;
        $activityPolls = 0;
        $reports = [];
        $handler = static function (string $method, string $uri, array $headers, ?array $body) use (&$workflowPolls, &$activityPolls, &$reports): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                ++$workflowPolls;

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                if (++$activityPolls === 1) {
                    return ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                        'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge', 'payload_codec' => 'avro']];
                }

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_contains($uri, '/api/worker/activity-tasks/activity-1/')) {
                $reports[] = substr($uri, strrpos($uri, '/') + 1);
                throw self::storageUnavailable();
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return $workflowPolls >= 2 ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                return ['worker_id' => 'worker-1', 'acknowledged' => true];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        $events = [];
        $now = 0.0;
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport(handler: $handler)), 'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            diagnosticListener: static function (string $event, array $context) use (&$events): void { $events[] = $event; });
        $worker->registerActivity('orders.charge', static fn (ActivityContext $context): string => 'charged');

        $worker->run(0);

        self::assertContains('complete', $reports);
        self::assertGreaterThanOrEqual(2, $workflowPolls, 'The Worker kept polling after the outage.');
        self::assertNotContains('worker.fatal_failure', $events);
        self::assertContains('worker.transient_failure', $events, 'The outage left the tick and was absorbed by the run loop.');
    }

    /** @return iterable<string, array{TransportException, int}> */
    public static function fatalFailures(): iterable
    {
        yield 'authentication' => [TransportException::fromResponse(401, ['reason' => 'authentication_failed'], '{}'), 401];
        yield 'authorization' => [TransportException::fromResponse(403, ['reason' => 'forbidden'], '{}'), 403];
        yield 'explicitly not retryable' => [TransportException::fromResponse(503,
            ['reason' => 'storage_unavailable', 'retryable' => false], '{}'), 503];
        yield 'protocol refusal' => [TransportException::fromResponse(400, ['reason' => 'unsupported_protocol_version'], '{}'), 400];
    }

    #[DataProvider('fatalFailures')]
    public function testAuthenticationAndProtocolFailuresStopTheWorkerAndAreLogged(TransportException $failure, int $status): void
    {
        $handler = static function (string $method, string $uri) use ($failure): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/deregister') || str_contains($uri, '/deregister')) {
                return ['deregistered' => true];
            }
            throw $failure;
        };
        $events = [];
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport(handler: $handler)), 'orders',
            workerId: 'worker-1', sleeper: static function (int $microseconds): void {},
            diagnosticListener: static function (string $event, array $context) use (&$events): void {
                $events[] = [$event, $context['status'] ?? null, $context['reason'] ?? null];
            });

        try {
            $worker->run(0);
            self::fail('A fatal failure must stop the Worker.');
        } catch (ServerException $error) {
            self::assertSame($status, $error->status);
        }
        $fatal = array_values(array_filter($events, static fn (array $event): bool => $event[0] === 'worker.fatal_failure'));
        self::assertCount(1, $fatal, json_encode($events));
        self::assertSame($status, $fatal[0][1]);
        self::assertSame([], array_values(array_filter($events, static fn (array $event): bool => $event[0] === 'worker.transient_failure')));
    }

    public function testClassificationAndJitterBounds(): void
    {
        self::assertTrue(TransientFailure::is(self::storageUnavailable()));
        self::assertTrue(TransientFailure::is(new ServerException('x', 503, 'backend_unavailable', ['reason' => 'backend_unavailable'])));
        self::assertTrue(TransientFailure::is(new ServerException('x', 502, 'anything', ['retryable' => true])));
        self::assertTrue(TransientFailure::is(new TransportException('reset', transientConnectionFailure: true)));
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, 'storage_unavailable', ['retryable' => false])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, 'backend_lock_pressure', ['reason' => 'backend_lock_pressure'])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 401, 'authentication_failed')));
        self::assertFalse(TransientFailure::is(new ServerException('x', 409, 'stale_attempt')));
        self::assertFalse(TransientFailure::is(new \RuntimeException('boom')));
        foreach ([1 => 0.25, 2 => 0.5, 3 => 1.0, 4 => 2.0, 5 => 4.0, 9 => 4.0] as $attempt => $cap) {
            for ($i = 0; $i < 50; ++$i) {
                $delay = TransientFailure::delay($attempt);
                self::assertGreaterThanOrEqual(0.0, $delay);
                self::assertLessThanOrEqual($cap, $delay);
            }
        }
    }
}
