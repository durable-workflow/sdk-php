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

    public function testAnOutageThatOutlastsPublishingAResultIsNeverReportedAsAFailure(): void
    {
        // The activity's completion keeps meeting the outage until its retries
        // are spent. The result is left to the Server (lease expiry and
        // re-delivery), never turned into an activity failure report.
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

        self::assertSame(array_fill(0, 9, 'complete'), $reports, 'The first try and eight bounded retries, never a failure report.');
        self::assertContains('worker.outcome_retry_exhausted', $events);
        self::assertContains('worker.outcome_unpublished', $events);
        self::assertGreaterThanOrEqual(2, $workflowPolls, 'The Worker kept polling after the outage.');
    }

    public function testAHeartbeatOutageBetweenTicksIsDeferredAndTheWorkerKeepsRunning(): void
    {
        // A worker heartbeat meets the outage after a tick. It is deferred on
        // the shared schedule; it never ends run().
        $now = 0.0;
        $workflowPolls = 0;
        $heartbeats = 0;
        $handler = static function (string $method, string $uri) use (&$now, &$workflowPolls, &$heartbeats): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                ++$workflowPolls;
                $now += 2.0;

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                if (++$heartbeats === 1) {
                    throw self::storageUnavailable();
                }

                return ['worker_id' => 'worker-1', 'acknowledged' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/poll')) {
                return $workflowPolls >= 3 && str_ends_with($uri, '/query-tasks/poll')
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        $events = [];
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport(handler: $handler)), 'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            diagnosticListener: static function (string $event, array $context) use (&$events): void { $events[] = $event; });

        $worker->run(0);

        self::assertGreaterThanOrEqual(2, $heartbeats, 'The deferred heartbeat was sent again.');
        self::assertContains('worker.retrying', $events);
        self::assertNotContains('worker.fatal_failure', $events);
        self::assertGreaterThanOrEqual(3, $workflowPolls, 'The Worker kept polling after the outage.');
    }

    /** Review HIGH 1: the original incident, a Server warming up while the Worker registers. */
    public function testStorageUnavailableDuringRegistrationIsRetried(): void
    {
        $registrations = 0;
        $handler = static function (string $method, string $uri) use (&$registrations): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                if (++$registrations <= 3) {
                    throw self::storageUnavailable();
                }

                return ['registered' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        [$worker, , $state] = $this->worker($handler);

        $worker->run(0);

        self::assertSame(4, $registrations, 'Three 503 storage_unavailable answers, then registration succeeds.');
        self::assertLessThanOrEqual(1.75, $state->slept, 'Jittered waits of at most 0.25, 0.5 and 1 s.');
    }

    /** Review HIGH 2: a failing heartbeat during a poll retry wait must not end run(). */
    public function testAnOutageThatAlsoFailsTheHeartbeatDuringARetryWaitDoesNotEndTheRun(): void
    {
        $now = 0.0;
        $workflowPolls = 0;
        $heartbeats = 0;
        $handler = static function (string $method, string $uri) use (&$workflowPolls, &$heartbeats): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                if (++$heartbeats <= 3) {
                    throw self::storageUnavailable();
                }

                return ['worker_id' => 'worker-1', 'acknowledged' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                // A 12-attempt outage: long enough for heartbeats to fall due inside the retry waits.
                if (++$workflowPolls <= 12) {
                    throw self::storageUnavailable();
                }

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        $events = [];
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport(handler: $handler)), 'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            diagnosticListener: static function (string $event, array $context) use (&$events): void { $events[] = $event; });

        $worker->run(0);

        self::assertSame(13, $workflowPolls);
        self::assertGreaterThanOrEqual(1, $heartbeats, 'A heartbeat fell due inside a retry wait and met the outage.');
        self::assertNotContains('worker.fatal_failure', $events);
    }

    /**
     * Review HIGH 3: cURL resent a completion whose reply was lost on a reused
     * connection, and the first answer the Worker sees is the Server's
     * duplicate: 409 stale_attempt, recorded false, every status completed.
     * That is our own committed completion, not an error.
     */
    public function testACommittedCompletionReportedOnTheFirstAnswerIsOurs(): void
    {
        $completions = 0;
        $failures = 0;
        $duplicate = ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'completed',
            'recorded' => false, 'reason' => 'stale_attempt', 'next_task_id' => null, 'lease_owner' => 'worker-1',
            'activity_status' => 'completed', 'attempt_status' => 'completed', 'task_status' => 'completed'];
        $handler = static function (string $method, string $uri) use (&$completions, &$failures, $duplicate): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return $completions === 0 ? ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro']] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-1/complete')) {
                ++$completions;
                throw TransportException::fromResponse(409, $duplicate, json_encode($duplicate, JSON_THROW_ON_ERROR));
            }
            if (str_ends_with($uri, '/activity-1/fail')) {
                ++$failures;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        $events = [];
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport(handler: $handler)), 'orders',
            workerId: 'worker-1', sleeper: static function (int $microseconds): void {},
            diagnosticListener: static function (string $event, array $context) use (&$events): void { $events[] = $event; });
        $worker->registerActivity('orders.charge', static fn (ActivityContext $context): string => 'charged');

        $worker->run(0);

        self::assertSame(1, $completions, 'Not resent: the answer already proves the completion committed.');
        self::assertSame(0, $failures);
        self::assertContains('worker.completion_already_committed', $events);
        self::assertNotContains('worker.fatal_failure', $events);
    }

    /**
     * The Server commits the completion but the Worker sees 503 storage_unavailable.
     * The retried completion is answered as Server 3.17.0 answers a duplicate, and
     * the Worker treats it as its own committed completion: no failure report.
     */
    #[DataProvider('completionRetryAnswers')]
    public function testACompletionMetByAnOutageIsRetriedAndADuplicateCountsAsCommitted(array|TransportException $second): void
    {
        $completions = [];
        $failures = 0;
        $handler = static function (string $method, string $uri, array $headers, ?array $body) use (&$completions, &$failures, $second): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return $completions === [] ? ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro']] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/complete')) {
                $completions[] = $body;
                if (count($completions) === 1) {
                    throw self::storageUnavailable();
                }
                if ($second instanceof TransportException) {
                    throw $second;
                }

                return $second;
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/fail')) {
                ++$failures;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        };
        [$worker, , $state] = $this->worker($handler);

        $worker->run(0);

        self::assertSame(1, $state->calls, 'The handler ran once; only the request was retried.');
        self::assertCount(2, $completions);
        self::assertSame($completions[0], $completions[1], 'The same completion, with the same attempt fence.');
        self::assertSame(0, $failures, 'An outage never turns a computed result into a failure report.');
    }

    /** @return iterable<string, array{array<string, mixed>|TransportException}> */
    public static function completionRetryAnswers(): iterable
    {
        yield 'first answer was lost; retry recorded' => [['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
            'outcome' => 'completed', 'recorded' => true, 'reason' => null]];
        $duplicate = ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'completed',
            'recorded' => false, 'reason' => 'stale_attempt', 'next_task_id' => null, 'lease_owner' => 'worker-1',
            'activity_status' => 'completed', 'attempt_status' => 'completed', 'task_status' => 'completed'];
        yield 'first was committed; duplicate answered as Server 3.17.0 does' => [
            TransportException::fromResponse(409, $duplicate, json_encode($duplicate, JSON_THROW_ON_ERROR))];
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
        self::assertTrue(TransientFailure::is(new ServerException('x', 502, null, ['retryable' => true])));
        // An unclassified answer cannot authorize repeating an acknowledgement.
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, null, ['retryable' => true]), acknowledgement: true));
        self::assertTrue(TransientFailure::is(self::storageUnavailable(), acknowledgement: true));
        // A typed refusal keeps its own protocol classification.
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, 'backend_lock_pressure',
            ['reason' => 'backend_lock_pressure', 'retryable' => true, 'retry_after_seconds' => PHP_INT_MAX])));
        self::assertLessThanOrEqual(60.0, TransientFailure::delay(1, PHP_INT_MAX));
        self::assertGreaterThanOrEqual(3.0, TransientFailure::delay(1, 3));
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
