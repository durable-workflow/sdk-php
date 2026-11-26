<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\TransientFailure;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * cloud#752: during a database failover native Server answered a worker poll
 * with HTTP 500 `server_error`, and a managed Worker exited. A generic Server
 * error is an outage: the Worker repeats the same request on a bounded,
 * jittered schedule that honours Retry-After, and run() keeps running.
 */
final class WorkerServerErrorRetryTest extends TestCase
{
    /** Native Server 3.28.0's answer while runtime credentials are unreadable during a failover. */
    private static function serverError(): TransportException
    {
        $body = ['reason' => 'server_error', 'message' => 'Database runtime credential authentication is enabled but unavailable.'];

        return TransportException::fromResponse(500, $body, json_encode($body, JSON_THROW_ON_ERROR));
    }

    /** A load balancer's 503 with a non-protocol JSON body and a Retry-After header. */
    private static function unavailable(string $retryAfter): TransportException
    {
        $body = ['message' => 'Service Unavailable'];

        return TransportException::fromResponse(503, $body, json_encode($body, JSON_THROW_ON_ERROR), ['retry-after' => $retryAfter]);
    }

    /**
     * @param list<array{0: string, 1: int, 2: float}> $retries
     * @param list<string> $events
     */
    private static function worker(FakeTransport $transport, float &$now, float &$slept, array &$retries, array &$events): Worker
    {
        return new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now, &$slept): void {
                // Injected: simulated time only, the test never really sleeps.
                $now += $microseconds / 1_000_000;
                $slept += $microseconds / 1_000_000;
            },
            transientPollRetryObserver: static function (string $operation, int $attempt, float $delay) use (&$retries): void {
                $retries[] = [$operation, $attempt, $delay];
            },
            diagnosticListener: static function (string $event) use (&$events): void { $events[] = $event; });
    }

    /** @return iterable<string, array{string}> */
    public static function pollKinds(): iterable
    {
        foreach (['workflow', 'activity', 'query'] as $kind) {
            yield $kind => [$kind];
        }
    }

    #[DataProvider('pollKinds')]
    public function testAPollThatMeets500Then503KeepsItsIdentityAndTheWorkerKeepsRunning(string $kind): void
    {
        $polls = [];
        $answers = [self::serverError(), self::unavailable('7')];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($kind, &$polls, &$answers): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, "/{$kind}-tasks/poll")) {
                $polls[] = $body;
                if ($answers !== []) {
                    throw array_shift($answers);
                }
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return $polls === [] ? ['task' => null, 'poll_status' => 'empty']
                    : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = $slept = 0.0;
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);

        $worker->run(0);

        self::assertCount(3, $polls, 'A 500, a 503, then the poll that succeeds.');
        self::assertCount(1, array_unique(array_map('serialize', $polls)), 'Repeated with its original poll request ID.');
        self::assertSame([$kind, $kind], array_column($retries, 0));
        self::assertSame([1, 2], array_column($retries, 1));
        self::assertGreaterThanOrEqual(0.0, $retries[0][2]);
        self::assertLessThanOrEqual(0.25, $retries[0][2], 'The first retry waits within the 0.25 s jitter ceiling.');
        self::assertGreaterThanOrEqual(7.0, $retries[1][2], 'The 503 waits at least its Retry-After.');
        self::assertLessThanOrEqual(7.0, $retries[1][2], 'and no longer than it, above the 0.5 s ceiling.');
        self::assertEqualsWithDelta($retries[0][2] + $retries[1][2], $slept, 0.001);
        self::assertContains('worker.stopped', $events);
        self::assertNotContains('worker.fatal_failure', $events);
        self::assertNotContains('worker.failed', $events);
    }

    public function testAnActivityCompletionThatMeets500Then503IsRepeatedWithTheSameAttemptAndLease(): void
    {
        $completions = [];
        $failures = 0;
        $calls = 0;
        $answers = [self::serverError(), self::unavailable('3')];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completions, &$failures, &$answers): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return $completions === [] ? ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro']] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/complete')) {
                $completions[] = $body;
                if ($answers !== []) {
                    throw array_shift($answers);
                }

                return ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'completed', 'recorded' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/fail')) {
                ++$failures;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return $completions === [] ? ['task' => null, 'poll_status' => 'empty']
                    : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = $slept = 0.0;
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$calls): string {
            ++$calls;

            return 'charged';
        });

        $worker->run(0);

        self::assertSame(1, $calls, 'The handler ran once; only the request was repeated.');
        self::assertCount(3, $completions);
        self::assertCount(1, array_unique(array_map('serialize', $completions)), 'Same attempt, lease and result bytes.');
        self::assertSame('attempt-1', $completions[0]['activity_attempt_id']);
        self::assertSame('worker-1', $completions[0]['lease_owner']);
        self::assertSame(0, $failures, 'An outage is never reported as an activity failure.');
        self::assertSame(['activity_complete', 'activity_complete'], array_column($retries, 0));
        self::assertLessThanOrEqual(0.25, $retries[0][2]);
        self::assertSame(3.0, $retries[1][2], 'The 503 waits its Retry-After.');
        self::assertEqualsWithDelta($retries[0][2] + 3.0, $slept, 0.001);
        self::assertContains('worker.stopped', $events);
        self::assertNotContains('worker.fatal_failure', $events);
    }

    public function testAWorkflowCompletionThatMeets500Then503IsRepeatedWithTheSameFence(): void
    {
        $completions = [];
        $answers = [self::serverError(), self::unavailable('2')];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completions, &$answers): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return $completions === [] ? ['poll_status' => 'leased', 'task' => ['task_id' => 'workflow-task-1',
                    'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1', 'workflow_id' => 'workflow-1', 'run_id' => 'run-1',
                    'workflow_type' => 'orders.workflow', 'payload_codec' => 'avro', 'history_events' => []]]
                    : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/heartbeat')) {
                return ['task_id' => 'workflow-task-1', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1',
                    'renewed' => true, 'lease_expires_at' => '2026-07-18T00:40:00Z', 'run_status' => 'running',
                    'task_status' => 'leased', 'reason' => null];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/complete')) {
                $completions[] = $body;
                if ($answers !== []) {
                    throw array_shift($answers);
                }

                return ['task_id' => 'workflow-task-1', 'outcome' => 'completed', 'recorded' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/fail')) {
                self::fail('An outage is never reported as a workflow task failure.');
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return $completions === [] ? ['task' => null, 'poll_status' => 'empty']
                    : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = $slept = 0.0;
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);
        $worker->registerWorkflow('orders.workflow', static fn (WorkflowContext $context): string => 'done');

        $worker->run(0);

        self::assertCount(3, $completions);
        self::assertCount(1, array_unique(array_map('serialize', $completions)), 'Same task attempt, lease and commands.');
        self::assertSame(3, $completions[0]['workflow_task_attempt']);
        self::assertSame('worker-1', $completions[0]['lease_owner']);
        self::assertSame('complete_workflow', $completions[0]['commands'][0]['type']);
        self::assertSame(['workflow_complete', 'workflow_complete'], array_column($retries, 0));
        self::assertLessThanOrEqual(0.25, $retries[0][2]);
        self::assertSame(2.0, $retries[1][2], 'The 503 waits its Retry-After.');
        self::assertContains('worker.stopped', $events);
        self::assertNotContains('worker.fatal_failure', $events);
    }

    public function testActivityAndWorkerHeartbeatsRideOutA500(): void
    {
        $now = $slept = 0.0;
        $activityHeartbeats = $workerHeartbeats = $completions = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$now, &$activityHeartbeats, &$workerHeartbeats, &$completions): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/worker/heartbeat')) {
                if (++$workerHeartbeats === 1) {
                    throw self::serverError();
                }

                return ['worker_id' => 'worker-1', 'acknowledged' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/heartbeat')) {
                if (++$activityHeartbeats === 1) {
                    throw self::serverError();
                }

                return ['cancel_requested' => false, 'can_continue' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/complete')) {
                ++$completions;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return $completions === 0 ? ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro']] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $now += 2.0;

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return $workerHeartbeats >= 2 && $completions === 1
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);
        $worker->registerActivity('orders.charge', static function (ActivityContext $context): string {
            $context->heartbeat(['progress' => 1]);

            return 'charged';
        });

        $worker->run(0);

        self::assertSame(2, $activityHeartbeats, 'The activity heartbeat was repeated, not raised into the handler.');
        self::assertSame(1, $completions);
        self::assertGreaterThanOrEqual(2, $workerHeartbeats, 'The worker heartbeat was deferred and sent again.');
        self::assertContains('activity_heartbeat', array_column($retries, 0));
        self::assertContains('heartbeat', array_column($retries, 0));
        self::assertNotContains('worker.fatal_failure', $events);
    }

    public function testARepeatedFailureReportThatMeetsItsOwnFenceIsSettled(): void
    {
        // The first failure report meets a 500 and may have been recorded. Its
        // repeat is refused as a stale attempt: the Server owns the outcome.
        $reports = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$reports): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return $reports === [] ? ['poll_status' => 'leased', 'task' => ['task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro']] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/fail')) {
                $reports[] = $body;
                if (count($reports) === 1) {
                    throw self::serverError();
                }
                $stale = ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'failed',
                    'recorded' => false, 'reason' => 'stale_attempt', 'next_task_id' => null];

                throw TransportException::fromResponse(409, $stale, json_encode($stale, JSON_THROW_ON_ERROR));
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return $reports === [] ? ['task' => null, 'poll_status' => 'empty']
                    : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = $slept = 0.0;
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);
        $worker->registerActivity('orders.charge', static function (ActivityContext $context): string {
            throw new \RuntimeException('card declined');
        });

        $worker->run(0);

        self::assertCount(2, $reports);
        self::assertSame($reports[0], $reports[1], 'Repeated with the same attempt and lease.');
        self::assertContains('worker.outcome_already_settled', $events);
        self::assertContains('worker.stopped', $events);
        self::assertNotContains('worker.fatal_failure', $events);
    }

    /** @return iterable<string, array{TransportException}> */
    public static function definitiveRefusals(): iterable
    {
        yield '401' => [TransportException::fromResponse(401, ['reason' => 'unauthorized'], '')];
        yield '403' => [TransportException::fromResponse(403, ['message' => 'Forbidden'], '')];
        yield '500 retryable false' => [TransportException::fromResponse(500, ['message' => 'x', 'retryable' => false], '')];
        yield '501' => [TransportException::fromResponse(501, ['message' => 'Not Implemented'], '')];
        yield '503 retryable false' => [TransportException::fromResponse(503, ['reason' => 'service_unavailable', 'retryable' => false], '')];
    }

    #[DataProvider('definitiveRefusals')]
    public function testDefinitiveRefusalsStillStopTheWorker(TransportException $refusal): void
    {
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($refusal): ?array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                throw $refusal;
            }
            if (str_contains($uri, '/worker/registrations/worker-1')) {
                return ['worker_id' => 'worker-1', 'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = $slept = 0.0;
        $retries = $events = [];
        $worker = self::worker($transport, $now, $slept, $retries, $events);

        try {
            $worker->run(0);
            self::fail('A definitive refusal must still end run().');
        } catch (ServerException $error) {
            self::assertSame($refusal->status, $error->status);
        }
        self::assertSame([], $retries);
        self::assertContains('worker.fatal_failure', $events);
    }

    public function testClassification(): void
    {
        self::assertTrue(TransientFailure::is(self::serverError()));
        self::assertTrue(TransientFailure::is(self::serverError(), acknowledgement: true));
        foreach ([500, 502, 503, 504] as $status) {
            self::assertTrue(TransientFailure::is(new ServerException('x', $status, null, ['message' => 'Server Error'])), (string) $status);
            self::assertTrue(TransientFailure::is(new ServerException('x', $status, null, ['message' => 'x']), acknowledgement: true));
        }
        // Typed refusals keep their protocol classification.
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, 'backend_lock_pressure', ['reason' => 'backend_lock_pressure'])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 503, 'storage_pressure', ['reason' => 'storage_pressure'])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 500, 'invalid_poll_receipt', ['reason' => 'invalid_poll_receipt'])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 409, 'stale_attempt', ['reason' => 'stale_attempt'])));
        self::assertFalse(TransientFailure::is(new ServerException('x', 429, null, ['message' => 'slow down'])));
    }

    public function testRetryAfterIsReadAsDeltaSecondsOrAnHttpDateAndIsCapped(): void
    {
        $now = 1_764_100_000.0;
        $header = static fn (string $value): ServerException => new ServerException('x', 503, null, ['message' => 'x'],
            TransportException::fromResponse(503, ['message' => 'x'], '', ['Retry-After' => $value]));

        self::assertSame(7, TransientFailure::retryAfterSeconds($header('7'), $now));
        self::assertSame(60, TransientFailure::retryAfterSeconds($header('3600'), $now), 'Capped at 60 s.');
        self::assertSame(12, TransientFailure::retryAfterSeconds($header(gmdate('D, d M Y H:i:s', (int) $now + 12).' GMT'), $now));
        self::assertSame(60, TransientFailure::retryAfterSeconds($header(gmdate('D, d M Y H:i:s', (int) $now + 900).' GMT'), $now));
        self::assertNull(TransientFailure::retryAfterSeconds($header(gmdate('D, d M Y H:i:s', (int) $now - 5).' GMT'), $now), 'A past date asks for no wait.');
        foreach (['0', '-1', '1.5', 'soon', 'Mon, 99 Foo 2026 00:00:00 GMT', ''] as $invalid) {
            self::assertNull(TransientFailure::retryAfterSeconds($header($invalid), $now), $invalid);
        }
        // The body's own retry_after_seconds wins over the header.
        self::assertSame(4, TransientFailure::retryAfterSeconds(new ServerException('x', 503, null, ['retry_after_seconds' => 4],
            TransportException::fromResponse(503, ['retry_after_seconds' => 4], '', ['retry-after' => '30'])), $now));
    }

    public function testTheBackoffIsBoundedAndNeverShorterThanABoundedRetryAfter(): void
    {
        for ($attempt = 1; $attempt <= 40; ++$attempt) {
            $delay = TransientFailure::delay($attempt);
            self::assertGreaterThanOrEqual(0.0, $delay);
            self::assertLessThanOrEqual(min(4.0, 0.25 * 2 ** ($attempt - 1)), $delay);
            self::assertGreaterThanOrEqual(5.0, TransientFailure::delay($attempt, 5));
            self::assertSame(60.0, TransientFailure::delay($attempt, 600));
        }
    }

    public function testTheHttpTransportKeepsTheRetryAfterHeaderOfAnErrorResponse(): void
    {
        $http = new class implements ClientInterface {
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                return new Response(503, ['Content-Type' => 'application/json', 'Retry-After' => '9'], '{"message":"Service Unavailable"}');
            }
        };
        $client = new Client('https://server.example', transport: new Psr18Transport($http));

        try {
            $client->heartbeatWorker('worker-1');
            self::fail('The 503 must be raised.');
        } catch (ServerException $error) {
            self::assertSame(503, $error->status);
            self::assertTrue(TransientFailure::is($error));
            self::assertSame(9, TransientFailure::retryAfterSeconds($error, 0.0));
        }
    }
}
