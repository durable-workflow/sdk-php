<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\InMemoryMetricsRecorder;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use PHPUnit\Framework\TestCase;

/**
 * #357: a worker the Server has fenced (409 stale_worker_registration, for
 * example after a pause longer than the registration-freshness window) drops
 * its claims, registers again as a new incarnation and keeps working.
 */
final class WorkerStaleRegistrationTest extends TestCase
{
    private const STOPPED = ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];

    private const EMPTY = ['task' => null, 'poll_status' => 'empty'];

    public function testAFencedActivityClaimIsNeverRetriedAndNewWorkCompletesAfterReregistering(): void
    {
        $registrations = 0;
        $completions = [];
        $activityPolls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$registrations, &$completions, &$activityPolls): mixed {
            if (str_ends_with($uri, '/api/worker/register')) {
                ++$registrations;

                return ['worker_id' => 'worker-1', 'registered' => true, 'heartbeat_interval_seconds' => 30];
            }
            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                return ['acknowledged' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                ++$activityPolls;

                return match ($activityPolls) {
                    1 => self::activityTask('fenced-task'),
                    2 => self::activityTask('fresh-task'),
                    default => self::STOPPED,
                };
            }
            if (preg_match('#/api/worker/(workflow|query)-tasks/poll$#', $uri) === 1) {
                return $activityPolls >= 2 && $completions !== [] && end($completions) === 'fresh-task' ? self::STOPPED : self::EMPTY;
            }
            if (preg_match('#/activity-tasks/([^/]+)/complete$#', $uri, $match) === 1) {
                $completions[] = $match[1];
                if ($match[1] === 'fenced-task') {
                    return self::stale();
                }

                return ['completed' => true];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $events = [];
        $metrics = new InMemoryMetricsRecorder();
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static fn (): float => 0.0,
            sleeper: static function (): void {},
            diagnosticListener: static function (string $event, array $context) use (&$events): void {
                $events[] = $event;
            },
            metrics: $metrics,
        );
        $worker->registerActivity('greet', static fn (ActivityContext $context): string => 'hello');

        $worker->run(0);

        self::assertSame(2, $registrations);
        self::assertSame(['fenced-task', 'fresh-task'], $completions, 'The fenced claim is reported once and never retried.');
        self::assertContains('worker.reregistered', $events);
        self::assertNotContains('worker.failed', $events);
        self::assertSame(1.0, $metrics->counter('durable_workflow_worker_reregistrations', ['task_queue' => 'queue']));
    }

    public function testAStalePollReregistersInsteadOfStopping(): void
    {
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 30, 'registration_token' => null],
            ['task' => null, 'poll_status' => 'stale_worker_registration', 'reason' => 'stale_worker_registration'],
            ['registered' => true, 'heartbeat_interval_seconds' => 30],
            self::STOPPED,
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static fn (): float => 0.0,
        );

        $worker->run(0);

        self::assertSame(
            ['/api/worker/register', '/api/worker/workflow-tasks/poll', '/api/worker/register', '/api/worker/workflow-tasks/poll'],
            array_map(static fn (array $request): string => (string) parse_url($request['uri'], PHP_URL_PATH), $transport->requests),
        );
    }

    public function testConsecutiveRecoveriesBackOffWithJitterUpToTheCap(): void
    {
        $now = 0.0;
        $recoveries = 0;
        $registeredAt = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$now, &$recoveries, &$registeredAt): array {
            if (str_ends_with($uri, '/api/worker/register')) {
                $registeredAt[] = $now;

                return ['registered' => true, 'heartbeat_interval_seconds' => 3600];
            }
            if (count($registeredAt) > 10) {
                return self::STOPPED;
            }
            ++$recoveries;

            return ['task' => null, 'poll_status' => 'stale_worker_registration'];
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
        );

        $worker->run(0);

        $gaps = [];
        for ($index = 1; $index < count($registeredAt); ++$index) {
            $gaps[] = $registeredAt[$index] - $registeredAt[$index - 1];
        }
        self::assertSame(10, $recoveries);
        self::assertEqualsWithDelta(0.0, $gaps[0], 1e-9, 'The first recovery is immediate.');
        foreach ([1 => 1.0, 2 => 2.0, 3 => 4.0, 4 => 8.0, 5 => 16.0, 6 => 32.0, 7 => 60.0, 8 => 60.0, 9 => 60.0] as $index => $base) {
            self::assertGreaterThanOrEqual(0.8 * $base - 0.11, $gaps[$index], "recovery {$index}");
            self::assertLessThanOrEqual($base + 0.11, $gaps[$index], "recovery {$index}");
        }
    }

    public function testAHealthyHeartbeatIntervalResetsTheBackoff(): void
    {
        $now = 0.0;
        $registeredAt = [];
        $staleAt = [];
        $polls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$now, &$registeredAt, &$staleAt, &$polls): array {
            if (str_ends_with($uri, '/api/worker/register')) {
                $registeredAt[] = $now;

                return ['registered' => true, 'heartbeat_interval_seconds' => 10];
            }
            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                return ['acknowledged' => true, 'heartbeat_interval_seconds' => 10];
            }
            ++$polls;
            // Two consecutive stale answers, then healthy polling for longer than one heartbeat interval, then stale again.
            if ($polls <= 2 || $polls === 40) {
                $staleAt[] = $now;

                return ['task' => null, 'poll_status' => 'stale_worker_registration'];
            }
            if ($polls > 40) {
                return self::STOPPED;
            }
            $now += 1.0;

            return self::EMPTY;
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
        );

        $worker->run(0);

        self::assertCount(4, $registeredAt);
        self::assertCount(3, $staleAt);
        self::assertEqualsWithDelta(0.0, $registeredAt[1] - $staleAt[0], 1e-9, 'The first recovery is immediate.');
        self::assertGreaterThanOrEqual(0.8, $registeredAt[2] - $staleAt[1], 'A consecutive recovery backs off.');
        self::assertEqualsWithDelta(0.0, $registeredAt[3] - $staleAt[2], 1e-9, 'After a healthy interval the next recovery is immediate again.');
    }

    public function testStopOnStaleRegistrationKeepsThePreviousStopBehavior(): void
    {
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 30],
            ['task' => null, 'poll_status' => 'stale_worker_registration'],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static fn (): float => 0.0,
            stopOnStaleRegistration: true,
        );

        $worker->run(0);

        self::assertCount(2, $transport->requests);
    }

    public function testStopOnStaleRegistrationStillFailsOnAFencedCompletion(): void
    {
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 30],
            self::EMPTY,
            self::activityTask('fenced-task'),
            self::stale(),
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static fn (): float => 0.0,
            stopOnStaleRegistration: true,
        );
        $worker->registerActivity('greet', static fn (ActivityContext $context): string => 'hello');

        try {
            $worker->run(0);
            self::fail('The fenced completion must still fail the opted-out worker.');
        } catch (ServerException $exception) {
            self::assertSame('stale_worker_registration', $exception->reason);
        }
    }

    public function testPublicTickNeverReregisters(): void
    {
        $transport = new FakeTransport([
            ['task' => null, 'poll_status' => 'stale_worker_registration'],
        ]);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');

        self::assertFalse($worker->tick(0));
        self::assertFalse($worker->tick(0));
        self::assertCount(1, $transport->requests);
    }

    /** @return array<string, mixed> */
    private static function activityTask(string $taskId): array
    {
        return [
            'poll_status' => 'leased',
            'task' => [
                'task_id' => $taskId,
                'activity_attempt_id' => "{$taskId}-attempt",
                'lease_owner' => 'worker-1',
                'activity_type' => 'greet',
                'payload_codec' => 'avro',
            ],
        ];
    }

    private static function stale(): TransportException
    {
        $body = ['reason' => 'stale_worker_registration', 'message' => 'The worker registration is stale.'];

        return TransportException::fromResponse(409, $body, json_encode($body, JSON_THROW_ON_ERROR));
    }
}
