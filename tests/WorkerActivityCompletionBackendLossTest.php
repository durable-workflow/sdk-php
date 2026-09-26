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

final class WorkerActivityCompletionBackendLossTest extends TestCase
{
    #[DataProvider('completionOutcomes')]
    public function testBackendLossRetriesTheSameActivityResultWithoutRerunningHandler(bool $committedFirst): void
    {
        $now = 0.0;
        $handlerCalls = 0;
        $completionBodies = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$completionBodies, $committedFirst): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => [
                    'task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1',
                    'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro',
                ], 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/complete')) {
                $completionBodies[] = $body;
                if (count($completionBodies) === 1) {
                    throw self::failure(503, self::backendUnavailable());
                }

                return $committedFirst
                    ? throw self::failure(409, self::committedConflict())
                    : ['recorded' => true, 'outcome' => 'completed'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$handlerCalls): string {
            ++$handlerCalls;

            return 'charged';
        });

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $handlerCalls);
        self::assertCount(2, $completionBodies);
        self::assertSame($completionBodies[0], $completionBodies[1]);
        self::assertEqualsWithDelta(1.0, $now, 0.000_01);
    }

    /** @return iterable<string, array{bool}> */
    public static function completionOutcomes(): iterable
    {
        yield 'rollback then accepted' => [false];
        yield 'committed before the response was lost' => [true];
    }

    public function testTypedBackendLossMustMatchTheSubmittedActivityFence(): void
    {
        $error = new ServerException('Backend unavailable', 503, 'backend_unavailable', self::backendUnavailable());

        self::assertTrue($error->isActivityTaskBackendUnavailable('activity-1', 'attempt-1', 'worker-1'));
        self::assertFalse($error->isActivityTaskBackendUnavailable('other-task', 'attempt-1', 'worker-1'));
        self::assertFalse($error->isActivityTaskBackendUnavailable('activity-1', 'other-attempt', 'worker-1'));
        self::assertFalse($error->isActivityTaskBackendUnavailable('activity-1', 'attempt-1', 'other-worker'));

        $wrongOperation = self::backendUnavailable();
        $wrongOperation['operation'] = 'heartbeat_activity_task';
        self::assertFalse((new ServerException('Backend unavailable', 503, 'backend_unavailable', $wrongOperation))
            ->isActivityTaskBackendUnavailable('activity-1', 'attempt-1', 'worker-1'));
    }

    public function testIncompleteStaleAttemptDoesNotBecomeACompletion(): void
    {
        $now = 0.0;
        $completionCalls = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
        ) use (&$completionCalls): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => [
                    'task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1',
                    'activity_type' => 'orders.charge',
                    'payload_codec' => 'avro',
                ], 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-1/complete')) {
                ++$completionCalls;
                if ($completionCalls === 1) {
                    throw self::failure(503, self::backendUnavailable());
                }
                $conflict = self::committedConflict();
                $conflict['attempt_status'] = 'failed';
                throw self::failure(409, $conflict);
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );
        $worker->registerActivity('orders.charge', static fn (ActivityContext $context): string => 'charged');

        $this->expectException(ServerException::class);
        $this->expectExceptionMessage('HTTP 409');
        try {
            $worker->tick(0);
        } finally {
            self::assertSame(2, $completionCalls);
        }
    }

    /** @return array<string, mixed> */
    private static function backendUnavailable(): array
    {
        return [
            'reason' => 'backend_unavailable',
            'operation' => 'complete_activity_task',
            'outcome' => 'unknown',
            'task_id' => 'activity-1',
            'activity_attempt_id' => 'attempt-1',
            'lease_owner' => 'worker-1',
            'worker_id' => 'worker-1',
            'task_queue' => null,
            'retryable' => true,
            'retry_after_seconds' => 1,
        ];
    }

    /** @return array<string, mixed> */
    private static function committedConflict(): array
    {
        return [
            'reason' => 'stale_attempt',
            'outcome' => 'completed',
            'recorded' => false,
            'task_id' => 'activity-1',
            'activity_attempt_id' => 'attempt-1',
            'lease_owner' => 'worker-1',
            'activity_status' => 'completed',
            'attempt_status' => 'completed',
            'task_status' => 'completed',
        ];
    }

    /** @param array<string, mixed> $response */
    private static function failure(int $status, array $response): TransportException
    {
        return TransportException::fromResponse($status, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }
}
