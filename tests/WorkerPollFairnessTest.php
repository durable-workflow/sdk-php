<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerPollFairnessTest extends TestCase
{
    /** @param list<string> $blockedKinds */
    #[DataProvider('blockedKinds')]
    public function testWaitCapacityRefusalsGiveEveryTaskKindATurn(array $blockedKinds): void
    {
        $now = 0.0;
        $worker = null;
        $polls = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use ($blockedKinds, &$polls): array {
            preg_match('#/worker/(workflow|activity|query)-tasks/poll$#', $uri, $match);
            self::assertNotEmpty($match);
            $kind = $match[1];
            $polls[] = ['kind' => $kind, 'id' => $body['poll_request_id']];
            if (in_array($kind, $blockedKinds, true)) {
                throw self::capacityRefusal($kind);
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, &$worker): void {
                $now += $microseconds / 1_000_000;
                if ($now >= 20) {
                    $worker->requestShutdown();
                }
            },
        );

        self::assertFalse($worker->tick(5));
        self::assertFalse($worker->tick(5));

        self::assertSame(['workflow', 'activity', 'query', 'workflow', 'activity', 'query'], array_column($polls, 'kind'));
        self::assertCount(6, array_unique(array_column($polls, 'id')));
        self::assertGreaterThanOrEqual(count($blockedKinds) * 2, $now);
        self::assertEqualsWithDelta(count($blockedKinds) * 2, $now, 0.000_01);
    }

    /** @return iterable<string, array{list<string>}> */
    public static function blockedKinds(): iterable
    {
        yield 'workflow' => [['workflow']];
        yield 'activity' => [['activity']];
        yield 'query' => [['query']];
        yield 'all' => [['workflow', 'activity', 'query']];
    }

    public function testManagedWorkerCompletesActivityWhileWorkflowWaitsAreFull(): void
    {
        $now = 0.0;
        $worker = null;
        $calls = 0;
        $completions = 0;
        $heartbeats = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$heartbeats, &$completions): array {
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                ++$heartbeats;
                return ['acknowledged' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                throw self::capacityRefusal('workflow');
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge', 'payload_codec' => 'avro',
                ]];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-1/complete')) {
                ++$completions;
                self::assertSame('worker-1', $body['lease_owner']);
                self::assertSame('attempt-1', $body['activity_attempt_id']);
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if ($method === 'DELETE' && str_ends_with($uri, '/registrations/worker-1')) {
                return ['deregistered' => true];
            }
            self::fail("Unexpected request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, &$worker): void {
                $now += $microseconds / 1_000_000;
                if ($now >= 20) {
                    $worker->requestShutdown();
                }
            },
        );
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$calls): string {
            ++$calls;
            return 'charged';
        });
        $worker->run(5);

        self::assertSame(1, $calls);
        self::assertSame(1, $completions);
        self::assertGreaterThanOrEqual(1, $heartbeats);
        self::assertLessThan(2, $now);
    }

    public function testShutdownDuringCapacityBackoffDoesNotPollAnotherKind(): void
    {
        $now = 0.0;
        $worker = null;
        $transport = new FakeTransport([self::capacityRefusal('workflow')]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, &$worker): void {
                $now += $microseconds / 1_000_000;
                $worker->requestShutdown();
            },
        );

        self::assertFalse($worker->tick(5));
        self::assertCount(1, $transport->requests);
        self::assertStringEndsWith('/workflow-tasks/poll', $transport->requests[0]['uri']);
        self::assertGreaterThan(0, $now);
        self::assertLessThan(1, $now);
    }

    private static function capacityRefusal(string $kind): TransportException
    {
        $response = [
            'task' => null, 'poll_status' => 'long_poll_capacity_exhausted',
            'reason' => 'long_poll_capacity_exhausted', 'retryable' => true,
            'retry_after_seconds' => 1, 'task_kind' => $kind.'_task',
        ];

        return TransportException::fromResponse(429, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }
}
