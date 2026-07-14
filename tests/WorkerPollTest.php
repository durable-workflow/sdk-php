<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerPollTest extends TestCase
{
    /**
     * @param list<array<string, mixed>> $responses
     */
    #[DataProvider('stalePollResponses')]
    public function testStalePollStopsEveryTaskKind(array $responses, int $expectedRequestCount): void
    {
        $transport = new FakeTransport($responses);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );

        self::assertFalse($worker->tick(0));
        self::assertCount($expectedRequestCount, $transport->requests);

        self::assertFalse($worker->tick(0));
        self::assertCount($expectedRequestCount, $transport->requests);
    }

    /** @return iterable<string, array{list<array<string, mixed>>, int}> */
    public static function stalePollResponses(): iterable
    {
        $empty = ['task' => null, 'poll_status' => 'empty'];
        $stale = [
            'task' => null,
            'poll_status' => 'stale_worker_registration',
            'reason' => 'worker_heartbeat_stale',
        ];

        yield 'workflow poll' => [[$stale], 1];
        yield 'activity poll' => [[$empty, $stale], 2];
        yield 'query poll' => [[$empty, $empty, $stale], 3];
    }

    public function testConflictDrainResponseStopsTheWorker(): void
    {
        $response = [
            'task' => null,
            'poll_status' => 'draining',
            'reason' => 'worker_draining',
            'worker_status' => 'draining',
        ];
        $transport = new FakeTransport([
            TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR)),
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );

        self::assertFalse($worker->tick(0));
        self::assertCount(1, $transport->requests);
    }

    public function testOrdinaryEmptyPollsRemainIdle(): void
    {
        $transport = new FakeTransport([
            ['task' => null, 'poll_status' => 'timeout'],
            ['task' => null, 'poll_status' => 'empty'],
            ['task' => null, 'poll_status' => 'workflow_task_pending'],
            ['task' => null, 'poll_status' => 'empty'],
            ['task' => null, 'poll_status' => 'timeout'],
            ['task' => null, 'poll_status' => 'empty'],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );

        self::assertFalse($worker->tick(0));
        self::assertFalse($worker->tick(0));
        self::assertCount(6, $transport->requests);
    }
}
