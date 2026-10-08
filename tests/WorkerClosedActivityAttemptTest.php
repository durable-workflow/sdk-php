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

final class WorkerClosedActivityAttemptTest extends TestCase
{
    #[DataProvider('lateOutcomes')]
    public function testClosedAttemptIsDiscardedAndTheSameWorkerTakesAnotherTask(bool $failure): void
    {
        $calls = 0;
        $diagnostics = [];
        $transport = $this->transport(self::closedAttempt(), $failure);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders',
            workerId: 'worker-1', clock: static fn (): float => 0,
            diagnosticListener: static function (string $name, array $context) use (&$diagnostics): void {
                $diagnostics[] = compact('name', 'context');
            });
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$calls, $failure): string {
            if (++$calls === 1 && $failure) {
                throw new \RuntimeException('Late application failure');
            }

            return $calls === 1 ? 'late result' : 'next result';
        });

        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        self::assertSame(2, $calls);
        $outcomes = array_values(array_filter($transport->requests,
            static fn (array $request): bool => preg_match('~/activity-tasks/activity-[12]/(complete|fail)$~', $request['uri']) === 1));
        self::assertCount(2, $outcomes);
        self::assertStringEndsWith('/activity-1/'.($failure ? 'fail' : 'complete'), $outcomes[0]['uri']);
        self::assertStringEndsWith('/activity-2/complete', $outcomes[1]['uri']);
        self::assertSame('attempt-1', $outcomes[0]['body']['activity_attempt_id']);
        self::assertSame('attempt-2', $outcomes[1]['body']['activity_attempt_id']);
        $aborted = array_values(array_filter($diagnostics,
            static fn (array $event): bool => $event['name'] === 'worker.claim_aborted'));
        self::assertCount(1, $aborted);
        self::assertSame('activity-1', $aborted[0]['context']['task_id']);
        self::assertSame('attempt-1', $aborted[0]['context']['activity_attempt_id']);
        self::assertSame('stale_attempt', $aborted[0]['context']['reason']);
        self::assertSame(0, count(array_filter($transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/activity-1/status'))));
    }

    /** @return iterable<string, array{bool}> */
    public static function lateOutcomes(): iterable
    {
        yield 'late result' => [false];
        yield 'late application failure' => [true];
    }

    #[DataProvider('invalidProofs')]
    public function testUnprovedConflictRemainsAnError(string $field, mixed $value, bool $failure): void
    {
        $proof = self::closedAttempt();
        $proof[$field] = $value;
        $transport = $this->transport($proof, $failure);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders',
            workerId: 'worker-1', clock: static fn (): float => 0);
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use ($failure): string {
            if ($failure) {
                throw new \RuntimeException('Late application failure');
            }

            return 'late result';
        });

        $this->expectException(ServerException::class);
        $worker->tick(0);
    }

    /** @return iterable<string, array{string, mixed, bool}> */
    public static function invalidProofs(): iterable
    {
        foreach ([
            'task_id' => 'another-task', 'activity_attempt_id' => 'another-attempt',
            'lease_owner' => 'another-worker', 'attempt_status' => 'running',
            'activity_status' => 'completed', 'task_status' => 'leased',
        ] as $field => $value) {
            yield 'completion '.$field => [$field, $value, false];
        }
        foreach (['task_id' => 'another-task', 'activity_attempt_id' => 'another-attempt',
            'reason' => 'lease_owner_mismatch', 'recorded' => true] as $field => $value) {
            yield 'failure '.$field => [$field, $value, true];
        }
        yield 'completion grants authority' => ['can_continue', true, false];
        yield 'completion records a heartbeat' => ['heartbeat_recorded', true, false];
        yield 'completion lacks attempt state' => ['attempt_status', null, false];
        yield 'completion lacks worker identity' => ['lease_owner', null, false];
        yield 'completion was recorded' => ['recorded', true, false];
        yield 'completion has another reason' => ['reason', 'lease_owner_mismatch', false];
    }

    /** @param array<string, mixed> $proof */
    private function transport(array $proof, bool $failure): FakeTransport
    {
        $polls = 0;

        return new FakeTransport(handler: static function (string $method, string $uri) use (&$polls, $proof, $failure): ?array {
            if (str_ends_with($uri, '/workflow-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                $number = ++$polls;

                return ['task' => ['task_id' => 'activity-'.$number, 'activity_attempt_id' => 'attempt-'.$number,
                    'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/activity-1/'.($failure ? 'fail' : 'complete'))) {
                $response = $failure ? array_intersect_key($proof,
                    array_flip(['task_id', 'activity_attempt_id', 'reason', 'recorded'])) + ['outcome' => 'failed'] : $proof;
                throw TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR));
            }
            if (str_ends_with($uri, '/activity-2/complete')) {
                return ['recorded' => true, 'outcome' => 'completed'];
            }

            throw new \LogicException('Unexpected request: '.$method.' '.$uri);
        });
    }

    /** @return array<string, mixed> */
    private static function closedAttempt(): array
    {
        return ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1',
            'reason' => 'stale_attempt', 'outcome' => 'completed', 'recorded' => false,
            'attempt_status' => 'failed', 'activity_status' => 'failed', 'task_status' => 'cancelled',
            'can_continue' => false, 'heartbeat_recorded' => false];
    }
}
