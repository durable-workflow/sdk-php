<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerWorkflowTaskLeaseRetryTest extends TestCase
{
    public function testTransientLeaseRenewalThenSuccessExecutesAndCompletesExactlyOnce(): void
    {
        $now = 0.0;
        $heartbeatBodies = [];
        $handlerCalls = 0;
        $completions = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$heartbeatBodies, &$completions): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return self::leasedWorkflowTask();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/heartbeat')) {
                $heartbeatBodies[] = $body;

                return count($heartbeatBodies) === 1
                    ? self::transientLeaseRefusal()
                    : self::renewedLease();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/complete')) {
                ++$completions;

                return ['completed' => true];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
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
        $worker->registerWorkflow(
            'orders.workflow',
            static function (WorkflowContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'completed';
            },
        );

        self::assertTrue($worker->tick(0));

        self::assertSame(1, $handlerCalls);
        self::assertSame(1, $completions);
        self::assertCount(2, $heartbeatBodies);
        self::assertSame($heartbeatBodies[0], $heartbeatBodies[1]);
        self::assertSame([
            'lease_owner' => 'worker-1',
            'workflow_task_attempt' => 3,
        ], $heartbeatBodies[0]);
        self::assertEqualsWithDelta(1.0, $now, 0.000_01);
    }

    public function testTerminalRunClosureDuringLeaseRetryDiscardsTaskWithoutExecutionOrCompletion(): void
    {
        $handlerCalls = 0;
        $heartbeatCalls = 0;
        $taskAcknowledgements = 0;
        $now = 0.0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$heartbeatCalls, &$taskAcknowledgements): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return self::leasedWorkflowTask();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/heartbeat')) {
                ++$heartbeatCalls;
                if ($heartbeatCalls === 1) {
                    return self::transientLeaseRefusal();
                }

                throw self::runClosedConflict();
            }

            if (str_contains($uri, '/api/worker/workflow-tasks/workflow-task-1/complete')
                || str_contains($uri, '/api/worker/workflow-tasks/workflow-task-1/fail')) {
                ++$taskAcknowledgements;
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
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
        $worker->registerWorkflow(
            'orders.workflow',
            static function (WorkflowContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'must-not-run';
            },
        );

        self::assertTrue($worker->tick(0));

        self::assertSame(2, $heartbeatCalls);
        self::assertSame(0, $handlerCalls);
        self::assertSame(0, $taskAcknowledgements);
    }

    public function testLeaseLossDuringRetryRemainsFatalWithoutExecutingTheTask(): void
    {
        $handlerCalls = 0;
        $heartbeatCalls = 0;
        $now = 0.0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$heartbeatCalls): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return self::leasedWorkflowTask();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/heartbeat')) {
                ++$heartbeatCalls;
                if ($heartbeatCalls === 1) {
                    return self::transientLeaseRefusal();
                }

                throw self::leaseExpiredConflict();
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
        $worker->registerWorkflow(
            'orders.workflow',
            static function (WorkflowContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'must-not-run';
            },
        );

        try {
            $worker->tick(0);
            self::fail('Lease loss must remain visible to the managed worker supervisor.');
        } catch (ServerException $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('lease_expired', $exception->reason);
        }

        self::assertSame(2, $heartbeatCalls);
        self::assertSame(0, $handlerCalls);
        self::assertCount(3, $transport->requests);
    }

    public function testShutdownInterruptsLeaseRenewalBackoffBeforeTaskExecution(): void
    {
        $now = 0.0;
        $handlerCalls = 0;
        $worker = null;
        $transport = new FakeTransport([
            self::leasedWorkflowTask(),
            self::transientLeaseRefusal(5),
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now, &$worker): void {
                $now += $microseconds / 1_000_000;
                $worker?->requestShutdown();
            },
        );
        $worker->registerWorkflow(
            'orders.workflow',
            static function (WorkflowContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'must-not-run';
            },
        );

        self::assertTrue($worker->tick(0));

        self::assertSame(0, $handlerCalls);
        self::assertCount(2, $transport->requests);
        self::assertEqualsWithDelta(0.1, $now, 0.000_01);
    }

    #[DataProvider('invalidLeaseResponseProvider')]
    public function testNonRetryableAndMalformedLeaseResponsesRemainFatal(array $leaseResponse): void
    {
        $handlerCalls = 0;
        $transport = new FakeTransport([
            self::leasedWorkflowTask(),
            $leaseResponse,
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'orders',
            workerId: 'worker-1',
        );
        $worker->registerWorkflow(
            'orders.workflow',
            static function (WorkflowContext $context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'must-not-run';
            },
        );

        try {
            $worker->tick(0);
            self::fail('The lease response must remain fatal.');
        } catch (ServerException $exception) {
            self::assertSame(200, $exception->status);
        }

        self::assertSame(0, $handlerCalls);
        self::assertCount(2, $transport->requests);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function invalidLeaseResponseProvider(): iterable
    {
        yield 'explicitly non-retryable refusal' => [[
            ...self::leaseFence(),
            'renewed' => false,
            'reason' => 'lease_renewal_refused',
            'retryable' => false,
        ]];

        yield 'malformed retry delay' => [[
            ...self::transientLeaseRefusal(),
            'retry_after_seconds' => 'soon',
        ]];

        yield 'mismatched successful fence' => [[
            ...self::renewedLease(),
            'workflow_task_attempt' => 4,
        ]];

        yield 'missing renewal outcome' => [self::leaseFence()];
    }

    /** @return array{poll_status: string, task: array<string, mixed>} */
    private static function leasedWorkflowTask(): array
    {
        return [
            'poll_status' => 'leased',
            'task' => [
                'task_id' => 'workflow-task-1',
                'workflow_task_attempt' => 3,
                'lease_owner' => 'worker-1',
                'workflow_id' => 'workflow-1',
                'run_id' => 'run-1',
                'workflow_type' => 'orders.workflow',
                'history_events' => [],
            ],
        ];
    }

    /** @return array<string, mixed> */
    private static function transientLeaseRefusal(int $retryAfterSeconds = 1): array
    {
        return [
            ...self::leaseFence(),
            'renewed' => false,
            'lease_expires_at' => null,
            'run_status' => null,
            'task_status' => null,
            'reason' => 'backend_lock_pressure',
            'retryable' => true,
            'retry_after_seconds' => $retryAfterSeconds,
            'backend' => ['driver' => 'sqlite', 'lock_pressure' => true],
        ];
    }

    /** @return array<string, mixed> */
    private static function renewedLease(): array
    {
        return [
            ...self::leaseFence(),
            'renewed' => true,
            'lease_expires_at' => '2026-07-18T00:40:00Z',
            'run_status' => 'running',
            'task_status' => 'leased',
            'reason' => null,
        ];
    }

    /** @return array{task_id: string, workflow_task_attempt: int, lease_owner: string} */
    private static function leaseFence(): array
    {
        return [
            'task_id' => 'workflow-task-1',
            'workflow_task_attempt' => 3,
            'lease_owner' => 'worker-1',
        ];
    }

    private static function runClosedConflict(): TransportException
    {
        $response = [
            ...self::leaseFence(),
            'reason' => 'run_closed',
            'can_continue' => false,
            'run_status' => 'cancelled',
            'task_status' => 'cancelled',
        ];

        return self::failure(409, $response);
    }

    private static function leaseExpiredConflict(): TransportException
    {
        return self::failure(409, [
            ...self::leaseFence(),
            'reason' => 'lease_expired',
            'task_status' => 'leased',
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
