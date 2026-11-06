<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;

final class WorkerWorkflowTaskLeaseRetryTest extends TestCase
{
    #[DataProvider('workerWallClocks')]
    public function testFreshWorkflowPollCompletesWithItsOriginalFenceWithoutInitialRenewal(float $wallClock): void
    {
        $transport = self::grantTransport(self::freshWorkflowPoll());
        $worker = self::grantWorker($transport, $wallClock);

        self::assertTrue($worker->tick(0));
        self::assertSame([], self::taskRequests($transport, 'heartbeat'));
        $completions = self::taskRequests($transport, 'complete');
        self::assertCount(1, $completions);
        self::assertSame('worker-1', $completions[0]['body']['lease_owner']);
        self::assertSame(3, $completions[0]['body']['workflow_task_attempt']);
        self::assertSame('complete_workflow', $completions[0]['body']['commands'][0]['type']);
    }

    /** @return iterable<string, array{float}> */
    public static function workerWallClocks(): iterable
    {
        yield 'worker clock behind Server' => [0.0];
        yield 'worker clock ahead of Server' => [4_000_000_000.0];
    }

    /** @param array<string, mixed> $poll */
    #[DataProvider('unqualifiedWorkflowPolls')]
    public function testUnqualifiedWorkflowPollRetainsInitialRenewal(array $poll): void
    {
        $transport = self::grantTransport($poll);

        self::assertTrue(self::grantWorker($transport)->tick(0));
        self::assertCount(1, self::taskRequests($transport, 'heartbeat'));
        self::assertCount(1, self::taskRequests($transport, 'complete'));
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function unqualifiedWorkflowPolls(): iterable
    {
        yield 'older Server' => [self::leasedWorkflowTask()];
        foreach ([null, 123, 'not-a-clock', '2026-07-18T00:00:00Z',
            '2026-02-30T00:00:00.000000Z', "2026-07-18T00:00:00.000000Z\n"] as $index => $clock) {
            yield 'invalid Server clock '.$index => [array_replace(self::freshWorkflowPoll(), ['server_time' => $clock])];
        }
        foreach ([null, '2026-07-18T00:00:09.999999Z', '2026-07-18T00:00:00.000000Z',
            '2026-07-17T23:59:59.000000Z', '2026-02-30T00:00:30.000000Z'] as $index => $expiry) {
            $poll = self::freshWorkflowPoll();
            $poll['task']['lease_expires_at'] = $expiry;
            yield 'unusable lease '.$index => [$poll];
        }
    }

    public function testHistoryPagingTimeMakesThePollObservationStale(): void
    {
        $poll = self::freshWorkflowPoll();
        $poll['task']['next_history_page_token'] = 'history-page';
        $transport = self::grantTransport($poll, history: static function (): array {
            usleep(1_100_000);

            return ['history_events' => [], 'next_history_page_token' => null];
        });

        self::assertTrue(self::grantWorker($transport)->tick(0));
        self::assertCount(1, self::taskRequests($transport, 'heartbeat'));
        self::assertCount(1, self::taskRequests($transport, 'complete'));
    }

    public function testPollRoundTripConsumesTheRemainingServerLeaseMargin(): void
    {
        $poll = self::freshWorkflowPoll();
        $poll['task']['lease_expires_at'] = '2026-07-18T00:00:10.050000Z';
        $transport = self::grantTransport($poll, beforePoll: static function (): void {
            usleep(120_000);
        });

        self::assertTrue(self::grantWorker($transport)->tick(0));
        self::assertCount(1, self::taskRequests($transport, 'heartbeat'));
    }

    public function testFreshPollCompletionStillObeysTheServerFence(): void
    {
        $transport = self::grantTransport(self::freshWorkflowPoll(), completion: static function (): never {
            throw self::leaseExpiredConflict();
        });

        self::assertTrue(self::grantWorker($transport)->tick(0));
        self::assertSame([], self::taskRequests($transport, 'heartbeat'));
        self::assertCount(1, self::taskRequests($transport, 'complete'));
        self::assertSame([], self::taskRequests($transport, 'fail'));
    }

    public function testFreshPollDoesNotSkipLocalActivityProgressRenewal(): void
    {
        $transport = self::grantTransport(self::freshWorkflowPoll());
        $worker = self::grantWorker($transport, handler: static fn (WorkflowContext $context): mixed =>
            $context->localActivity('local.receipt'));
        $worker->registerActivity('local.receipt', static function (ActivityContext $context): string {
            $context->heartbeat(['step' => 1]);

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        $heartbeats = self::taskRequests($transport, 'heartbeat');
        self::assertCount(1, $heartbeats);
        self::assertSame(['lease_owner' => 'worker-1', 'workflow_task_attempt' => 3], $heartbeats[0]['body']);
        $completion = self::taskRequests($transport, 'complete');
        self::assertSame('record_local_activity', $completion[0]['body']['commands'][0]['type']);
    }

    public function testShutdownDuringHistoryLoadingCannotUseTheFreshGrant(): void
    {
        $poll = self::freshWorkflowPoll();
        $poll['task']['next_history_page_token'] = 'history-page';
        $worker = null;
        $executions = 0;
        $transport = self::grantTransport($poll, history: static function () use (&$worker): array {
            $worker->requestShutdown();

            return ['history_events' => [], 'next_history_page_token' => null];
        });
        $worker = self::grantWorker($transport, handler: static function (WorkflowContext $context) use (&$executions): string {
            ++$executions;

            return 'must-not-run';
        });

        self::assertTrue($worker->tick(0));
        self::assertSame(0, $executions);
        self::assertSame([], self::taskRequests($transport, 'heartbeat'));
        self::assertSame([], self::taskRequests($transport, 'complete'));
    }

    public function testExternalPayloadLoadingCannotRefreshThePollObservation(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['loaded'])['blob'];
        $events = [];
        $http = new HttpClient(['handler' => static function (RequestInterface $request) use ($blob, &$events): FulfilledPromise {
            $path = $request->getUri()->getPath();
            if (str_contains($path, '/external-payloads/')) {
                $events[] = 'fetch';
                usleep(1_100_000);

                return new FulfilledPromise(new Response(200, [
                    'Content-Type' => 'application/octet-stream',
                    'X-Durable-Workflow-Payload-Codec' => 'avro',
                    'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                    'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
                ], $blob));
            }
            if (str_ends_with($path, '/workflow-tasks/poll')) {
                $reply = self::freshWorkflowPoll();
                $reply['task']['arguments'] = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
            } elseif (str_ends_with($path, '/heartbeat')) {
                $events[] = 'heartbeat';
                $reply = self::renewedLease();
            } elseif (str_ends_with($path, '/complete')) {
                $events[] = 'complete';
                $reply = ['completed' => true];
            } else {
                $reply = ['task' => null, 'poll_status' => 'empty'];
            }

            return new FulfilledPromise(new Response(200, [], json_encode($reply, JSON_THROW_ON_ERROR)));
        }]);
        $worker = new Worker(new Client('https://server.example', transport: new Psr18Transport($http)),
            'orders', workerId: 'worker-1');
        $worker->registerWorkflow('orders.workflow', static function (WorkflowContext $context, string $input) use (&$events): string {
            self::assertSame('loaded', $input);
            $events[] = 'handler';

            return 'completed';
        });

        self::assertTrue($worker->tick(0));
        self::assertSame(['heartbeat', 'handler', 'complete'], array_slice($events, -3));
    }

    /** @return array<string, mixed> */
    private static function freshWorkflowPoll(): array
    {
        $poll = self::leasedWorkflowTask();
        $poll['server_time'] = '2026-07-18T00:00:00.000000Z';
        $poll['task']['lease_expires_at'] = '2026-07-18T00:00:30.000000Z';

        return $poll;
    }

    /** @param array<string, mixed> $poll */
    private static function grantTransport(
        array $poll,
        ?\Closure $beforePoll = null,
        ?\Closure $history = null,
        ?\Closure $completion = null,
    ): FakeTransport {
        return new FakeTransport(handler: static function (string $method, string $uri) use (
            $poll, $beforePoll, $history, $completion,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $beforePoll?->__invoke();

                return $poll;
            }
            if (str_ends_with($uri, '/workflow-task-1/heartbeat')) {
                return self::renewedLease();
            }
            if (str_ends_with($uri, '/workflow-task-1/history') && $history !== null) {
                return $history();
            }
            if (str_ends_with($uri, '/workflow-task-1/complete')) {
                return $completion === null ? ['completed' => true] : $completion();
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
    }

    private static function grantWorker(FakeTransport $transport, float $wallClock = 0.0, ?\Closure $handler = null): Worker
    {
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders',
            workerId: 'worker-1', clock: static fn (): float => $wallClock);

        return $worker->registerWorkflow('orders.workflow', $handler ?? static fn (WorkflowContext $context): string => 'completed');
    }

    /** @return list<array{method: string, uri: string, headers: array<string, string>, body: ?array<string, mixed>}> */
    private static function taskRequests(FakeTransport $transport, string $operation): array
    {
        return array_values(array_filter($transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/workflow-task-1/'.$operation)));
    }

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

    public function testBackendLossRetriesTheSameWorkflowTaskHeartbeatFence(): void
    {
        $now = 0.0;
        $heartbeats = [];
        $completions = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$heartbeats, &$completions): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return self::leasedWorkflowTask();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/heartbeat')) {
                $heartbeats[] = $body;

                return count($heartbeats) === 1
                    ? throw self::failure(503, [
                        ...self::leaseFence(),
                        'reason' => 'backend_unavailable',
                        'operation' => 'heartbeat_workflow_task',
                        'outcome' => 'unknown',
                        'worker_id' => 'worker-1',
                        'task_queue' => null,
                        'retryable' => true,
                        'retry_after_seconds' => 1,
                    ])
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
        $worker->registerWorkflow('orders.workflow', static fn (WorkflowContext $context): string => 'completed');

        self::assertTrue($worker->tick(0));
        self::assertCount(2, $heartbeats);
        self::assertSame($heartbeats[0], $heartbeats[1]);
        self::assertSame(1, $completions);
        self::assertEqualsWithDelta(1.0, $now, 0.000_01);
    }

    public function testBackendLossHeartbeatResponseMustMatchTheSubmittedFence(): void
    {
        $response = [
            ...self::leaseFence(),
            'reason' => 'backend_unavailable',
            'operation' => 'heartbeat_workflow_task',
            'outcome' => 'unknown',
            'worker_id' => 'worker-1',
            'task_queue' => null,
            'retryable' => true,
            'retry_after_seconds' => 1,
        ];
        $error = new ServerException('Backend unavailable', 503, 'backend_unavailable', $response);

        self::assertTrue($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 3));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('other-task', 'worker-1', 3));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'other-worker', 3));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 4));
        $response['task_queue'] = 'orders';
        self::assertFalse((new ServerException('Backend unavailable', 503, 'backend_unavailable', $response))
            ->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 3));
    }

    #[DataProvider('completionOutcomes')]
    public function testBackendLossRetriesTheSameWorkflowCompletionWithoutReexecuting(
        bool $completedBeforeTheRetry,
    ): void {
        $now = 0.0;
        $handlerCalls = 0;
        $completionBodies = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$completionBodies, $completedBeforeTheRetry): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return self::leasedWorkflowTask();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/heartbeat')) {
                return self::renewedLease();
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/workflow-task-1/complete')) {
                $completionBodies[] = $body;
                if (count($completionBodies) === 1) {
                    throw self::failure(503, [
                        ...self::leaseFence(),
                        'reason' => 'backend_unavailable',
                        'operation' => 'complete_workflow_task',
                        'outcome' => 'unknown',
                        'worker_id' => 'worker-1',
                        'task_queue' => null,
                        'retryable' => true,
                        'retry_after_seconds' => 1,
                    ]);
                }

                return $completedBeforeTheRetry
                    ? throw self::failure(409, [
                        ...self::leaseFence(),
                        'reason' => 'task_not_leased',
                        'task_status' => 'completed',
                    ])
                    : ['completed' => true];
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
        $worker->registerWorkflow('orders.workflow', static function (WorkflowContext $context) use (&$handlerCalls): string {
            ++$handlerCalls;

            return 'completed';
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
        yield 'committed before the unknown response' => [true];
    }

    public function testCompletionBackendLossResponseMustMatchTheSubmittedFence(): void
    {
        $response = [
            ...self::leaseFence(),
            'reason' => 'backend_unavailable',
            'operation' => 'complete_workflow_task',
            'outcome' => 'unknown',
            'worker_id' => 'worker-1',
            'task_queue' => null,
            'retryable' => true,
            'retry_after_seconds' => 1,
        ];
        $error = new ServerException('Backend unavailable', 503, 'backend_unavailable', $response);

        self::assertTrue($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 3, 'complete_workflow_task'));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 3));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 4, 'complete_workflow_task'));
        self::assertFalse($error->isWorkflowTaskBackendUnavailable('workflow-task-1', 'worker-1', 3, 'fail_workflow_task'));
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

    public function testLeaseLossDuringRetryDiscardsTheClaimWithoutExecutingTheTask(): void
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

            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
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
        self::assertCount(5, $transport->requests);
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
                'payload_codec' => 'avro',
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
