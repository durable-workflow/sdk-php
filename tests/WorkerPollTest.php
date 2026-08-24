<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

final class WorkerPollTest extends TestCase
{
    public function testSagaCompensationFailureReachesFrameworkLoggerAndEventDiagnostics(): void
    {
        $codec = new AvroPayloadCodec();
        $commands = null;
        $diagnostics = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$commands, $codec): array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return [
                    'poll_status' => 'leased',
                    'task' => [
                        'task_id' => 'saga-task-1',
                        'workflow_task_attempt' => 1,
                        'lease_owner' => 'worker-1',
                        'workflow_id' => 'trip-1',
                        'run_id' => 'run-1',
                        'workflow_type' => 'trip.book',
                        'payload_codec' => 'avro',
                        'history_events' => [
                            [
                                'event_type' => 'ActivityCompleted',
                                'payload' => [
                                    'sequence' => 1,
                                    'activity_type' => 'trip.reserve-flight',
                                    'result' => $codec->envelope('flight-1'),
                                ],
                            ],
                            [
                                'event_type' => 'ActivityFailed',
                                'payload' => [
                                    'sequence' => 2,
                                    'activity_type' => 'trip.charge',
                                    'message' => 'card declined',
                                    'exception_type' => 'CardDeclined',
                                    'non_retryable' => true,
                                ],
                            ],
                            [
                                'event_type' => 'ActivityFailed',
                                'payload' => [
                                    'sequence' => 3,
                                    'activity_type' => 'trip.cancel-flight',
                                    'message' => 'cancellation rejected',
                                    'exception_type' => 'CancellationRejected',
                                    'non_retryable' => true,
                                ],
                            ],
                        ],
                    ],
                ];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/saga-task-1/heartbeat')) {
                return [
                    'task_id' => 'saga-task-1',
                    'workflow_task_attempt' => 1,
                    'lease_owner' => 'worker-1',
                    'renewed' => true,
                ];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/saga-task-1/complete')) {
                $commands = $body['commands'] ?? null;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $logger = new SagaRecordingLogger();
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'trip-workers',
            workerId: 'worker-1',
            logger: $logger,
            diagnosticListener: static function (string $name, array $context) use (&$diagnostics): void {
                $diagnostics[] = ['name' => $name, 'context' => $context];
            },
        );
        $worker->registerWorkflow('trip.book', static function (WorkflowContext $context): mixed {
            return $context->saga()->run(static function (Saga $saga) use ($context): mixed {
                $flight = $context->activity('trip.reserve-flight');
                $saga->addCompensation('trip.cancel-flight', [$flight]);

                return $context->activity('trip.charge');
            });
        });

        $worker->tick(0);

        self::assertSame('fail_workflow', $commands[0]['type'] ?? null);
        self::assertSame(
            'DurableWorkflow\\Exception\\SagaCompensationFailed',
            $commands[0]['exception_type'] ?? null,
        );
        $event = array_values(array_filter(
            $diagnostics,
            static fn (array $entry): bool => $entry['name'] === 'worker.handler_failed',
        ))[0] ?? null;
        $eventSagaFailure = is_array($event)
            ? ($event['context']['saga_failure'] ?? null)
            : null;
        self::assertSame([
            'forward_step' => 'trip.charge',
            'forward_exception_type' => 'DurableWorkflow\\Exception\\ActivityFailed',
            'forward_message' => 'card declined',
            'compensation_activity_type' => 'trip.cancel-flight',
            'compensation_registration_order' => 1,
            'compensation_exception_type' => 'CancellationRejected',
            'compensation_message' => 'cancellation rejected',
        ], $eventSagaFailure);
        $log = array_values(array_filter(
            $logger->records,
            static fn (array $entry): bool => $entry['message'] === 'worker.handler_failed',
        ))[0] ?? null;
        $logSagaFailure = is_array($log)
            ? ($log['context']['saga_failure'] ?? null)
            : null;
        self::assertSame($eventSagaFailure, $logSagaFailure);
    }

    public function testConditionWaitUsesItsProtocolCommandAndDedicatedDiagnosticAfterLeaseRenewal(): void
    {
        $commands = null;
        $diagnostics = [];
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$commands): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return [
                    'poll_status' => 'leased',
                    'task' => [
                        'task_id' => 'condition-task-1',
                        'workflow_task_attempt' => 2,
                        'lease_owner' => 'worker-1',
                        'workflow_id' => 'approval-workflow-1',
                        'run_id' => 'approval-run-1',
                        'workflow_type' => 'approval.workflow',
                        'payload_codec' => 'avro',
                        'history_events' => [],
                    ],
                ];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/condition-task-1/heartbeat')) {
                return [
                    'task_id' => 'condition-task-1',
                    'workflow_task_attempt' => 2,
                    'lease_owner' => 'worker-1',
                    'renewed' => true,
                ];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/condition-task-1/complete')) {
                $commands = $body['commands'] ?? null;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'approvals',
            workerId: 'worker-1',
            diagnosticListener: static function (string $name, array $context) use (&$diagnostics): void {
                $diagnostics[] = ['name' => $name, 'context' => $context];
            },
        );
        $worker->registerWorkflow(
            'approval.workflow',
            static fn (WorkflowContext $context): bool => $context->waitCondition(
                static fn (): bool => false,
                key: 'approval.ready',
                timeout: 45,
            ),
        );

        self::assertTrue($worker->tick(0));

        self::assertSame('open_condition_wait', $commands[0]['type'] ?? null);
        self::assertArrayNotHasKey('activity_type', $commands[0]);
        self::assertSame('worker.workflow_waiting', $diagnostics[0]['name'] ?? null);
        self::assertSame([
            'worker_id' => 'worker-1',
            'workflow_id' => 'approval-workflow-1',
            'run_id' => 'approval-run-1',
            'task_id' => 'condition-task-1',
            'wait_kind' => 'condition',
            'condition_key' => 'approval.ready',
            'condition_definition_fingerprint' => $commands[0]['condition_definition_fingerprint'],
            'timeout_seconds' => 45,
        ], $diagnostics[0]['context']);
    }

    public function testRunAdvertisesHandlerDerivedContractsForEveryWorkflow(): void
    {
        $transport = new FakeTransport([
            ['registered' => true],
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );
        $worker
            ->registerWorkflow(
                'orders.process',
                static fn (WorkflowContext $context): string => 'complete',
            )
            ->registerQuery(
                'orders.process',
                'order-summary',
                static fn (QueryContext $context): array => [],
            )
            ->declareSignal(
                'orders.process',
                'order-approved',
                static fn (int $amount, ?string $source = null): mixed => null,
            )
            ->registerUpdate(
                'orders.process',
                'approve-order',
                static fn (
                    QueryContext $context,
                    int $amount,
                    string $source = 'manual',
                    ?bool $approved = null,
                    string ...$tags,
                ): bool => true,
            )
            ->registerWorkflow(
                'inventory.audit',
                static fn (WorkflowContext $context): string => 'complete',
            );

        $worker->run(0);

        self::assertSame(
            ['orders.process', 'inventory.audit'],
            $transport->requests[0]['body']['supported_workflow_types'] ?? null,
        );
        self::assertSame([
            'orders.process' => [
                'queries' => ['order-summary'],
                'query_contracts' => [[
                    'name' => 'order-summary',
                    'parameters' => [],
                ]],
                'signals' => ['order-approved'],
                'signal_contracts' => [[
                    'name' => 'order-approved',
                    'parameters' => [
                        [
                            'name' => 'amount',
                            'position' => 0,
                            'required' => true,
                            'variadic' => false,
                            'default_available' => false,
                            'default' => null,
                            'type' => 'int',
                            'allows_null' => false,
                        ],
                        [
                            'name' => 'source',
                            'position' => 1,
                            'required' => false,
                            'variadic' => false,
                            'default_available' => true,
                            'default' => null,
                            'type' => '?string',
                            'allows_null' => true,
                        ],
                    ],
                ]],
                'updates' => ['approve-order'],
                'update_contracts' => [[
                    'name' => 'approve-order',
                    'parameters' => [
                        [
                            'name' => 'amount',
                            'position' => 0,
                            'required' => true,
                            'variadic' => false,
                            'default_available' => false,
                            'default' => null,
                            'type' => 'int',
                            'allows_null' => false,
                        ],
                        [
                            'name' => 'source',
                            'position' => 1,
                            'required' => false,
                            'variadic' => false,
                            'default_available' => true,
                            'default' => 'manual',
                            'type' => 'string',
                            'allows_null' => false,
                        ],
                        [
                            'name' => 'approved',
                            'position' => 2,
                            'required' => false,
                            'variadic' => false,
                            'default_available' => true,
                            'default' => null,
                            'type' => '?bool',
                            'allows_null' => true,
                        ],
                        [
                            'name' => 'tags',
                            'position' => 3,
                            'required' => false,
                            'variadic' => true,
                            'default_available' => false,
                            'default' => null,
                            'type' => 'string',
                            'allows_null' => false,
                        ],
                    ],
                ]],
                'update_validators' => [],
            ],
            'inventory.audit' => [
                'queries' => [],
                'query_contracts' => [],
                'signals' => [],
                'signal_contracts' => [],
                'updates' => [],
                'update_contracts' => [],
                'update_validators' => [],
            ],
        ], $transport->requests[0]['body']['workflow_command_contracts'] ?? null);
    }

    public function testDuplicateSignalDeclarationsFailLocally(): void
    {
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'queue',
        );
        $worker->declareSignal('orders.process', 'approve');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Duplicate signal registration: approve.');

        $worker->declareSignal('orders.process', 'approve');
    }

    public function testRuntimeMessageStreamTransportCannotBeDeclaredAsAUserSignal(): void
    {
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'queue',
        );

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('reserved by the workflow runtime');

        $worker->declareSignal('orders.process', WorkflowContext::MESSAGE_STREAM_SIGNAL);
    }

    #[DataProvider('invalidSignalDeclarations')]
    public function testInvalidSignalDeclarationsFailLocally(
        string $workflowType,
        string $signalName,
        string $expectedKind,
    ): void {
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'queue',
        );

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("Signal declaration {$expectedKind} must be non-empty without surrounding whitespace.");

        $worker->declareSignal($workflowType, $signalName);
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function invalidSignalDeclarations(): iterable
    {
        yield 'empty workflow type' => ['', 'approve', 'workflow type'];
        yield 'workflow type whitespace' => [' orders.process', 'approve', 'workflow type'];
        yield 'empty signal name' => ['orders.process', '', 'signal'];
        yield 'signal name whitespace' => ['orders.process', 'approve ', 'signal'];
    }

    public function testRegisteredUpdateHandlerCompletesAWorkerUpdateTask(): void
    {
        $completedCommand = null;
        $codecClient = new Client('https://server.example', transport: new FakeTransport());
        $arguments = $codecClient->payloadCodec()->envelope([41]);
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$completedCommand, $arguments): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return [
                    'poll_status' => 'leased',
                    'task' => [
                        'task_id' => 'update-task-1',
                        'workflow_task_attempt' => 1,
                        'lease_owner' => 'worker-1',
                        'workflow_id' => 'counter-1',
                        'run_id' => 'run-1',
                        'workflow_type' => 'counter',
                        'payload_codec' => 'avro',
                        'workflow_update_id' => 'update-1',
                        'history_events' => [[
                            'event_type' => 'UpdateAccepted',
                            'payload' => [
                                'update_id' => 'update-1',
                                'update_name' => 'increment',
                                'arguments' => $arguments,
                            ],
                        ]],
                    ],
                ];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/update-task-1/heartbeat')) {
                return [
                    'task_id' => 'update-task-1',
                    'workflow_task_attempt' => 1,
                    'lease_owner' => 'worker-1',
                    'renewed' => true,
                    'reason' => null,
                ];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/update-task-1/complete')) {
                $completedCommand = $body['commands'][0] ?? null;

                return ['completed' => true];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $client = new Client('https://server.example', transport: $transport);
        $worker = new Worker($client, 'queue', workerId: 'worker-1');
        $worker
            ->registerWorkflow('counter', static fn (WorkflowContext $context): string => 'waiting')
            ->registerUpdate(
                'counter',
                'increment',
                static fn (QueryContext $context, int $value): int => $value + 1,
            );

        $worker->run(0);

        self::assertIsArray($completedCommand);
        self::assertSame('complete_update', $completedCommand['type'] ?? null);
        self::assertSame('update-1', $completedCommand['update_id'] ?? null);
        self::assertSame(42, $client->payloadCodec()->decodeEnvelope($completedCommand['result'] ?? []));
        self::assertSame([
            'counter' => [
                'queries' => [],
                'query_contracts' => [],
                'signals' => [],
                'signal_contracts' => [],
                'updates' => ['increment'],
                'update_contracts' => [[
                    'name' => 'increment',
                    'parameters' => [[
                        'name' => 'value',
                        'position' => 0,
                        'required' => true,
                        'variadic' => false,
                        'default_available' => false,
                        'default' => null,
                        'type' => 'int',
                        'allows_null' => false,
                    ]],
                ]],
                'update_validators' => [],
            ],
        ], $transport->requests[0]['body']['workflow_command_contracts'] ?? null);
    }

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

    public function testRunAdoptsNegotiatedCadenceAndCompletesWorkAfterExtendedIdlePolling(): void
    {
        $now = 0.0;
        $lastServerHeartbeatAt = 0.0;
        $workerHeartbeatTimes = [];
        $pollTimeouts = [];
        $workDelivered = false;
        $workCompleted = false;

        $transport = new FakeTransport(handler: function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (
            &$now,
            &$lastServerHeartbeatAt,
            &$workerHeartbeatTimes,
            &$pollTimeouts,
            &$workDelivered,
            &$workCompleted,
        ): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return [
                    'worker_id' => 'worker-1',
                    'registered' => true,
                    'heartbeat_interval_seconds' => 10,
                ];
            }

            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                self::assertLessThan(30, $now - $lastServerHeartbeatAt);
                $lastServerHeartbeatAt = $now;
                $workerHeartbeatTimes[] = $now;

                return ['acknowledged' => true, 'heartbeat_interval_seconds' => 10];
            }

            if (preg_match('#/api/worker/(workflow|activity|query)-tasks/poll$#', $uri) === 1) {
                if ($workCompleted) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }

                $timeout = (int) ($body['timeout_seconds'] ?? 0);
                $pollTimeouts[] = $timeout;
                $now += $timeout;
                if ($now - $lastServerHeartbeatAt >= 30) {
                    return [
                        'task' => null,
                        'poll_status' => 'stale_worker_registration',
                        'reason' => 'worker_heartbeat_stale',
                    ];
                }

                if (!$workDelivered && $now > 60 && str_ends_with($uri, '/activity-tasks/poll')) {
                    $workDelivered = true;

                    return [
                        'poll_status' => 'leased',
                        'task' => [
                            'task_id' => 'activity-after-idle',
                            'activity_attempt_id' => 'attempt-1',
                            'lease_owner' => 'worker-1',
                            'activity_type' => 'after-idle',
                            'payload_codec' => 'avro',
                        ],
                    ];
                }

                return ['task' => null, 'poll_status' => 'timeout'];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/activity-after-idle/complete')) {
                self::assertSame('POST', $method);
                self::assertSame('worker-1', $body['lease_owner'] ?? null);
                $workCompleted = true;

                return ['completed' => true];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            heartbeatIntervalSeconds: 30,
            clock: static function () use (&$now): float {
                return $now;
            },
        );
        $worker->registerActivity(
            'after-idle',
            static fn (ActivityContext $context): string => 'completed-after-idle',
        );

        $worker->run(3);

        self::assertTrue($workDelivered);
        self::assertTrue($workCompleted);
        self::assertGreaterThan(60, $now);
        self::assertNotEmpty($workerHeartbeatTimes);
        self::assertLessThanOrEqual(10, $workerHeartbeatTimes[0]);
        foreach (array_slice($workerHeartbeatTimes, 1) as $index => $heartbeatAt) {
            self::assertLessThanOrEqual(10, $heartbeatAt - $workerHeartbeatTimes[$index]);
        }
        self::assertNotEmpty($pollTimeouts);
        self::assertSame([3], array_values(array_unique($pollTimeouts)));
    }

    public function testManagedLongPollLeavesTimeToHeartbeatBeforeTheNegotiatedCadence(): void
    {
        $transport = new FakeTransport([
            ['registered' => true, 'heartbeat_interval_seconds' => 10],
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
        ]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            clock: static fn (): float => 0.0,
        );

        $worker->run(30);

        self::assertCount(2, $transport->requests);
        self::assertSame(9, $transport->requests[1]['body']['timeout_seconds'] ?? null);
    }

    /** @param mixed $advertisedInterval */
    #[DataProvider('invalidHeartbeatIntervals')]
    public function testInvalidNegotiatedCadenceKeepsTheSafeConfiguredFallback(mixed $advertisedInterval): void
    {
        $now = 0.0;
        $heartbeatTimes = [];
        $transport = new FakeTransport(handler: function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$now, &$heartbeatTimes, $advertisedInterval): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return [
                    'registered' => true,
                    'heartbeat_interval_seconds' => $advertisedInterval,
                ];
            }

            if (str_ends_with($uri, '/api/worker/heartbeat')) {
                $heartbeatTimes[] = $now;

                return [
                    'acknowledged' => true,
                    'heartbeat_interval_seconds' => $advertisedInterval,
                ];
            }

            if (preg_match('#/api/worker/(workflow|activity|query)-tasks/poll$#', $uri) === 1) {
                if (count($heartbeatTimes) >= 2) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }
                $now += (int) ($body['timeout_seconds'] ?? 0);

                return ['task' => null, 'poll_status' => 'timeout'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
            heartbeatIntervalSeconds: 4,
            clock: static function () use (&$now): float {
                return $now;
            },
        );

        $worker->run(3);

        self::assertSame([3.0, 6.0], $heartbeatTimes);
    }

    /** @return iterable<string, array{mixed}> */
    public static function invalidHeartbeatIntervals(): iterable
    {
        yield 'missing' => [null];
        yield 'zero' => [0];
        yield 'negative' => [-1];
        yield 'above protocol maximum' => [3601];
        yield 'numeric string' => ['10'];
        yield 'fractional number' => [10.5];
    }

    public function testStoppedPollResponseStopsTheWorker(): void
    {
        $transport = new FakeTransport([[
            'task' => null,
            'poll_status' => 'stopped',
            'reason' => 'worker_stopped',
        ]]);
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );

        self::assertFalse($worker->tick(0));
        self::assertFalse($worker->tick(0));
        self::assertCount(1, $transport->requests);
    }
}

final class SagaRecordingLogger extends AbstractLogger
{
    /** @var list<array{level: mixed, message: string, context: array<string, mixed>}> */
    public array $records = [];

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
    }
}
