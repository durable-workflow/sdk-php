<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CapabilityManifest;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\StickyWorkflowCache;
use DurableWorkflow\Worker\WorkerSessionOptions;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class PortableWorkerAffinityTest extends TestCase
{
    public function testPhpWorkerManifestSupportsEveryPortableFeature(): void
    {
        $manifest = CapabilityManifest::portableWorkerAffinity();

        self::assertSame(
            ['local_activities', 'worker_sessions', 'sticky_execution'],
            array_keys($manifest),
        );
        foreach ($manifest as $entry) {
            self::assertTrue($entry['supported']);
            self::assertSame('1.18', $entry['minimum_protocol_version']);
        }
    }

    public function testLocalActivityIsExecutedOnceAndReplayUsesRecordedResult(): void
    {
        $codec = new AvroPayloadCodec();
        $calls = 0;
        $workflow = static function (WorkflowContext $context): array {
            return ['receipt' => $context->localActivity('charge-card', ['order-1'])];
        };
        $executor = static function () use (&$calls): array {
            ++$calls;

            return [
                'outcome' => 'completed',
                'result' => 'receipt-1',
                'attempts' => [['attempt_number' => 1, 'outcome' => 'completed']],
            ];
        };

        $initial = (new Replayer($codec))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            $executor,
        );

        self::assertSame(1, $calls);
        self::assertSame('record_local_activity', $initial->commands[0]['type']);
        self::assertSame('local', $initial->commands[0]['execution_mode']);
        self::assertSame('receipt-1', $codec->decodeEnvelope($initial->commands[0]['result']));

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'charge-card',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'charge-card',
                    'execution_mode' => 'local',
                    'result' => $codec->envelope('receipt-1'),
                ],
            ],
        ];
        $replayed = (new Replayer($codec))->replay(
            $workflow,
            $history,
            [],
            'php-workers',
            [],
            $executor,
        );

        self::assertSame(1, $calls, 'Replay must not repeat the local side effect.');
        self::assertSame('complete_workflow', $replayed->commands[0]['type']);
        self::assertSame(
            ['receipt' => 'receipt-1'],
            $codec->decodeEnvelope($replayed->commands[0]['result']),
        );
    }

    public function testLocalActivityIdentityAndOptionsAreCanonicalBeforeExecution(): void
    {
        $received = null;
        $command = WorkflowCommand::localActivity(
            '  charge-card  ',
            ['order-1'],
            [
                'retry_policy' => [
                    'max_attempts' => 3,
                    'backoff_seconds' => [1, 2],
                    'non_retryable_error_types' => [' DomainFailure ', 'DomainFailure'],
                ],
                'start_to_close_timeout' => 10,
                'schedule_to_close_timeout' => 30,
                'heartbeat_timeout' => 5,
            ],
            static function (string $activityType, array $arguments, array $options) use (&$received): array {
                $received = compact('activityType', 'arguments', 'options');

                return [
                    'outcome' => 'completed',
                    'result' => 'receipt-1',
                    'attempts' => [[
                        'attempt_number' => 1,
                        'outcome' => 'completed',
                        'duration_ms' => 0,
                        'heartbeats' => [],
                    ]],
                ];
            },
        )->resolveLocalActivity(new AvroPayloadCodec());

        $expectedOptions = [
            'retry_policy' => [
                'max_attempts' => 3,
                'backoff_seconds' => [1, 2],
                'non_retryable_error_types' => ['DomainFailure'],
            ],
            'start_to_close_timeout' => 10,
            'schedule_to_close_timeout' => 30,
            'heartbeat_timeout' => 5,
        ];
        self::assertSame([
            'activityType' => 'charge-card',
            'arguments' => ['order-1'],
            'options' => $expectedOptions,
        ], $received);

        $codec = new AvroPayloadCodec();
        $wire = $command->toWire($codec, 'php-workers');
        self::assertSame('charge-card', $wire['activity_type']);
        self::assertSame(['order-1'], $codec->decodeEnvelope($wire['arguments']));
        self::assertSame('local', $wire['execution_mode']);
        foreach ($expectedOptions as $field => $value) {
            self::assertSame($value, $wire[$field]);
        }
    }

    public function testLocalActivityArgumentsAreEncodedBeforeTheHandlerRuns(): void
    {
        $calls = 0;
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->localActivity('unsupported-argument', [new \stdClass()]);
        };

        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                $workflow,
                [],
                [],
                'php-workers',
                [],
                static function () use (&$calls): array {
                    ++$calls;

                    return ['outcome' => 'completed', 'result' => 'must-not-run'];
                },
            );
            self::fail('Unsupported local activity arguments must fail before handler execution.');
        } catch (\DurableWorkflow\Exception\CodecException $exception) {
            self::assertStringContainsString('unsupported_value_type', $exception->getMessage());
        }

        self::assertSame(0, $calls);
    }

    public function testJsonUnsafeLocalActivityWireFieldsAreRejectedBeforeTheHandlerRuns(): void
    {
        $calls = 0;

        foreach ([
            ['activity_type' => "charge-card-\xff", 'options' => []],
            [
                'activity_type' => 'charge-card',
                'options' => ['retry_policy' => ['non_retryable_error_types' => ["DomainFailure\xff"]]],
            ],
        ] as $case) {
            try {
                WorkflowCommand::localActivity(
                    $case['activity_type'],
                    ['order-1'],
                    $case['options'],
                    static function () use (&$calls): array {
                        ++$calls;

                        return ['outcome' => 'completed', 'result' => 'must-not-run'];
                    },
                )->resolveLocalActivity(new AvroPayloadCodec());
                self::fail('JSON-unsafe local activity wire fields must fail before handler execution.');
            } catch (\JsonException) {
                self::assertSame(0, $calls);
            }
        }
    }

    public function testJsonUnsafeReturnedOrThrownFailureBecomesRecordedNonRetryableFailure(): void
    {
        $calls = 0;
        foreach (['returned', 'thrown'] as $mode) {
            $result = (new Replayer(new AvroPayloadCodec()))->replay(
                static fn (WorkflowContext $context): mixed => $context->localActivity("unsafe-{$mode}-failure"),
                [],
                [],
                'php-workers',
                [],
                static function () use (&$calls, $mode): array {
                    ++$calls;
                    if ($mode === 'thrown') {
                        throw new \RuntimeException("unsafe failure \xff");
                    }

                    return [
                        'outcome' => 'failed',
                        'message' => "unsafe failure \xff",
                        'exception_type' => \RuntimeException::class,
                        'non_retryable' => false,
                        'attempts' => [[
                            'attempt_number' => 1,
                            'outcome' => 'failed',
                            'message' => "unsafe failure \xff",
                            'exception_type' => \RuntimeException::class,
                            'non_retryable' => false,
                            'heartbeats' => [],
                        ]],
                    ];
                },
            );

            self::assertSame(['record_local_activity'], array_column($result->commands, 'type'));
            self::assertSame('failed', $result->commands[0]['outcome']);
            self::assertTrue($result->commands[0]['non_retryable']);
            self::assertSame(
                \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
                $result->commands[0]['exception_type'],
            );
            self::assertStringContainsString('HTTP JSON wire boundary', $result->commands[0]['message']);
            self::assertInstanceOf(\DurableWorkflow\Exception\ActivityFailed::class, $result->terminalFailure);
            self::assertJson(json_encode($result->commands, JSON_THROW_ON_ERROR));
        }

        self::assertSame(2, $calls);
    }

    public function testJsonUnsafeThrownFailureDoesNotRetryAndColdReplayDoesNotRepeatIt(): void
    {
        $calls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );
        $worker->registerActivity('unsafe-thrown-failure', static function (
            ActivityContext $_context,
        ) use (&$calls): never {
            ++$calls;

            throw new \RuntimeException("unsafe failure \xff");
        });

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-unsafe-thrown-failure',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'unsafe-thrown-failure', [], [
            'retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [0, 0]],
        ]);

        self::assertSame(1, $calls, 'A JSON-unsafe thrown failure must not retry the local side effect.');
        self::assertSame('failed', $outcome['outcome']);
        self::assertTrue($outcome['non_retryable']);
        self::assertCount(1, $outcome['attempts']);
        self::assertSame(
            \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
            $outcome['exception_type'],
        );
        self::assertStringContainsString('HTTP JSON wire boundary', $outcome['message']);

        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): mixed => $context->localActivity(
            'unsafe-thrown-failure',
            [],
            ['retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [0, 0]]],
        );
        $initial = (new Replayer($codec))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            static fn (): array => $outcome,
        );
        $record = $initial->commands[0];

        self::assertSame('record_local_activity', $record['type']);
        self::assertTrue($record['non_retryable']);
        self::assertJson(json_encode($record, JSON_THROW_ON_ERROR));

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'unsafe-thrown-failure',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityFailed',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'unsafe-thrown-failure',
                    'execution_mode' => 'local',
                    'message' => $record['message'],
                    'exception_type' => $record['exception_type'],
                    'non_retryable' => $record['non_retryable'],
                    'attempts' => $record['attempts'],
                ],
            ],
        ];
        try {
            (new Replayer($codec))->replay(
                $workflow,
                $history,
                [],
                'php-workers',
                [],
                static function () use (&$calls): array {
                    ++$calls;

                    return ['outcome' => 'completed', 'result' => 'must-not-run'];
                },
            );
            self::fail('Cold replay must restore the recorded JSON-safe local activity failure.');
        } catch (\DurableWorkflow\Exception\ActivityFailed $exception) {
            self::assertTrue($exception->nonRetryable);
        }

        self::assertSame(1, $calls, 'Cold replay must not repeat the recorded local side effect.');
    }

    public function testOverlongThrownFailureIsBoundedAndColdReplayDoesNotRepeatIt(): void
    {
        $calls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );
        $worker->registerActivity('overlong-thrown-failure', static function (
            ActivityContext $_context,
        ) use (&$calls): never {
            ++$calls;

            throw self::overlongLocalActivityFailure();
        });

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-overlong-thrown-failure',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'overlong-thrown-failure', [], [
            'retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [0, 0]],
        ]);

        self::assertSame(1, $calls, 'An overlong failure type must not retry the local side effect.');
        self::assertSame('failed', $outcome['outcome']);
        self::assertTrue($outcome['non_retryable']);
        self::assertCount(1, $outcome['attempts']);
        self::assertSame(
            \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
            $outcome['exception_type'],
        );
        self::assertSame($outcome['exception_type'], $outcome['attempts'][0]['exception_type']);

        $codec = new AvroPayloadCodec();
        $replayCalls = 0;
        $workflow = static fn (WorkflowContext $context): mixed => $context->localActivity(
            'overlong-thrown-failure',
            [],
            ['retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [0, 0]]],
        );
        $initial = (new Replayer($codec))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            static function () use (&$replayCalls): never {
                ++$replayCalls;

                throw self::overlongLocalActivityFailure();
            },
        );
        $record = $initial->commands[0];

        self::assertSame(1, $replayCalls);
        self::assertSame('record_local_activity', $record['type']);
        self::assertTrue($record['non_retryable']);
        self::assertSame(
            \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
            $record['exception_type'],
        );
        self::assertSame($record['exception_type'], $record['attempts'][0]['exception_type']);
        self::assertLessThanOrEqual(255, strlen($record['exception_type']));
        self::assertLessThanOrEqual(255, strlen($record['attempts'][0]['exception_type']));
        self::assertJson(json_encode($record, JSON_THROW_ON_ERROR));

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'overlong-thrown-failure',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityFailed',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'overlong-thrown-failure',
                    'execution_mode' => 'local',
                    'message' => $record['message'],
                    'exception_type' => $record['exception_type'],
                    'non_retryable' => $record['non_retryable'],
                    'attempts' => $record['attempts'],
                ],
            ],
        ];
        try {
            (new Replayer($codec))->replay(
                $workflow,
                $history,
                [],
                'php-workers',
                [],
                static function () use (&$replayCalls): array {
                    ++$replayCalls;

                    return ['outcome' => 'completed', 'result' => 'must-not-run'];
                },
            );
            self::fail('Cold replay must restore the bounded local activity failure.');
        } catch (\DurableWorkflow\Exception\ActivityFailed $exception) {
            self::assertTrue($exception->nonRetryable);
        }

        self::assertSame(1, $replayCalls, 'Cold replay must not repeat the recorded local side effect.');
    }

    public function testUnencodableLocalActivityResultBecomesRecordedNonRetryableFailure(): void
    {
        $codec = new AvroPayloadCodec();
        $calls = 0;
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->localActivity('unsupported-result', ['order-1']);
        };
        $executor = static function () use (&$calls): array {
            ++$calls;

            return [
                'outcome' => 'completed',
                'result' => new \stdClass(),
                'attempts' => [[
                    'attempt_id' => 'unsupported-result-attempt-1',
                    'attempt_number' => 1,
                    'outcome' => 'completed',
                    'duration_ms' => 1,
                    'heartbeats' => [],
                ]],
            ];
        };

        $initial = (new Replayer($codec))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            $executor,
        );

        self::assertSame(1, $calls);
        self::assertCount(1, $initial->commands);
        $record = $initial->commands[0];
        self::assertSame('record_local_activity', $record['type']);
        self::assertSame(['order-1'], $codec->decodeEnvelope($record['arguments']));
        self::assertSame('failed', $record['outcome']);
        self::assertTrue($record['non_retryable']);
        self::assertArrayNotHasKey('result', $record);
        self::assertSame('failed', $record['attempts'][0]['outcome']);
        self::assertTrue($record['attempts'][0]['non_retryable']);
        self::assertSame($record['message'], $record['attempts'][0]['message']);
        self::assertSame($record['exception_type'], $record['attempts'][0]['exception_type']);
        self::assertInstanceOf(\DurableWorkflow\Exception\ActivityFailed::class, $initial->terminalFailure);

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'unsupported-result',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityFailed',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'unsupported-result',
                    'execution_mode' => 'local',
                    'message' => $record['message'],
                    'exception_type' => $record['exception_type'],
                    'non_retryable' => $record['non_retryable'],
                    'attempts' => $record['attempts'],
                ],
            ],
        ];
        try {
            (new Replayer($codec))->replay(
                $workflow,
                $history,
                [],
                'php-workers',
                [],
                $executor,
            );
            self::fail('Cold replay must restore the recorded local activity failure.');
        } catch (\DurableWorkflow\Exception\ActivityFailed $exception) {
            self::assertSame($record['message'], $exception->getMessage());
            self::assertTrue($exception->nonRetryable);
        }

        self::assertSame(1, $calls, 'Cold replay must not repeat a local activity with a recorded codec failure.');
    }

    public function testLaterUnencodableCommandPreservesCompletedLocalActivityRecord(): void
    {
        $calls = 0;
        $workflow = static function (WorkflowContext $context): mixed {
            $context->localActivity('record-before-command', ['order-1']);

            return $context->activity('unsupported-command', [new \stdClass()]);
        };

        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            static function () use (&$calls): array {
                ++$calls;

                return [
                    'outcome' => 'completed',
                    'result' => 'recorded',
                    'attempts' => [[
                        'attempt_number' => 1,
                        'outcome' => 'completed',
                        'heartbeats' => [],
                    ]],
                ];
            },
        );

        self::assertSame(1, $calls);
        self::assertSame(['record_local_activity'], array_column($result->commands, 'type'));
        self::assertInstanceOf(\DurableWorkflow\Exception\CodecException::class, $result->terminalFailure);
        self::assertJson(json_encode($result->commands, JSON_THROW_ON_ERROR));
    }

    public function testLaterUnencodableLocalActivityArgumentsPreserveCompletedRecord(): void
    {
        $codec = new AvroPayloadCodec();
        $calls = [];
        $workflow = static function (WorkflowContext $context): mixed {
            $context->localActivity('record-before-local-arguments', ['order-1']);

            return $context->localActivity('unsupported-local-arguments', [new \stdClass()]);
        };
        $executor = static function (string $activityType) use (&$calls): array {
            $calls[] = $activityType;

            return [
                'outcome' => 'completed',
                'result' => 'recorded',
                'attempts' => [[
                    'attempt_number' => 1,
                    'outcome' => 'completed',
                    'heartbeats' => [],
                ]],
            ];
        };

        $result = (new Replayer($codec))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            $executor,
        );

        self::assertSame(['record-before-local-arguments'], $calls);
        self::assertSame(['record_local_activity'], array_column($result->commands, 'type'));
        self::assertInstanceOf(\DurableWorkflow\Exception\CodecException::class, $result->terminalFailure);
        self::assertJson(json_encode($result->commands, JSON_THROW_ON_ERROR));

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'record-before-local-arguments',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'record-before-local-arguments',
                    'execution_mode' => 'local',
                    'result' => $result->commands[0]['result'],
                ],
            ],
        ];
        try {
            (new Replayer($codec))->replay(
                $workflow,
                $history,
                [],
                'php-workers',
                [],
                $executor,
            );
            self::fail('Cold replay must still reject the unencodable later command.');
        } catch (\DurableWorkflow\Exception\CodecException) {
            self::assertSame(
                ['record-before-local-arguments'],
                $calls,
                'Cold replay must not repeat the recorded local activity.',
            );
        }
    }

    public function testUnencodableWorkflowResultPreservesCompletedLocalActivityRecord(): void
    {
        $calls = 0;
        $workflow = static function (WorkflowContext $context): mixed {
            $context->localActivity('record-before-result', ['order-1']);

            return new \stdClass();
        };

        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            $workflow,
            [],
            [],
            'php-workers',
            [],
            static function () use (&$calls): array {
                ++$calls;

                return [
                    'outcome' => 'completed',
                    'result' => 'recorded',
                    'attempts' => [[
                        'attempt_number' => 1,
                        'outcome' => 'completed',
                        'heartbeats' => [],
                    ]],
                ];
            },
        );

        self::assertSame(1, $calls);
        self::assertSame(['record_local_activity'], array_column($result->commands, 'type'));
        self::assertInstanceOf(\DurableWorkflow\Exception\CodecException::class, $result->terminalFailure);
        self::assertJson(json_encode($result->commands, JSON_THROW_ON_ERROR));
    }

    public function testMalformedLocalActivityOptionsAreRejectedBeforeTheHandlerRuns(): void
    {
        $calls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );
        $worker->registerActivity('charge-card', static function (ActivityContext $_context) use (&$calls): string {
            ++$calls;

            return 'must-not-run';
        });

        $invalidOptions = [
            ['activity_type' => 'other-activity'],
            ['arguments_value' => ['other-order']],
            ['execution_mode' => 'remote'],
            ['unsupported' => true],
            ['retry_policy' => ['max_attempts' => 101]],
            ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0, 0]]],
            ['retry_policy' => ['unexpected' => true]],
            ['start_to_close_timeout' => 0],
            ['start_to_close_timeout' => 2, 'schedule_to_close_timeout' => 1],
            ['start_to_close_timeout' => 1, 'heartbeat_timeout' => 2],
        ];

        foreach ($invalidOptions as $options) {
            $executorCalls = 0;
            try {
                WorkflowCommand::localActivity(
                    'charge-card',
                    [],
                    $options,
                    static function () use (&$executorCalls): array {
                        ++$executorCalls;

                        return ['outcome' => 'completed', 'result' => 'must-not-run'];
                    },
                )->resolveLocalActivity(new AvroPayloadCodec());
                self::fail('Malformed local activity authoring options must be rejected.');
            } catch (\InvalidArgumentException) {
                self::assertSame(0, $executorCalls);
            }

            try {
                $this->executeLocalActivity($worker, [
                    'task_id' => 'workflow-task-invalid-options',
                    'lease_owner' => 'php-worker-1',
                    'workflow_task_attempt' => 1,
                ], 'charge-card', [], $options);
                self::fail('Malformed local activity options must be rejected.');
            } catch (\InvalidArgumentException) {
                self::assertSame(0, $calls);
            }
        }
    }

    public function testLocalActivityHeartbeatReportBoundaryIsEnforced(): void
    {
        foreach ([1000, 1001] as $reportCount) {
            $transport = new FakeTransport(handler: static function (
                string $method,
                string $uri,
                array $_headers,
                ?array $_body,
            ) use ($reportCount): array {
                if (str_ends_with($uri, "/workflow-tasks/heartbeat-boundary-{$reportCount}/heartbeat")) {
                    return [
                        'task_id' => "heartbeat-boundary-{$reportCount}",
                        'workflow_task_attempt' => 1,
                        'lease_owner' => 'php-worker-1',
                        'renewed' => true,
                    ];
                }

                self::fail("Unexpected worker request: {$method} {$uri}");
            });
            $worker = new Worker(
                new Client('https://server.example', transport: $transport),
                'php-workers',
            );
            $worker->registerActivity(
                'heartbeat-boundary',
                static function (ActivityContext $context) use ($reportCount): string {
                    for ($report = 1; $report <= $reportCount; ++$report) {
                        $context->heartbeat($report % 2 === 0 ? [$report] : ['report' => $report]);
                    }

                    return 'completed';
                },
            );

            $outcome = $this->executeLocalActivity($worker, [
                'task_id' => "heartbeat-boundary-{$reportCount}",
                'lease_owner' => 'php-worker-1',
                'workflow_task_attempt' => 1,
            ], 'heartbeat-boundary', [], [
                'retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]],
            ]);

            self::assertCount(1000, $outcome['attempts'][0]['heartbeats']);
            self::assertCount(1000, $transport->requests);
            self::assertSame(['report' => 1], $outcome['attempts'][0]['heartbeats'][0]['details']);
            self::assertSame([2], $outcome['attempts'][0]['heartbeats'][1]['details']);
            if ($reportCount === 1000) {
                self::assertSame('completed', $outcome['outcome']);
                self::assertSame('completed', $outcome['attempts'][0]['outcome']);
            } else {
                self::assertSame('failed', $outcome['outcome']);
                self::assertTrue($outcome['non_retryable']);
                self::assertStringContainsString('at most 1000 heartbeats', $outcome['message']);
                self::assertCount(1, $outcome['attempts'], 'A report overflow must not retry the local side effect.');
            }
        }
    }

    public function testUnencodableLocalActivityHeartbeatBecomesRecordedNonRetryableFailure(): void
    {
        $transport = new FakeTransport();
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'php-workers',
        );
        $worker->registerActivity(
            'unsupported-heartbeat',
            static function (ActivityContext $context): string {
                $context->heartbeat(['unsupported' => new \stdClass()]);

                return 'must-not-complete';
            },
        );

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-unsupported-heartbeat',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'unsupported-heartbeat', [], []);

        self::assertSame('failed', $outcome['outcome']);
        self::assertTrue($outcome['non_retryable']);
        self::assertSame(
            \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
            $outcome['exception_type'],
        );
        self::assertSame([], $outcome['attempts'][0]['heartbeats']);
        self::assertSame([], $transport->requests, 'Invalid details must be rejected before lease renewal.');

        $replay = (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): mixed => $context->localActivity('unsupported-heartbeat'),
            [],
            [],
            'php-workers',
            [],
            static fn (): array => $outcome,
        );

        self::assertSame(['record_local_activity'], array_column($replay->commands, 'type'));
        self::assertSame('failed', $replay->commands[0]['outcome']);
        self::assertTrue($replay->commands[0]['non_retryable']);
        self::assertInstanceOf(\DurableWorkflow\Exception\ActivityFailed::class, $replay->terminalFailure);
        self::assertJson(json_encode($replay->commands, JSON_THROW_ON_ERROR));
    }

    public function testJsonUnsafeLocalActivityHeartbeatBecomesRecordedNonRetryableFailure(): void
    {
        $transport = new FakeTransport();
        $unsafeDetails = ['binary' => AvroBinaryValue::fromBytes("\xff")];
        self::assertNotSame('', (new AvroPayloadCodec())->encode($unsafeDetails));
        $handlerCalls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'php-workers',
        );
        $worker->registerActivity(
            'json-unsafe-heartbeat',
            static function (ActivityContext $context) use ($unsafeDetails, &$handlerCalls): string {
                ++$handlerCalls;
                $context->heartbeat($unsafeDetails);

                return 'must-not-complete';
            },
        );

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-json-unsafe-heartbeat',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'json-unsafe-heartbeat', [], []);

        self::assertSame(1, $handlerCalls);
        self::assertSame('failed', $outcome['outcome']);
        self::assertTrue($outcome['non_retryable']);
        self::assertStringContainsString('HTTP JSON wire boundary', $outcome['message']);
        self::assertSame(
            \DurableWorkflow\Exception\InvalidLocalActivityReport::class,
            $outcome['exception_type'],
        );
        self::assertSame([], $outcome['attempts'][0]['heartbeats']);
        self::assertSame([], $transport->requests, 'JSON-unsafe details must be rejected before lease renewal.');

        $replay = (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): mixed => $context->localActivity('json-unsafe-heartbeat'),
            [],
            [],
            'php-workers',
            [],
            static fn (): array => $outcome,
        );

        self::assertSame(['record_local_activity'], array_column($replay->commands, 'type'));
        self::assertSame('failed', $replay->commands[0]['outcome']);
        self::assertTrue($replay->commands[0]['non_retryable']);
        self::assertInstanceOf(\DurableWorkflow\Exception\ActivityFailed::class, $replay->terminalFailure);
        self::assertJson(json_encode($replay->commands, JSON_THROW_ON_ERROR));
    }

    public function testLocalActivityHeartbeatLimitAppliesAcrossRetryAttempts(): void
    {
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $_headers,
            ?array $_body,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/heartbeat-retry-boundary/heartbeat')) {
                return [
                    'task_id' => 'heartbeat-retry-boundary',
                    'workflow_task_attempt' => 1,
                    'lease_owner' => 'php-worker-1',
                    'renewed' => true,
                ];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'php-workers',
        );
        $worker->registerActivity(
            'heartbeat-retry-boundary',
            static function (ActivityContext $context): string {
                if ($context->attemptNumber === 1) {
                    for ($report = 1; $report <= 999; ++$report) {
                        $context->heartbeat([$report]);
                    }
                    throw new \RuntimeException('retry with one heartbeat remaining');
                }
                $context->heartbeat(['attempt' => 2, 'report' => 1000]);
                $context->heartbeat([1001]);

                return 'must-not-complete';
            },
        );

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'heartbeat-retry-boundary',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'heartbeat-retry-boundary', [], [
            'retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]],
        ]);

        self::assertSame('failed', $outcome['outcome']);
        self::assertTrue($outcome['non_retryable']);
        self::assertStringContainsString('at most 1000 heartbeats', $outcome['message']);
        self::assertCount(2, $outcome['attempts']);
        self::assertSame('failed', $outcome['attempts'][0]['outcome']);
        self::assertCount(999, $outcome['attempts'][0]['heartbeats']);
        self::assertSame([1], $outcome['attempts'][0]['heartbeats'][0]['details']);
        self::assertCount(1, $outcome['attempts'][1]['heartbeats']);
        self::assertSame(
            ['attempt' => 2, 'report' => 1000],
            $outcome['attempts'][1]['heartbeats'][0]['details'],
        );
        self::assertCount(1000, $transport->requests);
    }

    public function testStickyCacheIsExactBoundedAndFallsBackCold(): void
    {
        $cache = new StickyWorkflowCache(capacity: 1, ttlSeconds: 300);
        $historyA = [['event_type' => 'WorkflowStarted', 'payload' => ['sequence' => 0]]];
        $historyB = [['event_type' => 'WorkflowStarted', 'payload' => ['sequence' => 0, 'run' => 'b']]];

        $cache->remember('workflow', 'run-a', 'build-a', $historyA);
        self::assertSame($historyA, $cache->history(
            'workflow',
            'run-a',
            'build-a',
            [],
            'sticky_hit_expected',
        ));
        self::assertNull($cache->history(
            'workflow',
            'run-a',
            'build-b',
            [],
            'sticky_hit_expected',
        ));

        $cache->remember('workflow', 'run-b', 'build-a', $historyB);
        self::assertNull($cache->history(
            'workflow',
            'run-a',
            'build-a',
            [],
            'forced_cold_replay',
        ));

        self::assertSame([
            'hit' => 1,
            'miss' => 2,
            'eviction' => 1,
            'forced_cold_replay' => 2,
        ], $cache->metrics());
    }

    public function testStickyCacheTtlMatchesThePublishedClaimBoundary(): void
    {
        $cache = new StickyWorkflowCache(capacity: 1, ttlSeconds: 3600);

        self::assertSame(3600, $cache->ttlSeconds());

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Sticky cache TTL must not exceed 3600 seconds.');

        new StickyWorkflowCache(capacity: 1, ttlSeconds: 3601);
    }

    public function testWorkerRejectsAnOversizedStickyCacheTtlBeforePolling(): void
    {
        $transport = new FakeTransport(handler: static function (): array {
            self::fail('An invalid sticky cache TTL must be rejected before polling.');
        });

        try {
            new Worker(
                new Client('https://server.example', transport: $transport),
                'sticky-workers',
                stickyCacheTtlSeconds: 3601,
            );
            self::fail('An oversized sticky cache TTL must be rejected.');
        } catch (\InvalidArgumentException $exception) {
            self::assertSame('Sticky cache TTL must not exceed 3600 seconds.', $exception->getMessage());
        }

        self::assertSame([], $transport->requests);
    }

    public function testStickyCacheNeverPromotesAnUnreplayableRuntimeSuffix(): void
    {
        $cache = new StickyWorkflowCache(capacity: 1, ttlSeconds: 300);
        $suffix = [[
            'event_type' => 'ActivityCompleted',
            'payload' => ['sequence' => 1, 'activity_type' => 'sticky.activity'],
        ]];

        self::assertNull($cache->history(
            'workflow',
            'run-a',
            'build-a',
            $suffix,
            'sticky_hit_expected',
        ));
        self::assertSame([
            'hit' => 0,
            'miss' => 1,
            'eviction' => 0,
            'forced_cold_replay' => 1,
        ], $cache->metrics());
    }

    public function testWorkerRejectsEmptyAuthoritativeHistoryWithoutCompletionOrCachePromotion(): void
    {
        $handlerCalls = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $_headers,
            ?array $_body,
        ): array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return [
                    'poll_status' => 'leased',
                    'task' => [
                        'task_id' => 'empty-authoritative-history',
                        'workflow_task_attempt' => 1,
                        'lease_owner' => 'sticky-worker',
                        'workflow_id' => 'workflow-a',
                        'run_id' => 'run-a',
                        'workflow_type' => 'sticky.workflow',
                        'payload_codec' => 'avro',
                        'sticky_replay_mode' => 'sticky_hit_expected',
                        'history_events' => [[
                            'event_type' => 'ActivityCompleted',
                            'payload' => ['sequence' => 1, 'activity_type' => 'sticky.activity'],
                        ]],
                    ],
                ];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/empty-authoritative-history/history')) {
                return ['history_events' => [], 'next_history_page_token' => null];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/empty-authoritative-history/fail')) {
                return ['failed' => true];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'sticky-workers',
            workerId: 'sticky-worker',
            buildId: 'build-a',
        );
        $worker->registerWorkflow(
            'sticky.workflow',
            static function (WorkflowContext $_context) use (&$handlerCalls): string {
                ++$handlerCalls;

                return 'must-not-run';
            },
        );

        self::assertTrue($worker->tick(0));
        self::assertSame(0, $handlerCalls);
        $failureRequests = array_values(array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/fail'),
        ));
        self::assertCount(1, $failureRequests);
        self::assertStringContainsString(
            'Authoritative workflow history must begin with WorkflowStarted',
            (string) ($failureRequests[0]['body']['failure']['message'] ?? ''),
        );
        self::assertSame([], array_values(array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/complete'),
        )));

        $cacheProperty = new \ReflectionProperty($worker, 'stickyCache');
        $cache = $cacheProperty->getValue($worker);
        self::assertInstanceOf(StickyWorkflowCache::class, $cache);
        $entriesProperty = new \ReflectionProperty($cache, 'entries');
        self::assertSame([], $entriesProperty->getValue($cache));
    }

    public function testWorkerColdReplayAfterStickyEvictionExpiryAndBuildMismatchMatchesFullReplay(): void
    {
        $eviction = $this->runStickyColdReplayScenario('eviction');
        $expiry = $this->runStickyColdReplayScenario('expiry');
        $mismatch = $this->runStickyColdReplayScenario('build_mismatch');

        self::assertSame($eviction['initial_commands'], $eviction['fallback_commands']);
        self::assertSame($expiry['initial_commands'], $expiry['fallback_commands']);
        self::assertSame($eviction['initial_commands'], $mismatch['fallback_commands']);
        foreach ([$eviction, $expiry, $mismatch] as $scenario) {
            self::assertSame(1, $scenario['history_fetches']);
            self::assertSame('complete_workflow', $scenario['fallback_commands'][0]['type'] ?? null);
            self::assertNotContains(
                'schedule_activity',
                array_column($scenario['fallback_commands'], 'type'),
                'Forced cold replay must preserve recorded durable behavior.',
            );
        }

        self::assertSame([
            'hit' => 0,
            'miss' => 3,
            'eviction' => 2,
            'forced_cold_replay' => 1,
        ], $eviction['metrics']);
        self::assertSame([
            'hit' => 0,
            'miss' => 2,
            'eviction' => 1,
            'forced_cold_replay' => 1,
        ], $expiry['metrics']);
        self::assertSame([
            'hit' => 0,
            'miss' => 1,
            'eviction' => 0,
            'forced_cold_replay' => 1,
        ], $mismatch['metrics']);
    }

    public function testStickyMetricsDescribeTheHistoryPathActuallySelected(): void
    {
        $cache = new StickyWorkflowCache(capacity: 1, ttlSeconds: 300);
        $cached = [['event_type' => 'WorkflowStarted', 'payload' => ['source' => 'cache']]];
        $authoritative = [['event_type' => 'WorkflowStarted', 'payload' => ['source' => 'runtime']]];

        $cache->remember('workflow', 'run-a', 'build-a', $cached);

        self::assertSame($authoritative, $cache->history(
            'workflow',
            'run-a',
            'build-a',
            $authoritative,
            'sticky_hit_expected',
        ));
        self::assertSame([
            'hit' => 0,
            'miss' => 1,
            'eviction' => 0,
            'forced_cold_replay' => 1,
        ], $cache->metrics());
    }

    public function testLocalActivityRetriesAreBoundedAndRecorded(): void
    {
        $now = 0.0;
        $calls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
            clock: static function () use (&$now): float {
                return $now;
            },
            sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
        );
        $worker->registerActivity('charge-card', static function (ActivityContext $_context) use (&$calls): string {
            ++$calls;
            if ($calls === 1) {
                throw new \RuntimeException('retry once');
            }

            return 'receipt-1';
        });

        $outcome = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-1',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'charge-card', [], [
            'retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [1]],
            'start_to_close_timeout' => 10,
            'schedule_to_close_timeout' => 30,
        ]);

        self::assertSame('completed', $outcome['outcome']);
        self::assertSame('receipt-1', $outcome['result']);
        self::assertSame(2, $calls);
        self::assertCount(2, $outcome['attempts']);
        self::assertSame([1, 2], array_column($outcome['attempts'], 'attempt_number'));
        self::assertSame('failed', $outcome['attempts'][0]['outcome']);
        self::assertSame('failure', $outcome['attempts'][0]['retry_reason']);
        self::assertSame(1, $outcome['attempts'][0]['backoff_seconds']);
        self::assertSame('completed', $outcome['attempts'][1]['outcome']);
        self::assertArrayNotHasKey('retry_reason', $outcome['attempts'][1]);
        self::assertArrayNotHasKey('backoff_seconds', $outcome['attempts'][1]);
    }

    public function testLocalActivityCooperativeTimeoutAndCancellationBoundariesAreRecorded(): void
    {
        $now = 0.0;
        $calls = 0;
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
            clock: static function () use (&$now): float {
                return $now;
            },
        );
        $worker->registerActivity('slow-activity', static function (ActivityContext $_context) use (&$calls, &$now): string {
            ++$calls;
            $now += 2;

            return 'late';
        });

        $timedOut = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-timeout',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'slow-activity', [], [
            'retry_policy' => ['max_attempts' => 1],
            'start_to_close_timeout' => 1,
        ]);
        self::assertSame('timed_out', $timedOut['outcome']);
        self::assertSame('Local activity start-to-close timeout elapsed.', $timedOut['message']);
        self::assertSame('start_to_close', $timedOut['timeout_kind']);
        self::assertSame(1, $calls);
        self::assertCount(1, $timedOut['attempts']);
        self::assertSame('start_to_close', $timedOut['attempts'][0]['timeout_kind']);
        self::assertArrayNotHasKey('retry_reason', $timedOut['attempts'][0]);
        self::assertArrayNotHasKey('backoff_seconds', $timedOut['attempts'][0]);

        $continuedAfterHeartbeat = false;
        $worker->registerActivity(
            'heartbeat-timeout',
            static function (ActivityContext $context) use (&$continuedAfterHeartbeat, &$now): string {
                $now += 2;
                $context->heartbeat(['stage' => 'after-blocking-call']);
                $continuedAfterHeartbeat = true;

                return 'late';
            },
        );
        $heartbeatTimedOut = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-heartbeat-timeout',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'heartbeat-timeout', [], [
            'retry_policy' => ['max_attempts' => 1],
            'start_to_close_timeout' => 1,
        ]);
        self::assertSame('timed_out', $heartbeatTimedOut['outcome']);
        self::assertSame('start_to_close', $heartbeatTimedOut['timeout_kind']);
        self::assertFalse(
            $continuedAfterHeartbeat,
            'Elapsed timeout is detected at the cooperative heartbeat boundary.',
        );

        $cancelled = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-cancelled',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
            'cancel_requested' => true,
        ], 'slow-activity', [], [
            'retry_policy' => ['max_attempts' => 100],
        ]);
        self::assertSame('cancelled', $cancelled['outcome']);
        self::assertTrue($cancelled['non_retryable']);
        self::assertSame(1, $calls, 'Cancellation must happen before the local handler is invoked.');
        self::assertCount(1, $cancelled['attempts']);
        self::assertArrayNotHasKey('retry_reason', $cancelled['attempts'][0]);
        self::assertArrayNotHasKey('backoff_seconds', $cancelled['attempts'][0]);
    }

    public function testMissingLocalActivityHandlerStillReportsOneAuthoritativeAttempt(): void
    {
        $worker = new Worker(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );

        $failed = $this->executeLocalActivity($worker, [
            'task_id' => 'workflow-task-missing-handler',
            'lease_owner' => 'php-worker-1',
            'workflow_task_attempt' => 1,
        ], 'missing-activity', [], []);

        self::assertSame('failed', $failed['outcome']);
        self::assertTrue($failed['non_retryable']);
        self::assertCount(1, $failed['attempts']);
        self::assertSame(1, $failed['attempts'][0]['attempt_number']);
        self::assertSame('failed', $failed['attempts'][0]['outcome']);
        self::assertSame($failed['message'], $failed['attempts'][0]['message']);
        self::assertSame($failed['non_retryable'], $failed['attempts'][0]['non_retryable']);
    }

    public function testUncaughtTerminalLocalActivityOutcomesAreRecordedBeforeWorkflowFailure(): void
    {
        $terminalEvents = [
            'failed' => 'ActivityFailed',
            'timed_out' => 'ActivityTimedOut',
            'cancelled' => 'ActivityCancelled',
        ];

        foreach ($terminalEvents as $outcome => $terminalEvent) {
            $calls = 0;
            $now = 0.0;
            $initial = $this->runTerminalLocalActivityWorkflow($outcome, [], $calls, $now);

            self::assertCount(2, $initial, $outcome);
            self::assertSame('record_local_activity', $initial[0]['type'] ?? null, $outcome);
            self::assertSame($outcome, $initial[0]['outcome'] ?? null, $outcome);
            self::assertSame($outcome, $initial[0]['attempts'][0]['outcome'] ?? null, $outcome);
            self::assertSame('fail_workflow', $initial[1]['type'] ?? null, $outcome);
            self::assertSame(
                \DurableWorkflow\Exception\ActivityFailed::class,
                $initial[1]['exception_type'] ?? null,
                $outcome,
            );

            $callsAfterRecording = $calls;
            self::assertSame($outcome === 'cancelled' ? 0 : 1, $callsAfterRecording, $outcome);
            $terminalPayload = [
                'sequence' => 1,
                'activity_type' => 'terminal-local-activity',
                'execution_mode' => 'local',
                'message' => $initial[0]['message'] ?? null,
                'exception_type' => $initial[0]['exception_type'] ?? null,
                'non_retryable' => $initial[0]['non_retryable'] ?? false,
                'attempts' => $initial[0]['attempts'] ?? [],
            ];
            if (isset($initial[0]['timeout_kind'])) {
                $terminalPayload['timeout_kind'] = $initial[0]['timeout_kind'];
            }
            $history = [
                [
                    'event_type' => 'ActivityScheduled',
                    'payload' => [
                        'sequence' => 1,
                        'activity_type' => 'terminal-local-activity',
                        'execution_mode' => 'local',
                    ],
                ],
                ['event_type' => $terminalEvent, 'payload' => $terminalPayload],
            ];

            $replayed = $this->runTerminalLocalActivityWorkflow($outcome, $history, $calls, $now);

            self::assertSame(
                $callsAfterRecording,
                $calls,
                "Cold replay repeated the {$outcome} local activity side effect.",
            );
            self::assertCount(1, $replayed, $outcome);
            self::assertSame('fail_workflow', $replayed[0]['type'] ?? null, $outcome);
            self::assertSame($initial[0]['message'] ?? null, $replayed[0]['message'] ?? null, $outcome);
        }
    }

    public function testCompletedLocalActivityIsRecordedWhenWorkflowThrowsAndColdReplayDoesNotRepeatIt(): void
    {
        $outcome = 'completed_then_workflow_failed';
        $calls = 0;
        $now = 0.0;
        $initial = $this->runTerminalLocalActivityWorkflow($outcome, [], $calls, $now);

        self::assertCount(2, $initial);
        self::assertSame('record_local_activity', $initial[0]['type'] ?? null);
        self::assertSame('completed', $initial[0]['outcome'] ?? null);
        self::assertSame('completed', $initial[0]['attempts'][0]['outcome'] ?? null);
        self::assertSame('fail_workflow', $initial[1]['type'] ?? null);
        self::assertSame('Workflow failed after local activity completed.', $initial[1]['message'] ?? null);
        self::assertSame(\RuntimeException::class, $initial[1]['exception_type'] ?? null);
        self::assertSame(1, $calls);

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'terminal-local-activity',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'terminal-local-activity',
                    'execution_mode' => 'local',
                    'outcome' => 'completed',
                    'result' => $initial[0]['result'] ?? null,
                    'attempts' => $initial[0]['attempts'] ?? [],
                ],
            ],
        ];
        $replayed = $this->runTerminalLocalActivityWorkflow($outcome, $history, $calls, $now);

        self::assertSame(1, $calls, 'Cold replay repeated the completed local activity side effect.');
        self::assertCount(1, $replayed);
        self::assertSame('fail_workflow', $replayed[0]['type'] ?? null);
        self::assertSame('Workflow failed after local activity completed.', $replayed[0]['message'] ?? null);
        self::assertSame(\RuntimeException::class, $replayed[0]['exception_type'] ?? null);
    }

    public function testJsonUnsafeWorkflowFailureIsNormalizedOnInitialExecutionAndColdReplay(): void
    {
        $outcome = 'completed_then_json_unsafe_workflow_failed';
        $calls = 0;
        $now = 0.0;
        $initial = $this->runTerminalLocalActivityWorkflow($outcome, [], $calls, $now);

        self::assertCount(2, $initial);
        self::assertSame('record_local_activity', $initial[0]['type'] ?? null);
        self::assertSame('completed', $initial[0]['outcome'] ?? null);
        self::assertSame('fail_workflow', $initial[1]['type'] ?? null);
        self::assertSame(
            'Workflow failure metadata could not be encoded for the HTTP JSON wire boundary.',
            $initial[1]['message'] ?? null,
        );
        self::assertSame(\RuntimeException::class, $initial[1]['exception_type'] ?? null);
        self::assertJson(json_encode($initial, JSON_THROW_ON_ERROR));
        self::assertSame(1, $calls);

        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'terminal-local-activity',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'terminal-local-activity',
                    'execution_mode' => 'local',
                    'outcome' => 'completed',
                    'result' => $initial[0]['result'] ?? null,
                    'attempts' => $initial[0]['attempts'] ?? [],
                ],
            ],
        ];
        $replayed = $this->runTerminalLocalActivityWorkflow($outcome, $history, $calls, $now);

        self::assertSame(1, $calls, 'Cold replay repeated the completed local activity side effect.');
        self::assertSame([$initial[1]], $replayed);
        self::assertJson(json_encode($replayed, JSON_THROW_ON_ERROR));
    }

    public function testTypedWorkerSessionLifecycleIsIdempotentAndSupportsReacquisition(): void
    {
        $holderTransport = new FakeTransport([
            ['outcome' => 'created', 'session' => ['lease_owner' => 'php-holder-1']],
            ['outcome' => 'heartbeat_recorded', 'session' => ['lease_owner' => 'php-holder-1']],
            ['outcome' => 'closed', 'session' => ['lease_owner' => 'php-holder-1', 'status' => 'closed']],
        ]);
        $options = new WorkerSessionOptions(
            sessionId: 'gpu-render',
            queue: 'gpu-activities',
            requirements: ['gpu:nvidia-l4'],
            leaseSeconds: 120,
            ttlSeconds: 600,
            maxConcurrentActivities: 2,
        );
        $holder = new Worker(
            new Client('https://server.example', transport: $holderTransport),
            'php-workers',
            workerId: 'php-holder-1',
        );
        $session = $holder->workerSession($options);

        self::assertSame('created', $session->create()['outcome']);
        self::assertSame([
            'worker_session' => [
                'session_id' => 'gpu-render',
                'queue' => 'gpu-activities',
                'requirements' => ['gpu:nvidia-l4'],
                'lease_seconds' => 120,
                'ttl_seconds' => 600,
                'max_concurrent_activities' => 2,
                'create_if_missing' => true,
                'allow_reacquire_after_failure' => true,
            ],
        ], $session->activityOptions());
        self::assertSame('heartbeat_recorded', $session->renew(180)['outcome']);
        $closed = $session->close('graceful_shutdown');
        self::assertSame('closed', $closed['outcome']);
        self::assertSame($closed, $session->close('graceful_shutdown'));
        self::assertCount(3, $holderTransport->requests, 'Duplicate graceful close must not repeat the request.');
        self::assertSame(180, $holderTransport->requests[1]['body']['lease_seconds']);

        $replacementTransport = new FakeTransport([[
            'outcome' => 'reacquired',
            'session' => ['lease_owner' => 'php-holder-2', 'status' => 'active'],
        ]]);
        $replacement = (new Worker(
            new Client('https://server.example', transport: $replacementTransport),
            'php-workers',
            workerId: 'php-holder-2',
        ))->workerSession($options);

        self::assertTrue($replacement->rebuildRequiredAfterHolderLoss());
        self::assertSame('reacquired', $replacement->create()['outcome']);
        self::assertSame('php-holder-2', $replacementTransport->requests[0]['body']['worker_id']);
    }

    public function testWorkerSessionIdIsCanonicalAcrossTheCompleteLifecycle(): void
    {
        $transport = new FakeTransport([
            ['outcome' => 'created'],
            ['outcome' => 'heartbeat_recorded'],
            ['outcome' => 'closed'],
            ['outcome' => 'closed'],
        ]);
        $options = new WorkerSessionOptions(sessionId: " \tgpu-render\n ");
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'php-workers',
            workerId: 'php-holder-1',
        );
        $session = $worker->workerSession($options);

        self::assertSame('gpu-render', $options->sessionId);
        self::assertSame('created', $session->create()['outcome']);
        self::assertSame('gpu-render', $session->activityOptions()['worker_session']['session_id']);
        self::assertSame('heartbeat_recorded', $session->renew()['outcome']);
        self::assertSame('closed', $session->close('explicit_close')['outcome']);

        $worker->requestShutdown();
        $worker->run(0);

        self::assertSame('gpu-render', $transport->requests[0]['body']['session_id']);
        self::assertStringEndsWith('/api/worker/sessions/gpu-render/heartbeat', $transport->requests[1]['uri']);
        self::assertStringEndsWith('/api/worker/sessions/gpu-render', $transport->requests[2]['uri']);
        self::assertSame('explicit_close', $transport->requests[2]['body']['reason']);
        self::assertStringEndsWith('/api/worker/sessions/gpu-render', $transport->requests[3]['uri']);
        self::assertSame('worker_shutdown', $transport->requests[3]['body']['reason']);
    }

    /**
     * @param array<string, mixed> $task
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    private function executeLocalActivity(
        Worker $worker,
        array $task,
        string $activityType,
        array $arguments,
        array $options,
    ): array {
        $method = new \ReflectionMethod($worker, 'executeLocalActivity');
        $outcome = $method->invoke($worker, $task, $activityType, $arguments, $options);
        self::assertIsArray($outcome);

        return $outcome;
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private function runTerminalLocalActivityWorkflow(
        string $outcome,
        array $history,
        int &$calls,
        float &$now,
    ): array {
        $commands = null;
        $polled = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $_headers,
            ?array $body,
        ) use (&$commands, &$polled, $history, $outcome): array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                if ($polled) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }
                $polled = true;

                return [
                    'poll_status' => 'leased',
                    'task' => [
                        'task_id' => "local-activity-{$outcome}",
                        'workflow_task_attempt' => 1,
                        'lease_owner' => 'php-worker-1',
                        'workflow_id' => "local-activity-workflow-{$outcome}",
                        'run_id' => 'run-1',
                        'workflow_type' => 'terminal-local-activity.workflow',
                        'payload_codec' => 'avro',
                        'cancel_requested' => $outcome === 'cancelled',
                        'history_events' => $history,
                    ],
                ];
            }
            if (str_ends_with($uri, "/api/worker/workflow-tasks/local-activity-{$outcome}/heartbeat")) {
                return [
                    'task_id' => "local-activity-{$outcome}",
                    'workflow_task_attempt' => 1,
                    'lease_owner' => 'php-worker-1',
                    'renewed' => true,
                ];
            }
            if (str_ends_with($uri, "/api/worker/workflow-tasks/local-activity-{$outcome}/complete")) {
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
            'php-workers',
            workerId: 'php-worker-1',
            clock: static function () use (&$now): float {
                return $now;
            },
        );
        $worker->registerWorkflow(
            'terminal-local-activity.workflow',
            static function (WorkflowContext $context) use ($outcome): string {
                $options = ['retry_policy' => ['max_attempts' => 1]];
                if ($outcome === 'timed_out') {
                    $options['start_to_close_timeout'] = 1;
                }
                $result = $context->localActivity('terminal-local-activity', [], $options);

                if (str_starts_with($outcome, 'completed_then_')) {
                    throw $outcome === 'completed_then_json_unsafe_workflow_failed'
                        ? self::jsonUnsafeWorkflowFailure()
                        : new \RuntimeException('Workflow failed after local activity completed.');
                }

                return (string) $result;
            },
        );
        $worker->registerActivity(
            'terminal-local-activity',
            static function (ActivityContext $_context) use (&$calls, &$now, $outcome): string {
                ++$calls;
                if ($outcome === 'failed') {
                    throw new \RuntimeException('Local activity failed before workflow completion.');
                }
                if ($outcome === 'timed_out') {
                    $now += 2;
                }

                return 'local-result';
            },
        );

        self::assertTrue($worker->tick(0));
        self::assertIsArray($commands);

        return $commands;
    }

    private static function jsonUnsafeWorkflowFailure(): \RuntimeException
    {
        $shortName = 'PortableWorkerAffinityJsonUnsafe'.chr(0xff).'Failure';
        $className = __NAMESPACE__.'\\'.$shortName;
        if (!class_exists($className, false)) {
            eval('namespace '.__NAMESPACE__."; class {$shortName} extends \\RuntimeException {}");
        }

        $exception = new $className('Workflow failed after local activity completed.');
        self::assertInstanceOf(\RuntimeException::class, $exception);
        self::assertFalse(json_encode($exception::class));
        self::assertSame(JSON_ERROR_UTF8, json_last_error());

        return $exception;
    }

    private static function overlongLocalActivityFailure(): \RuntimeException
    {
        $shortName = 'PortableWorkerAffinityOverlongFailure'.str_repeat('X', 256);
        $className = __NAMESPACE__.'\\'.$shortName;
        if (!class_exists($className, false)) {
            eval('namespace '.__NAMESPACE__."; class {$shortName} extends \\RuntimeException {}");
        }

        $exception = new $className('Local activity failed after its side effect completed.');
        self::assertInstanceOf(\RuntimeException::class, $exception);
        self::assertGreaterThan(255, strlen($exception::class));

        return $exception;
    }

    /**
     * @return array{
     *     initial_commands: list<array<string, mixed>>|null,
     *     fallback_commands: list<array<string, mixed>>,
     *     history_fetches: int,
     *     metrics: array{hit: int, miss: int, eviction: int, forced_cold_replay: int}
     * }
     */
    private function runStickyColdReplayScenario(string $scenario): array
    {
        $codec = new AvroPayloadCodec();
        $fullHistory = [
            ['event_type' => 'WorkflowStarted', 'payload' => ['sequence' => 0]],
            [
                'event_type' => 'ActivityScheduled',
                'payload' => ['sequence' => 1, 'activity_type' => 'sticky.activity'],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'sticky.activity',
                    'result' => $codec->envelope('durable-result'),
                ],
            ],
        ];
        $suffix = array_slice($fullHistory, 1);
        $task = static fn (
            string $taskId,
            string $workflowId,
            string $runId,
            array $history,
            string $replayMode,
        ): array => [
            'task_id' => $taskId,
            'workflow_task_attempt' => 1,
            'lease_owner' => 'sticky-worker',
            'workflow_id' => $workflowId,
            'run_id' => $runId,
            'workflow_type' => 'sticky.workflow',
            'payload_codec' => 'avro',
            'history_events' => $history,
            'sticky_replay_mode' => $replayMode,
        ];
        $tasks = match ($scenario) {
            'eviction' => [
                $task('target-initial', 'target-workflow', 'target-run', $fullHistory, 'cold_replay'),
                $task('other-initial', 'other-workflow', 'other-run', $fullHistory, 'cold_replay'),
                $task('target-fallback', 'target-workflow', 'target-run', $suffix, 'sticky_hit_expected'),
            ],
            'expiry' => [
                $task('target-initial', 'target-workflow', 'target-run', $fullHistory, 'cold_replay'),
                $task('target-fallback', 'target-workflow', 'target-run', $suffix, 'sticky_hit_expected'),
            ],
            'build_mismatch' => [
                $task('target-fallback', 'target-workflow', 'target-run', $suffix, 'sticky_hit_expected'),
            ],
            default => throw new \LogicException("Unknown sticky fallback scenario {$scenario}."),
        };
        $commands = [];
        $historyFetches = 0;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $_headers,
            ?array $body,
        ) use (&$tasks, &$commands, &$historyFetches, $fullHistory): array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $next = array_shift($tasks);

                return ['poll_status' => 'leased', 'task' => $next];
            }
            if (str_ends_with($uri, '/history')) {
                ++$historyFetches;
                self::assertSame(base64_encode('0'), $body['next_history_page_token'] ?? null);

                return [
                    'history_events' => $fullHistory,
                    'next_history_page_token' => null,
                ];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                preg_match('~/workflow-tasks/([^/]+)/heartbeat$~', $uri, $matches);

                return [
                    'task_id' => $matches[1] ?? '',
                    'workflow_task_attempt' => 1,
                    'lease_owner' => 'sticky-worker',
                    'renewed' => true,
                ];
            }
            if (str_ends_with($uri, '/complete')) {
                preg_match('~/workflow-tasks/([^/]+)/complete$~', $uri, $matches);
                $commands[$matches[1] ?? ''] = $body;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $now = 0.0;
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'sticky-workers',
            workerId: 'sticky-worker',
            buildId: 'build-b',
            clock: static function () use (&$now): float {
                return $now;
            },
            stickyCacheCapacity: $scenario === 'eviction' ? 1 : 10,
            stickyCacheTtlSeconds: $scenario === 'expiry' ? 1 : 300,
        );
        $worker->registerWorkflow(
            'sticky.workflow',
            static fn (WorkflowContext $context): array => ['result' => $context->activity('sticky.activity')],
        );

        if ($scenario === 'build_mismatch') {
            $property = new \ReflectionProperty($worker, 'stickyCache');
            $cache = $property->getValue($worker);
            self::assertInstanceOf(StickyWorkflowCache::class, $cache);
            $cache->remember('target-workflow', 'target-run', 'build-a', $fullHistory);
        }

        $taskCount = count($tasks);
        for ($index = 0; $index < $taskCount; ++$index) {
            self::assertTrue($worker->tick(0), "{$scenario} workflow task {$index} was not handled.");
            if ($scenario === 'expiry' && $index === 0) {
                $now = 2.0;
            }
        }

        $initial = $commands['target-initial']['commands'] ?? null;
        $fallback = $commands['target-fallback']['commands'] ?? null;
        self::assertTrue($initial === null || is_array($initial));
        self::assertIsArray($fallback);

        return [
            'initial_commands' => $initial,
            'fallback_commands' => $fallback,
            'history_fetches' => $historyFetches,
            'metrics' => $worker->stickyCacheMetrics(),
        ];
    }
}
