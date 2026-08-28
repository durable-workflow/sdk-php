<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\DurableOperationHandle;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ReplayerTest extends TestCase
{
    public function testNewActivityProducesScheduleCommand(): void
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay(self::workflow(), [], ['Ada'], 'php-workers');

        self::assertSame('schedule_activity', $result->commands[0]['type']);
        self::assertSame(['Ada'], $codec->decodeEnvelope($result->commands[0]['arguments']));
    }

    public function testCompletedActivityReplaysIntoWorkflowResult(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'greet']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('hello, Ada')]],
        ];
        $result = (new Replayer($codec))->replay(self::workflow(), $history, ['Ada'], 'php-workers');

        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame(['message' => 'hello, Ada'], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testJsonUnsafeWorkflowFailureRetainsCompletedLocalActivityCommand(): void
    {
        $calls = 0;
        $workflow = static function (WorkflowContext $context): never {
            $context->localActivity('write-once');

            throw self::jsonUnsafeWorkflowFailure();
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
        self::assertInstanceOf(\RuntimeException::class, $result->terminalFailure);
        self::assertSame('Workflow failure after a recorded side effect.', $result->terminalFailure->getMessage());
        self::assertFalse(json_encode($result->terminalFailure::class));
        self::assertSame(JSON_ERROR_UTF8, json_last_error());
        self::assertJson(json_encode($result->commands, JSON_THROW_ON_ERROR));

        $record = $result->commands[0];
        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'write-once',
                    'execution_mode' => 'local',
                ],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => 'write-once',
                    'execution_mode' => 'local',
                    'outcome' => 'completed',
                    'result' => $record['result'],
                    'attempts' => $record['attempts'],
                ],
            ],
        ];

        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
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
            self::fail('Cold replay must restore the terminal workflow failure.');
        } catch (\RuntimeException $exception) {
            self::assertSame('Workflow failure after a recorded side effect.', $exception->getMessage());
            self::assertFalse(json_encode($exception::class));
            self::assertSame(JSON_ERROR_UTF8, json_last_error());
        }

        self::assertSame(1, $calls, 'Cold replay repeated the completed local activity side effect.');
    }

    private static function jsonUnsafeWorkflowFailure(): \RuntimeException
    {
        $shortName = 'ReplayerJsonUnsafe'.chr(0xff).'Failure';
        $className = __NAMESPACE__.'\\'.$shortName;
        if (!class_exists($className, false)) {
            eval('namespace '.__NAMESPACE__."; class {$shortName} extends \\RuntimeException {}");
        }

        $exception = new $className('Workflow failure after a recorded side effect.');
        self::assertInstanceOf(\RuntimeException::class, $exception);

        return $exception;
    }

    public function testChangedCommandOrderFailsDeterministically(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1]]];

        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer($codec))->replay(self::workflow(), $history, ['Ada'], 'php-workers');
    }

    public function testChangedActivityTypeFailsDeterministically(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [[
            'event_type' => 'ActivityScheduled',
            'payload' => ['sequence' => 1, 'activity_type' => 'send-email'],
        ]];

        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer($codec))->replay(self::workflow(), $history, ['Ada'], 'php-workers');
    }

    public function testDirectCompletionRejectsUnconsumedRecordedActivity(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [[
            'event_type' => 'ActivityScheduled',
            'payload' => ['sequence' => 7, 'activity_type' => 'greet'],
        ]];
        $workflow = static fn (WorkflowContext $context): array => ['message' => 'changed'];

        try {
            (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
            self::fail('Direct completion must not bypass recorded durable history.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame(7, $exception->sequence);
            self::assertSame('activity', $exception->expected);
            self::assertSame('complete_workflow', $exception->actual);
        }
    }

    public function testContinueAsNewRejectsUnconsumedRecordedTimer(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [[
            'event_type' => 'TimerScheduled',
            'payload' => ['sequence' => 11, 'delay_seconds' => 30],
        ]];
        $workflow = static function (WorkflowContext $context): never {
            $context->continueAsNew(['changed']);
        };

        try {
            (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
            self::fail('Continue-as-new must not bypass recorded durable history.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame(11, $exception->sequence);
            self::assertSame('timer', $exception->expected);
            self::assertSame('continue_as_new', $exception->actual);
        }
    }

    public function testContinueAsNewStopsTheCurrentFiberWithItsProtocolCommand(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): never {
            $context->continueAsNew(['next'], 'renamed-workflow', 'next-queue');
        };

        $result = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertSame('continue_as_new', $result->commands[0]['type']);
        self::assertSame(['next'], $codec->decodeEnvelope($result->commands[0]['arguments']));
        self::assertSame('renamed-workflow', $result->commands[0]['workflow_type']);
        self::assertSame('next-queue', $result->commands[0]['queue']);
    }

    public function testSearchAttributeUpsertUsesWorkerProtocolCommandShape(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertSearchAttributes(['status' => 'processing']);

            return 'search attributes recorded';
        };

        $result = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertSame([
            'type' => 'upsert_search_attributes',
            'attributes' => ['status' => 'processing'],
        ], $result->commands[0]);
    }

    public function testMemoUpsertUsesIdiomaticCommandShapeAndReplaysByEntries(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertMemo([
                'text' => 'same',
                'nested' => ['beta' => 2, 'alpha' => 1],
                'long' => 7,
                'double' => 7.0,
                'binary' => AvroBinaryValue::fromBytes('same'),
            ]);

            return 'memo recorded';
        };

        $initial = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
        $serverEntries = [
            'codec' => 'avro',
            'blob' => 'wwHioz3/VYAiNw4KDGJpbmFyeQgIc2FtZQxkb3VibGUGAAAAAAAAHEAIbG9uZwQODG5lc3RlZA4ECmFscGhhBAIIYmV0YQQEAAh0ZXh0CghzYW1lAA==',
        ];

        self::assertSame('upsert_memo', $initial->commands[0]['type']);
        self::assertSame(['codec', 'blob'], array_keys($initial->commands[0]['entries']));
        self::assertSame($serverEntries, $initial->commands[0]['entries']);
        $entries = $codec->decodeEnvelope($initial->commands[0]['entries']);
        self::assertSame(['binary', 'double', 'long', 'nested', 'text'], array_keys($entries));
        self::assertInstanceOf(AvroBinaryValue::class, $entries['binary']);
        self::assertSame('same', $entries['binary']->bytes);
        self::assertSame(7.0, $entries['double']);
        self::assertSame(7, $entries['long']);
        self::assertSame(['alpha' => 1, 'beta' => 2], $entries['nested']);
        self::assertSame('same', $entries['text']);

        $replay = (new Replayer($codec))->replay($workflow, [[
            'event_type' => 'MemoUpserted',
            'payload' => [
                'sequence' => 1,
                'entries' => $serverEntries,
                'merged' => $serverEntries,
            ],
        ]], [], 'php-workers');

        self::assertCount(1, $replay->commands);
        self::assertSame('complete_workflow', $replay->commands[0]['type']);

        $changedTypes = static function (WorkflowContext $context): string {
            $context->upsertMemo([
                'text' => AvroBinaryValue::fromBytes('same'),
                'nested' => ['alpha' => 1, 'beta' => 2],
                'long' => 7.0,
                'double' => 7,
                'binary' => 'same',
            ]);

            return 'memo recorded';
        };

        try {
            (new Replayer($codec))->replay($changedTypes, [[
                'event_type' => 'MemoUpserted',
                'payload' => [
                    'sequence' => 1,
                    'entries' => $serverEntries,
                    'merged' => $serverEntries,
                ],
            ]], [], 'php-workers');
            self::fail('Memo replay identity must preserve long, double, bytes, and text types.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('workflow_nondeterministic', $exception->reason);
            self::assertNotSame($exception->expected, $exception->actual);
        }
    }

    public function testMemoReplayRejectsChangedLogicalUpdate(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertMemo(['stage' => 'changed']);

            return 'done';
        };

        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer($codec))->replay($workflow, [[
            'event_type' => 'MemoUpserted',
            'payload' => [
                'sequence' => 1,
                'entries' => $codec->envelope(['stage' => 'original']),
                'merged' => $codec->envelope(['stage' => 'original']),
            ],
        ]], [], 'php-workers');
    }

    public function testMemoReplayRejectsDuplicateOrMissingHistoryIdentity(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertMemo(['stage' => 'recorded']);

            return 'done';
        };
        $codec = new AvroPayloadCodec();
        $event = [
            'event_type' => 'MemoUpserted',
            'payload' => [
                'sequence' => 1,
                'entries' => $codec->envelope(['stage' => 'recorded']),
                'merged' => $codec->envelope(['stage' => 'recorded']),
            ],
        ];

        foreach ([[$event, $event], [[
            'event_type' => 'MemoUpserted',
            'payload' => [
                'entries' => $codec->envelope(['stage' => 'recorded']),
                'merged' => $codec->envelope(['stage' => 'recorded']),
            ],
        ]]] as $history) {
            try {
                (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers');
                self::fail('Incomplete or duplicate memo history identity must fail replay.');
            } catch (NonDeterministicWorkflow $exception) {
                self::assertContains(
                    $exception->reason,
                    ['duplicate_memo_upsert_record', 'memo_sequence_missing'],
                );
            }
        }
    }

    #[DataProvider('invalidMemoReplaySequences')]
    public function testMemoReplayRejectsInvalidSequence(mixed $sequence): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertMemo(['stage' => 'recorded']);

            return 'done';
        };

        try {
            (new Replayer($codec))->replay($workflow, [[
                'event_type' => 'MemoUpserted',
                'payload' => [
                    'sequence' => $sequence,
                    'entries' => $codec->envelope(['stage' => 'recorded']),
                    'merged' => $codec->envelope(['stage' => 'recorded']),
                ],
            ]], [], 'php-workers');
            self::fail('Memo replay identity must be a positive integer sequence.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('memo_sequence_invalid', $exception->reason);
            self::assertSame('positive integer sequence', $exception->expected);
        }
    }

    /** @return iterable<string, array{mixed}> */
    public static function invalidMemoReplaySequences(): iterable
    {
        yield 'zero' => [0];
        yield 'negative integer' => [-1];
        yield 'fractional number' => [1.5];
        yield 'numeric string' => ['1'];
    }

    public function testMemoAuthoringRejectsStructurallyInvalidEntries(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            $context->upsertMemo([str_repeat('x', 65) => 'invalid']);

            return 'unreachable';
        };

        $this->expectException(\LogicException::class);
        (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers');
    }

    public function testSideEffectRunsOnlyDuringInitialExecution(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $calls = 0;
        $workflow = static function (WorkflowContext $context) use (&$calls): array {
            $value = $context->sideEffect(static function () use (&$calls): string {
                ++$calls;

                return 'generated-once';
            });

            return ['value' => $value];
        };

        $initial = $replayer->replay($workflow, [], [], 'php-workers');

        self::assertSame(1, $calls);
        self::assertSame('record_side_effect', $initial->commands[0]['type']);
        self::assertSame('generated-once', $codec->decode($initial->commands[0]['result']));

        $replay = $replayer->replay($workflow, [[
            'event_type' => 'SideEffectRecorded',
            'payload' => ['sequence' => 1, 'result' => $initial->commands[0]['result']],
        ]], [], 'php-workers');

        self::assertSame(1, $calls);
        self::assertCount(1, $replay->commands);
        self::assertSame('complete_workflow', $replay->commands[0]['type']);
        self::assertSame(['value' => 'generated-once'], $codec->decodeEnvelope($replay->commands[0]['result']));
    }

    public function testPendingRecordedActivityWaitsForHistoryAndResumes(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $workflow = static function (WorkflowContext $context): array {
            $result = $context->activity('charge-card');

            return ['activity' => $result];
        };
        $scheduled = [
            'event_type' => 'ActivityScheduled',
            'payload' => ['sequence' => 4, 'activity_type' => 'charge-card'],
        ];

        $pending = $replayer->replay($workflow, [$scheduled], [], 'php-workers');

        self::assertSame([], $pending->commands);

        $resolved = $replayer->replay($workflow, [$scheduled, [
            'event_type' => 'ActivityCompleted',
            'payload' => [
                'sequence' => 4,
                'activity_type' => 'charge-card',
                'result' => $codec->envelope('charged'),
            ],
        ]], [], 'php-workers');

        self::assertSame('complete_workflow', $resolved->commands[0]['type']);
        self::assertSame(['activity' => 'charged'], $codec->decodeEnvelope($resolved->commands[0]['result']));
    }

    public function testRecordedActivityFailureIsThrownAtTheStraightLineCallSite(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            try {
                $context->activity('charge-card');
            } catch (ActivityFailed $failure) {
                return [
                    'caught' => $failure->getMessage(),
                    'activity_type' => $failure->activityType,
                    'failure_type' => $failure->failureType,
                ];
            }

            return ['caught' => null];
        };
        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => ['sequence' => 4, 'activity_type' => 'charge-card'],
            ],
            [
                'event_type' => 'ActivityFailed',
                'payload' => [
                    'sequence' => 4,
                    'activity_type' => 'charge-card',
                    'message' => 'card declined',
                    'exception_type' => 'PaymentDeclined',
                ],
            ],
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([
            'caught' => 'card declined',
            'activity_type' => 'charge-card',
            'failure_type' => 'PaymentDeclined',
        ], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testPendingRecordedTimerWaitsForHistoryAndResumes(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $workflow = static function (WorkflowContext $context): string {
            $context->sleep(5);

            return 'timer-fired';
        };
        $scheduled = [
            'event_type' => 'TimerScheduled',
            'payload' => ['sequence' => 8, 'delay_seconds' => 5],
        ];

        $pending = $replayer->replay($workflow, [$scheduled], [], 'php-workers');

        self::assertSame([], $pending->commands);

        $resolved = $replayer->replay($workflow, [$scheduled, [
            'event_type' => 'TimerFired',
            'payload' => ['sequence' => 8, 'delay_seconds' => 5],
        ]], [], 'php-workers');

        self::assertSame('complete_workflow', $resolved->commands[0]['type']);
        self::assertSame('timer-fired', $codec->decodeEnvelope($resolved->commands[0]['result']));
    }

    public function testPendingRecordedChildWorkflowWaitsForHistoryAndResumes(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $workflow = static function (WorkflowContext $context): array {
            $result = $context->childWorkflow('invoice-child');

            return ['child' => $result];
        };
        $scheduled = [
            'event_type' => 'ChildWorkflowScheduled',
            'payload' => ['sequence' => 12, 'child_workflow_type' => 'invoice-child'],
        ];

        $pending = $replayer->replay($workflow, [$scheduled], [], 'php-workers');

        self::assertSame([], $pending->commands);

        $resolved = $replayer->replay($workflow, [$scheduled, [
            'event_type' => 'ChildRunCompleted',
            'payload' => [
                'sequence' => 12,
                'child_workflow_type' => 'invoice-child',
                'result' => $codec->envelope('invoiced'),
            ],
        ]], [], 'php-workers');

        self::assertSame('complete_workflow', $resolved->commands[0]['type']);
        self::assertSame(['child' => 'invoiced'], $codec->decodeEnvelope($resolved->commands[0]['result']));
    }

    public function testExecutionStateIsIsolatedAcrossNestedAndSequentialWorkflowFibers(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $outerContext = null;
        $outer = static function (WorkflowContext $context) use (&$outerContext, $replayer, $codec): array {
            $outerContext = $context;
            $nested = $context->sideEffect(static function () use (&$outerContext, $replayer, $codec): array {
                $nestedHistory = [
                    [
                        'event_type' => 'ActivityScheduled',
                        'payload' => ['sequence' => 1, 'activity_type' => 'nested-step'],
                    ],
                    [
                        'event_type' => 'ActivityCompleted',
                        'payload' => [
                            'sequence' => 1,
                            'activity_type' => 'nested-step',
                            'result' => $codec->envelope('nested-value'),
                        ],
                    ],
                ];
                $nestedWorkflow = static function (WorkflowContext $context) use (&$outerContext): array {
                    $crossExecutionRejected = false;
                    try {
                        $outerContext->activity('must-not-run');
                    } catch (\LogicException) {
                        $crossExecutionRejected = true;
                    }

                    return [
                        'workflow_id' => $context->workflowId,
                        'value' => $context->activity('nested-step'),
                        'cross_execution_rejected' => $crossExecutionRejected,
                    ];
                };
                $result = $replayer->replay(
                    $nestedWorkflow,
                    $nestedHistory,
                    [],
                    'php-workers',
                    ['workflow_id' => 'nested-workflow', 'run_id' => 'nested-run'],
                );

                return $codec->decodeEnvelope($result->commands[0]['result']);
            });

            return [
                'workflow_id' => $context->workflowId,
                'run_id' => $context->runId,
                'nested' => $nested,
            ];
        };

        $outerResult = $replayer->replay(
            $outer,
            [],
            [],
            'php-workers',
            ['workflow_id' => 'outer-workflow', 'run_id' => 'outer-run'],
        );
        $sequentialResult = $replayer->replay(
            static fn (WorkflowContext $context): array => [
                'workflow_id' => $context->workflowId,
                'run_id' => $context->runId,
            ],
            [],
            [],
            'php-workers',
            ['workflow_id' => 'later-workflow', 'run_id' => 'later-run'],
        );

        self::assertSame([
            'workflow_id' => 'outer-workflow',
            'run_id' => 'outer-run',
            'nested' => [
                'workflow_id' => 'nested-workflow',
                'value' => 'nested-value',
                'cross_execution_rejected' => true,
            ],
        ], $codec->decodeEnvelope($outerResult->commands[1]['result']));
        self::assertSame([
            'workflow_id' => 'later-workflow',
            'run_id' => 'later-run',
        ], $codec->decodeEnvelope($sequentialResult->commands[0]['result']));
    }

    public function testSignalsAndUpdatesRemainAvailableDuringStraightLineReplay(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            [
                'event_type' => 'SignalReceived',
                'payload' => [
                    'signal_name' => 'set-language',
                    'arguments' => $codec->envelope(['fr']),
                ],
            ],
            [
                'event_type' => 'UpdateAccepted',
                'payload' => [
                    'update_id' => 'rename-1',
                    'update_name' => 'rename',
                    'arguments' => $codec->envelope(['Grace']),
                ],
            ],
            [
                'event_type' => 'UpdateApplied',
                'payload' => [
                    'update_id' => 'rename-1',
                    'update_name' => 'rename',
                    'arguments' => $codec->envelope(['Grace']),
                ],
            ],
        ];
        $workflow = static fn (WorkflowContext $context): array => [
            'signals' => $context->signals('set-language'),
            'updates' => $context->updates('rename'),
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([
            'signals' => [['fr']],
            'updates' => [['Grace']],
        ], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testFalseConditionEmitsPublishedConditionWaitCommand(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->waitCondition(
                static fn (): bool => false,
                key: 'approval.ready',
                timeout: 60.2,
            );

            return 'approved';
        };

        $result = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertSame('open_condition_wait', $result->commands[0]['type']);
        self::assertSame('approval.ready', $result->commands[0]['condition_key']);
        self::assertSame(61, $result->commands[0]['timeout_seconds']);
        self::assertMatchesRegularExpression(
            '/\Asha256:[0-9a-f]{64}\z/',
            $result->commands[0]['condition_definition_fingerprint'],
        );
    }

    public function testImmediatelySatisfiedAndZeroTimeoutConditionsDoNotOpenServerWaits(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => [
            'satisfied' => $context->waitCondition(static fn (): bool => true, key: 'already-ready'),
            'zero_timeout' => $context->waitCondition(static fn (): bool => false, key: 'no-wait', timeout: 0),
        ];

        $result = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertCount(1, $result->commands);
        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame([
            'satisfied' => true,
            'zero_timeout' => false,
        ], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testSignalReevaluatesAndSatisfiesOpenCondition(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            $satisfied = $context->waitCondition(
                static fn (): bool => $context->signals('approve') !== [],
                key: 'approval',
                timeout: 30,
            );

            return ['satisfied' => $satisfied];
        };
        $history = [
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 4,
                    'condition_wait_id' => 'wait-approval',
                    'condition_key' => 'approval',
                    'timeout_seconds' => 30,
                ],
            ],
            [
                'event_type' => 'SignalReceived',
                'payload' => [
                    'workflow_sequence' => 4,
                    'signal_name' => 'approve',
                    'arguments' => $codec->envelope(['Ada']),
                ],
            ],
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame(['satisfied' => true], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testUpdateReevaluatesAndSatisfiesOpenCondition(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            $satisfied = $context->waitCondition(
                static fn (): bool => ($context->updates('approve')[0][0] ?? false) === true,
                key: 'update-approval',
            );

            return ['satisfied' => $satisfied];
        };
        $history = [
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 7,
                    'condition_wait_id' => 'wait-update',
                    'condition_key' => 'update-approval',
                ],
            ],
            [
                'event_type' => 'UpdateApplied',
                'payload' => [
                    'sequence' => 7,
                    'update_id' => 'update-1',
                    'update_name' => 'approve',
                    'arguments' => $codec->envelope([true]),
                ],
            ],
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame(['satisfied' => true], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testConditionTimeoutResumesFalseWithoutAdvancingAnOrdinaryTimer(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $satisfied = $context->waitCondition(
                static fn (): bool => false,
                key: 'approval',
                timeout: 10,
            );
            if (!$satisfied) {
                $context->sleep(60);
            }

            return 'done';
        };
        $history = [
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 8,
                    'condition_wait_id' => 'condition:8',
                    'condition_key' => 'approval',
                    'timeout_seconds' => 10,
                ],
            ],
            [
                'event_type' => 'TimerScheduled',
                'payload' => [
                    'sequence' => 9,
                    'timer_id' => 'condition-timer:9',
                    'timer_kind' => 'condition_timeout',
                    'condition_wait_id' => 'condition:8',
                    'delay_seconds' => 10,
                ],
            ],
            [
                'event_type' => 'TimerFired',
                'payload' => [
                    'sequence' => 9,
                    'timer_id' => 'condition-timer:9',
                    'timer_kind' => 'condition_timeout',
                    'condition_wait_id' => 'condition:8',
                    'delay_seconds' => 10,
                ],
            ],
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([[
            'type' => 'start_timer',
            'delay_seconds' => 60,
        ]], $result->commands);
    }

    public function testOpenConditionStateSurvivesFreshReplayersAndReopensAfterFalseExternalInput(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): bool {
            return $context->waitCondition(
                static fn (): bool => count($context->signals('vote')) >= 2,
                key: 'two-votes',
                timeout: 120,
            );
        };
        $history = [[
            'event_type' => 'ConditionWaitOpened',
            'payload' => [
                'sequence' => 3,
                'condition_wait_id' => 'wait-votes-1',
                'condition_key' => 'two-votes',
                'timeout_seconds' => 120,
            ],
        ], [
            'event_type' => 'SignalReceived',
            'payload' => [
                'workflow_sequence' => 3,
                'signal_name' => 'vote',
                'arguments' => $codec->envelope(['first']),
            ],
        ]];

        $firstWorker = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
        $restartedWorker = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('open_condition_wait', $firstWorker->commands[0]['type']);
        self::assertSame($firstWorker->commands, $restartedWorker->commands);
    }

    public function testRepeatedPhysicalOpensReplayAsOneLogicalCondition(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->waitCondition(
                static fn (): bool => count($context->signals('vote')) >= 2,
                key: 'two-votes',
            );
        };
        $history = [
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 3,
                    'condition_wait_id' => 'wait-votes-1',
                    'condition_key' => 'two-votes',
                ],
            ],
            [
                'event_type' => 'SignalReceived',
                'payload' => [
                    'workflow_sequence' => 3,
                    'signal_name' => 'vote',
                    'arguments' => $codec->envelope(['first']),
                ],
            ],
            [
                'event_type' => 'ConditionWaitSatisfied',
                'payload' => [
                    'sequence' => 3,
                    'condition_wait_id' => 'wait-votes-1',
                    'condition_key' => 'two-votes',
                ],
            ],
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 5,
                    'condition_wait_id' => 'wait-votes-2',
                    'condition_key' => 'two-votes',
                ],
            ],
            [
                'event_type' => 'SignalReceived',
                'payload' => [
                    'workflow_sequence' => 5,
                    'signal_name' => 'vote',
                    'arguments' => $codec->envelope(['second']),
                ],
            ],
            [
                'event_type' => 'ConditionWaitSatisfied',
                'payload' => [
                    'sequence' => 5,
                    'condition_wait_id' => 'wait-votes-2',
                    'condition_key' => 'two-votes',
                ],
            ],
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertTrue($codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testReplayRejectsChangedConditionIdentityPredicateAndTimeout(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->waitCondition(static fn (): bool => false, key: 'current', timeout: 20);
        };
        $initial = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
        $fingerprint = $initial->commands[0]['condition_definition_fingerprint'];

        foreach ([
            ['recorded', $fingerprint, 20],
            ['current', 'sha256:'.str_repeat('0', 64), 20],
            ['current', $fingerprint, 19],
        ] as [$recordedKey, $recordedFingerprint, $recordedTimeout]) {
            try {
                (new Replayer($codec))->replay($workflow, [[
                    'event_type' => 'ConditionWaitOpened',
                    'payload' => [
                        'sequence' => 12,
                        'condition_wait_id' => 'wait-mismatch',
                        'condition_key' => $recordedKey,
                        'condition_definition_fingerprint' => $recordedFingerprint,
                        'timeout_seconds' => $recordedTimeout,
                    ],
                ]], [], 'php-workers');
                self::fail('Changed condition wait definitions must fail replay.');
            } catch (NonDeterministicWorkflow $exception) {
                self::assertSame(12, $exception->sequence);
            }
        }
    }

    #[DataProvider('terminalChildOutcomeProvider')]
    public function testTerminalChildOutcomeRetainsIdentityForDeterminism(string $eventType): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            [
                'event_type' => 'ChildWorkflowScheduled',
                'payload' => ['sequence' => 16, 'child_workflow_type' => 'recorded-child'],
            ],
            [
                'event_type' => $eventType,
                'payload' => [
                    'sequence' => 16,
                    'child_workflow_type' => 'recorded-child',
                    'message' => 'Child did not complete.',
                ],
            ],
        ];
        $workflow = static function (WorkflowContext $context): void {
            $context->childWorkflow('changed-child');
        };

        try {
            (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
            self::fail("{$eventType} must retain the recorded child workflow identity.");
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame(16, $exception->sequence);
            self::assertSame('recorded-child', $exception->expected);
            self::assertSame('changed-child', $exception->actual);
        }
    }

    public function testParallelSchedulesEveryMixedNestedLeafWithStableMetadata(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->activity('fetch-a'),
            static fn () => $context->all([
                static fn () => $context->childWorkflow('enrich-child'),
                static function () use ($context): void {
                    $context->sleep(5);
                },
            ]),
        ]);

        $commands = (new Replayer($codec))->replay($workflow, [], [], 'php-workers')->commands;

        self::assertSame(['schedule_activity', 'start_child_workflow', 'start_timer'], array_column($commands, 'type'));
        self::assertSame('parallel-calls:1:3', $commands[0]['parallel_group_id']);
        self::assertSame('parallel-calls:2:2', $commands[1]['parallel_group_id']);
        self::assertSame('parallel-calls:2:2', $commands[2]['parallel_group_id']);
        self::assertSame([0], array_column($commands[0]['parallel_group_path'], 'parallel_group_index'));
        self::assertSame([1, 0], array_column($commands[1]['parallel_group_path'], 'parallel_group_index'));
        self::assertSame([2, 1], array_column($commands[2]['parallel_group_path'], 'parallel_group_index'));
    }

    public function testSelectionSchedulesEveryMemberWithStableKeysAndMode(): void
    {
        $workflow = static fn (WorkflowContext $context) => $context->select([
            'work' => static fn () => $context->activity('lookup'),
            'deadline' => static function () use ($context): void {
                $context->sleep(5);
            },
        ]);

        $commands = (new Replayer(new AvroPayloadCodec()))->replay(
            $workflow,
            [],
            [],
            'php-workers',
        )->commands;

        self::assertSame(['schedule_activity', 'start_timer'], array_column($commands, 'type'));
        self::assertSame(['work', 'deadline'], array_column($commands, 'selection_member_key'));
        self::assertSame(['select', 'select'], array_column($commands, 'parallel_group_mode'));
        self::assertSame(['select-calls:1:2', 'select-calls:1:2'], array_column($commands, 'parallel_group_id'));
    }

    #[DataProvider('selectedExternalInputProvider')]
    public function testSelectedEventWaitAdvancesOnlyThroughRecordedResolution(
        string $inputKind,
        string $eventType,
    ): void {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context) use ($inputKind): array {
            $selected = $context->select([
                'event' => static fn (): bool => $context->waitCondition(
                    static fn (): bool => match ($inputKind) {
                        'signal' => $context->signals('approved') !== [],
                        'update' => $context->updates('approved') !== [],
                        'message' => $context->hasPendingMessageStreamMessages('orders'),
                    },
                    key: "selected-{$inputKind}",
                ),
                'deadline' => static function () use ($context): void {
                    $context->sleep(60);
                },
            ]);

            return ['key' => $selected->key, 'value' => $selected->result()];
        };

        $scheduled = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertSame(['open_condition_wait', 'start_timer'], array_column($scheduled->commands, 'type'));

        $condition = $scheduled->commands[0];
        unset($condition['type']);
        $condition['sequence'] = 1;
        $condition['condition_wait_id'] = 'selected-event-wait';
        $timer = $scheduled->commands[1];
        unset($timer['type']);
        $timer['sequence'] = 2;
        $timer['timer_id'] = 'selected-deadline';
        $inputPayload = match ($inputKind) {
            'signal' => [
                'signal_name' => 'approved',
                'arguments' => $codec->envelope(['yes']),
            ],
            'update' => [
                'update_id' => 'approval-update',
                'update_name' => 'approved',
                'arguments' => $codec->envelope(['yes']),
            ],
            'message' => [
                'signal_name' => WorkflowContext::MESSAGE_STREAM_SIGNAL,
                'value' => $codec->envelope([[
                    'schema' => WorkflowContext::MESSAGE_STREAM_SCHEMA,
                    'stream_name' => 'orders',
                    'message_id' => 'order-message',
                    'position' => 1,
                    'payload_envelope' => $codec->envelope([42]),
                ]]),
            ],
        };
        $unrelatedInputPayload = match ($inputKind) {
            'signal' => [
                'signal_name' => 'unrelated',
                'arguments' => $codec->envelope(['no']),
            ],
            'update' => [
                'update_id' => 'unrelated-update',
                'update_name' => 'unrelated',
                'arguments' => $codec->envelope(['no']),
            ],
            'message' => [
                'signal_name' => WorkflowContext::MESSAGE_STREAM_SIGNAL,
                'value' => $codec->envelope([[
                    'schema' => WorkflowContext::MESSAGE_STREAM_SCHEMA,
                    'stream_name' => 'other-orders',
                    'message_id' => 'unrelated-message',
                    'position' => 1,
                    'payload_envelope' => $codec->envelope([0]),
                ]]),
            ],
        };
        $openingHistory = [
            [
                'id' => 'event-ConditionWaitOpened-1',
                'event_type' => 'ConditionWaitOpened',
                'payload' => $condition,
            ],
            [
                'id' => 'event-TimerScheduled-2',
                'event_type' => 'TimerScheduled',
                'payload' => $timer,
            ],
        ];
        $stillWaiting = (new Replayer($codec))->replay($workflow, [
            ...$openingHistory,
            [
                'id' => "unrelated-{$inputKind}-input",
                'event_type' => $eventType,
                'payload' => $unrelatedInputPayload,
            ],
        ], [], 'php-workers');

        self::assertSame(['open_condition_wait'], array_column($stillWaiting->commands, 'type'));

        $history = [
            ...$openingHistory,
            [
                'id' => "selected-{$inputKind}-input",
                'event_type' => $eventType,
                'payload' => $inputPayload,
            ],
        ];
        $awaitingMarker = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([], $awaitingMarker->commands);

        $satisfied = [
            'sequence' => 1,
            'condition_wait_id' => 'selected-event-wait',
            'condition_key' => "selected-{$inputKind}",
            ...$condition,
        ];
        $resolved = (new Replayer($codec))->replay($workflow, [
            ...$history,
            [
                'id' => 'event-ConditionWaitSatisfied-1',
                'event_type' => 'ConditionWaitSatisfied',
                'payload' => $satisfied,
            ],
            self::selectionResolved('event', 0, 1, 'condition', 'selected-event-wait'),
        ], [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($resolved->commands, 'type'));
        self::assertSame(
            ['key' => 'event', 'value' => true],
            $codec->decodeEnvelope($resolved->commands[0]['result']),
        );
    }

    public function testSelectionRejectsDuplicateGeneratorKeys(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            $operations = (static function () use ($context): \Generator {
                yield 'duplicate' => static fn () => $context->activity('first');
                yield 'duplicate' => static fn () => $context->activity('second');
            })();

            return $context->select($operations);
        };

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('member key duplicate is duplicated');

        (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers');
    }

    public function testSelectionRejectsEmptyAndNegativeKeysButPreservesValidKeys(): void
    {
        foreach (['', -1] as $key) {
            $workflow = static fn (WorkflowContext $context): mixed => $context->select([
                $key => static fn () => $context->activity('invalid'),
            ]);

            try {
                (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers');
            } catch (\LogicException $exception) {
                self::assertStringContainsString(
                    'non-empty strings or non-negative integers',
                    $exception->getMessage(),
                );

                continue;
            }

            self::fail(sprintf('Selection accepted invalid key [%s].', (string) $key));
        }

        $workflow = static fn (WorkflowContext $context): mixed => $context->select([
            0 => static fn () => $context->activity('numeric'),
            'named' => static function () use ($context): void {
                $context->sleep(1);
            },
        ]);
        $commands = (new Replayer(new AvroPayloadCodec()))
            ->replay($workflow, [], [], 'php-workers')
            ->commands;

        self::assertSame([0, 'named'], array_column($commands, 'selection_member_key'));
    }

    public function testSelectionReplayRejectsOutOfDomainRecordedMemberKeys(): void
    {
        $workflow = static fn (WorkflowContext $context): mixed => $context->select([
            'work' => static fn () => $context->activity('lookup'),
            'deadline' => static function () use ($context): void {
                $context->sleep(5);
            },
        ]);

        foreach (['', -1] as $key) {
            $paths = self::selectionPaths(['work', 'deadline'], ['activity', 'timer']);
            $paths[0][0]['selection_member_key'] = $key;
            $history = [self::parallelEvent('ActivityScheduled', 1, 'lookup', $paths[0])];

            try {
                (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers');
            } catch (NonDeterministicWorkflow $exception) {
                self::assertSame('selection_group_metadata_invalid', $exception->reason);

                continue;
            }

            self::fail(sprintf('Selection replay accepted invalid recorded key [%s].', (string) $key));
        }
    }

    public function testSelectionRejectsNonScalarIterableKeys(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            $operations = new class($context) implements \Iterator {
                private bool $valid = true;

                public function __construct(private readonly WorkflowContext $context)
                {
                }

                public function current(): callable
                {
                    return fn () => $this->context->activity('first');
                }

                public function key(): object
                {
                    return new \stdClass();
                }

                public function next(): void
                {
                    $this->valid = false;
                }

                public function rewind(): void
                {
                    $this->valid = true;
                }

                public function valid(): bool
                {
                    return $this->valid;
                }
            };

            return (new \ReflectionMethod($context, 'select'))->invoke($context, $operations);
        };

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('member keys must be integers or strings');

        (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers');
    }

    public function testNestedSelectionCommandsCarryGroupKindForOneAndManyLeaves(): void
    {
        foreach ([1, 2] as $leafCount) {
            $workflow = static function (WorkflowContext $context) use ($leafCount): mixed {
                $nested = [];
                for ($index = 0; $index < $leafCount; ++$index) {
                    $nested[] = static fn () => $context->activity("nested-{$index}");
                }

                return $context->select([
                    'nested' => static fn () => $context->all($nested),
                    'deadline' => static function () use ($context): void {
                        $context->sleep(5);
                    },
                ]);
            };

            $commands = (new Replayer(new AvroPayloadCodec()))->replay(
                $workflow,
                [],
                [],
                'php-workers',
            )->commands;

            self::assertCount($leafCount + 1, $commands);
            foreach (array_slice($commands, 0, $leafCount) as $command) {
                self::assertSame('group', $command['parallel_group_path'][0]['selection_member_kind']);
                self::assertSame($leafCount, $command['parallel_group_path'][0]['selection_member_size']);
            }
        }
    }

    public function testSelectionReplaysRecordedWinnerIndependentOfLaterCompletionOrder(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['work', 'deadline'], ['activity', 'timer']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'lookup', $paths[0]),
            self::parallelEvent('TimerScheduled', 2, '5', $paths[1]),
            self::parallelEvent('ActivityCompleted', 1, 'lookup', $paths[0], $codec->envelope('answer')),
            self::selectionResolved('work', 0, 1, 'activity', 'activity-1'),
            self::parallelEvent('TimerFired', 2, '5', $paths[1]),
        ];
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'work' => static fn () => $context->activity('lookup'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(5);
                },
            ]);

            return [
                'key' => $selected->key,
                'kind' => $selected->kind,
                'identity' => $selected->identity,
                'value' => $selected->result(),
                'remaining' => array_keys($selected->remaining()),
            ];
        };

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([
            'key' => 'work',
            'kind' => 'activity',
            'identity' => 'activity-1',
            'value' => 'answer',
            'remaining' => ['deadline'],
        ], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testColdReplayWaitsForSelectionResolutionMarkerThenUsesExactRecordedWinner(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['first', 'second'], ['activity', 'activity']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'first-activity', $paths[0]),
            self::parallelEvent('ActivityScheduled', 2, 'second-activity', $paths[1]),
            self::parallelEvent('ActivityCompleted', 1, 'first-activity', $paths[0], $codec->envelope('first-value')),
            self::parallelEvent('ActivityCompleted', 2, 'second-activity', $paths[1], $codec->envelope('second-value')),
        ];
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'first' => static fn () => $context->activity('first-activity'),
                'second' => static fn () => $context->activity('second-activity'),
            ]);

            return [
                'key' => $selected->key,
                'identity' => $selected->identity,
                'value' => $selected->result(),
            ];
        };

        $waiting = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([], $waiting->commands);

        $resolved = (new Replayer($codec))->replay(
            $workflow,
            [...$history, self::selectionResolved('second', 1, 2, 'activity', 'activity-2')],
            [],
            'php-workers',
        );

        self::assertSame(['complete_workflow'], array_column($resolved->commands, 'type'));
        self::assertSame([
            'key' => 'second',
            'identity' => 'activity-2',
            'value' => 'second-value',
        ], $codec->decodeEnvelope($resolved->commands[0]['result']));
    }

    public function testSelectionLoserCanBeAwaitedAfterColdReplay(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['work', 'deadline'], ['activity', 'timer']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'lookup', $paths[0]),
            self::parallelEvent('TimerScheduled', 2, '0', $paths[1]),
            self::parallelEvent('TimerFired', 2, '0', $paths[1]),
            self::selectionResolved('deadline', 1, 2, 'timer', 'timer-2'),
            self::parallelEvent('ActivityCompleted', 1, 'lookup', $paths[0], $codec->envelope('late answer')),
        ];
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'work' => static fn () => $context->activity('lookup'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);

            return ['winner' => $selected->key, 'work' => $selected->handles['work']->await()];
        };

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(
            ['winner' => 'deadline', 'work' => 'late answer'],
            $codec->decodeEnvelope($result->commands[0]['result']),
        );
    }

    public function testSelectionHandleCannotResolveAgainstAnotherRunHistory(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['first', 'second'], ['activity', 'activity']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'first-activity', $paths[0]),
            self::parallelEvent('ActivityScheduled', 2, 'second-activity', $paths[1]),
            self::parallelEvent('ActivityCompleted', 1, 'first-activity', $paths[0], $codec->envelope('first')),
            self::parallelEvent('ActivityCompleted', 2, 'second-activity', $paths[1], $codec->envelope('second')),
            self::selectionResolved('first', 0, 1, 'activity', 'activity-1'),
        ];
        $handle = null;
        $workflow = static function (WorkflowContext $context) use (&$handle): array {
            $previousHandle = $handle;
            $selected = $context->select([
                'first' => static fn () => $context->activity('first-activity'),
                'second' => static fn () => $context->activity('second-activity'),
            ]);
            $handle = $selected->handles['second'];
            if ($previousHandle instanceof DurableOperationHandle) {
                return ['reused' => $previousHandle->await()];
            }

            return ['winner' => $selected->key];
        };
        $replayer = new Replayer($codec);

        $first = $replayer->replay($workflow, $history, [], 'php-workers', [
            'workflow_id' => 'workflow-a',
            'run_id' => 'run-a',
        ]);
        self::assertSame(['complete_workflow'], array_column($first->commands, 'type'));

        try {
            $replayer->replay($workflow, $history, [], 'php-workers', [
                'workflow_id' => 'workflow-a',
                'run_id' => 'run-b',
            ]);
            self::fail('A selection handle must not resolve against another run history.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('durable_operation_handle_execution_mismatch', $exception->reason);
            self::assertSame(2, $exception->sequence);
        }
    }

    public function testSelectionMarkerRejectsMissingNestedMemberSchedules(): void
    {
        $deadline = [[
            'parallel_group_id' => 'select-calls:1:3',
            'parallel_group_kind' => 'mixed',
            'parallel_group_mode' => 'select',
            'parallel_group_base_sequence' => 1,
            'parallel_group_size' => 3,
            'parallel_group_index' => 0,
            'selection_member_key' => 'deadline',
            'selection_member_index' => 0,
            'selection_member_base_sequence' => 1,
            'selection_member_size' => 1,
            'selection_member_kind' => 'timer',
        ]];
        $history = [
            self::parallelEvent('TimerScheduled', 1, '0', $deadline),
            self::parallelEvent('TimerFired', 1, '0', $deadline),
            [
                'event_type' => 'SelectionResolved',
                'payload' => [
                    'selection_group_id' => 'select-calls:1:3',
                    'selection_group_base_sequence' => 1,
                    'selection_group_size' => 3,
                    'member_key' => 'deadline',
                    'member_index' => 0,
                    'member_base_sequence' => 1,
                    'member_size' => 1,
                    'operation_kind' => 'timer',
                    'operation_identity' => 'timer-1',
                    'outcome' => 'completed',
                    'resolution_event_id' => 'event-TimerFired-1',
                    'resolution_event_type' => 'TimerFired',
                ],
            ],
        ];
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
                'nested' => static fn () => $context->all([
                    static fn () => $context->activity('nested-first'),
                    static fn () => $context->activity('nested-second'),
                ]),
            ]);

            return ['winner' => $selected->key];
        };

        try {
            (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers');
            self::fail('A selection marker must not bypass missing nested member schedules.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('selection_resolution_incomplete_group', $exception->reason);
            self::assertSame(2, $exception->sequence);
        }
    }

    public function testFreshProcessConsumesRuntimeProducedSelectionHistory(): void
    {
        $fixturePath = __DIR__ . '/fixtures/durable-selection-runtime-history.json';
        self::assertSame(
            '51fd8b9c16e978dcef536a5c727b9fdc0ae724d9afc17d9a7837d219f41ee3ba',
            hash_file('sha256', $fixturePath),
        );
        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/fixtures/durable-selection-cold-replay.php', $fixturePath],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            dirname(__DIR__),
        );
        self::assertIsResource($process);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_close($process);

        self::assertSame(0, $status, $stderr === false ? '' : $stderr);
        self::assertNotFalse($stdout);
        $observed = json_decode($stdout, true, flags: JSON_THROW_ON_ERROR);
        self::assertIsInt($observed['process_id'] ?? null);
        self::assertNotSame(getmypid(), $observed['process_id']);
        unset($observed['process_id']);
        self::assertSame([
            'winner' => 'fast',
            'winner_value' => 'winner-value',
            'slow' => 'loser-value',
        ], $observed);
    }

    public function testSelectionChildCancellationUsesRunIdentityWhenBothChildIdsExist(): void
    {
        $paths = self::selectionPaths(['child', 'deadline'], ['child', 'timer']);
        $history = [
            self::parallelEvent('ChildWorkflowScheduled', 1, 'child-workflow', $paths[0]),
            self::parallelEvent('TimerScheduled', 2, '0', $paths[1]),
            self::parallelEvent('TimerFired', 2, '0', $paths[1]),
            self::selectionResolved('deadline', 1, 2, 'timer', 'timer-2'),
        ];
        $history[0]['payload']['child_workflow_instance_id'] = 'child-instance';
        $history[0]['payload']['child_workflow_run_id'] = 'child-run';
        $workflow = static function (WorkflowContext $context): void {
            $selected = $context->select([
                'child' => static fn () => $context->childWorkflow('child-workflow'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);
            $selected->handles['child']->cancel();
        };

        $result = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('cancel_selection_operation', $result->commands[0]['type']);
        self::assertSame('child-run', $result->commands[0]['operation_identity']);
    }

    public function testSelectionLoserCancellationEmitsOneIdempotentControlCommand(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['work', 'deadline'], ['activity', 'timer']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'lookup', $paths[0]),
            self::parallelEvent('TimerScheduled', 2, '0', $paths[1]),
            self::parallelEvent('TimerFired', 2, '0', $paths[1]),
            self::selectionResolved('deadline', 1, 2, 'timer', 'timer-2'),
        ];
        $history[0]['payload']['activity_execution_id'] = 'activity-work';
        $workflow = static function (WorkflowContext $context): mixed {
            $selected = $context->select([
                'work' => static fn () => $context->activity('lookup'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);

            $selected->handles['work']->cancel();

            return null;
        };

        $first = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
        self::assertSame(['cancel_selection_operation', 'complete_workflow'], array_column($first->commands, 'type'));
        self::assertSame('work', $first->commands[0]['member_key']);
        self::assertSame('activity-work', $first->commands[0]['operation_identity']);

        $history[] = [
            'event_type' => 'SelectionOperationCancelled',
            'payload' => [
                'selection_group_id' => 'select-calls:1:2',
                'member_key' => 'work',
                'member_index' => 0,
                'member_base_sequence' => 1,
                'member_size' => 1,
                'operation_kind' => 'activity',
                'operation_identity' => 'activity-work',
                'cancelled_at' => '2026-08-27T00:00:00Z',
            ],
        ];
        $replay = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');
        self::assertSame(['complete_workflow'], array_column($replay->commands, 'type'));
    }

    public function testSelectionCompletionBeforeCancellationRemainsAwaitable(): void
    {
        $codec = new AvroPayloadCodec();
        $paths = self::selectionPaths(['work', 'deadline'], ['activity', 'timer']);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'lookup', $paths[0]),
            self::parallelEvent('TimerScheduled', 2, '0', $paths[1]),
            self::parallelEvent('TimerFired', 2, '0', $paths[1]),
            self::selectionResolved('deadline', 1, 2, 'timer', 'timer-2'),
            self::parallelEvent('ActivityCompleted', 1, 'lookup', $paths[0], $codec->envelope('completed-first')),
        ];
        $history[0]['payload']['activity_execution_id'] = 'activity-work';
        $history[4]['payload']['activity_execution_id'] = 'activity-work';
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'work' => static fn () => $context->activity('lookup'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);

            $selected->handles['work']->cancel();

            return ['work' => $selected->handles['work']->await()];
        };

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['cancel_selection_operation', 'complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(
            ['work' => 'completed-first'],
            $codec->decodeEnvelope($result->commands[1]['result']),
        );
    }

    public function testNestedFailureBeforeCancellationRemainsTheAwaitedFailure(): void
    {
        $codec = new AvroPayloadCodec();
        $outer = static fn (int $flatIndex): array => [
            'parallel_group_id' => 'select-calls:1:3',
            'parallel_group_kind' => 'mixed',
            'parallel_group_mode' => 'select',
            'parallel_group_base_sequence' => 1,
            'parallel_group_size' => 3,
            'parallel_group_index' => $flatIndex,
            'selection_member_key' => 'nested',
            'selection_member_index' => 0,
            'selection_member_base_sequence' => 1,
            'selection_member_size' => 2,
            'selection_member_kind' => 'group',
        ];
        $inner = static fn (int $index): array => [
            'parallel_group_id' => 'parallel-activities:1:2',
            'parallel_group_kind' => 'activity',
            'parallel_group_base_sequence' => 1,
            'parallel_group_size' => 2,
            'parallel_group_index' => $index,
        ];
        $deadline = [
            'parallel_group_id' => 'select-calls:1:3',
            'parallel_group_kind' => 'mixed',
            'parallel_group_mode' => 'select',
            'parallel_group_base_sequence' => 1,
            'parallel_group_size' => 3,
            'parallel_group_index' => 2,
            'selection_member_key' => 'deadline',
            'selection_member_index' => 1,
            'selection_member_base_sequence' => 3,
            'selection_member_size' => 1,
            'selection_member_kind' => 'timer',
        ];
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'nested-first', [$outer(0), $inner(0)]),
            self::parallelEvent('ActivityScheduled', 2, 'nested-second', [$outer(1), $inner(1)]),
            self::parallelEvent('TimerScheduled', 3, '0', [$deadline]),
            self::parallelEvent('TimerFired', 3, '0', [$deadline]),
            [
                'event_type' => 'SelectionResolved',
                'payload' => [
                    'selection_group_id' => 'select-calls:1:3',
                    'selection_group_base_sequence' => 1,
                    'selection_group_size' => 3,
                    'member_key' => 'deadline',
                    'member_index' => 1,
                    'member_base_sequence' => 3,
                    'member_size' => 1,
                    'operation_kind' => 'timer',
                    'operation_identity' => 'timer-3',
                    'outcome' => 'completed',
                    'resolution_event_id' => 'event-TimerFired-3',
                    'resolution_event_type' => 'TimerFired',
                ],
            ],
            self::parallelEvent('ActivityFailed', 2, 'nested-second', [$outer(1), $inner(1)]),
        ];
        $workflow = static function (WorkflowContext $context): array {
            $selected = $context->select([
                'nested' => static fn () => $context->all([
                    static fn () => $context->activity('nested-first'),
                    static fn () => $context->activity('nested-second'),
                ]),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);
            $selected->handles['nested']->cancel();

            try {
                $selected->handles['nested']->await();
            } catch (ActivityFailed $failure) {
                return ['winner' => $selected->key, 'failure' => $failure::class];
            }

            return ['winner' => $selected->key, 'failure' => null];
        };

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['cancel_selection_operation', 'complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(
            ['winner' => 'deadline', 'failure' => ActivityFailed::class],
            $codec->decodeEnvelope($result->commands[1]['result']),
        );
    }

    public function testParallelReplayReturnsNestedDeclarationOrderAfterMixedCompletionOrder(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->activity('first'),
            static fn () => $context->all([
                static fn () => $context->childWorkflow('second'),
                static fn () => $context->activity('third'),
            ]),
        ]);
        $outer = [
            self::parallelEntry('mixed', 1, 3, 0),
            self::parallelEntry('mixed', 1, 3, 1),
            self::parallelEntry('mixed', 1, 3, 2),
        ];
        $paths = [
            [$outer[0]],
            [$outer[1], self::parallelEntry('mixed', 2, 2, 0)],
            [$outer[2], self::parallelEntry('mixed', 2, 2, 1)],
        ];
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'first', $paths[0]),
            self::parallelEvent('ChildWorkflowScheduled', 2, 'second', $paths[1]),
            self::parallelEvent('ActivityScheduled', 3, 'third', $paths[2]),
            self::parallelEvent('ActivityCompleted', 3, 'third', $paths[2], $codec->envelope('three')),
            self::parallelEvent('ActivityCompleted', 1, 'first', $paths[0], $codec->envelope('one')),
            self::parallelEvent('ActivityCompleted', 1, 'first', $paths[0], $codec->envelope('duplicate')),
            self::parallelEvent('ChildRunCompleted', 2, 'second', $paths[1], $codec->envelope('two')),
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['one', ['two', 'three']], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testParallelPartialRestartDoesNotRescheduleCompletedOrPendingMembers(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->activity('first'),
            static fn () => $context->activity('second'),
        ]);
        $paths = self::parallelPaths([
            ['activity', 1, 2, 0],
            ['activity', 1, 2, 1],
        ]);
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'first', $paths[0]),
            self::parallelEvent('ActivityScheduled', 2, 'second', $paths[1]),
            self::parallelEvent('ActivityCompleted', 2, 'second', $paths[1], $codec->envelope('two')),
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame([], $result->commands);
    }

    public function testParallelFirstRecordedFailureThrowsWhileLateMembersRemainPending(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            try {
                $context->all([
                    static fn () => $context->activity('first'),
                    static fn () => $context->childWorkflow('second'),
                ]);
            } catch (ActivityFailed $failure) {
                return $failure->activityType ?? 'missing';
            }

            return 'unexpected';
        };
        $paths = self::parallelPaths([
            ['mixed', 1, 2, 0],
            ['mixed', 1, 2, 1],
        ]);
        $failed = self::parallelEvent('ActivityFailed', 1, 'first', $paths[0]);
        $failed['payload']['message'] = 'first failed';
        $history = [
            self::parallelEvent('ActivityScheduled', 1, 'first', $paths[0]),
            self::parallelEvent('ChildWorkflowScheduled', 2, 'second', $paths[1]),
            $failed,
        ];

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('first', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testParallelChildFailureUsesItsTypedFailure(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            try {
                $context->all([static fn () => $context->childWorkflow('child')]);
            } catch (ChildWorkflowFailed $failure) {
                return $failure->workflowType ?? 'missing';
            }

            return 'unexpected';
        };
        $path = self::parallelPaths([['child', 1, 1, 0]])[0];
        $failed = self::parallelEvent('ChildRunFailed', 1, 'child', $path);
        $failed['payload']['message'] = 'child failed';

        $result = (new Replayer($codec))->replay($workflow, [
            self::parallelEvent('ChildWorkflowScheduled', 1, 'child', $path),
            $failed,
        ], [], 'php-workers');

        self::assertSame('child', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testParallelDeferredChildGroupSchedulesEveryMemberBeforeSuspending(): void
    {
        $commands = (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): array => $context->parallel([
                $context->deferChildWorkflow('first-child'),
                $context->deferChildWorkflow('second-child'),
            ]),
            [],
            [],
            'php-workers',
        )->commands;

        self::assertSame(['start_child_workflow', 'start_child_workflow'], array_column($commands, 'type'));
        self::assertSame(['parallel-children:1:2', 'parallel-children:1:2'], array_column(
            $commands,
            'parallel_group_id',
        ));
    }

    public function testParallelRejectsChangedGroupShapeAndMissingMetadata(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->activity('first'),
            static fn () => $context->activity('second'),
            static fn () => $context->activity('third'),
        ]);
        $path = self::parallelPaths([['activity', 1, 2, 0]])[0];

        foreach ([
            self::parallelEvent('ActivityScheduled', 1, 'first', $path),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'first']],
        ] as $event) {
            try {
                (new Replayer($codec))->replay($workflow, [$event], [], 'php-workers');
                self::fail('Incompatible parallel history must fail replay.');
            } catch (NonDeterministicWorkflow $exception) {
                self::assertContains($exception->reason, [
                    'parallel_group_shape_mismatch',
                    'parallel_group_metadata_missing',
                ]);
            }
        }
    }

    public function testParallelEmptyAndFanOutBoundFailBeforeTransport(): void
    {
        $codec = new AvroPayloadCodec();
        $empty = (new Replayer($codec))->replay(
            static fn (WorkflowContext $context): array => $context->all([]),
            [],
            [],
            'php-workers',
        );
        self::assertSame([], $codec->decodeEnvelope($empty->commands[0]['result']));

        try {
            (new Replayer($codec))->replay(
                static fn (WorkflowContext $context): array => $context->all([
                    static fn () => $context->all([]),
                ]),
                [],
                [],
                'php-workers',
            );
            self::fail('An empty nested parallel barrier must fail before transport.');
        } catch (\LogicException $exception) {
            self::assertSame(
                'WorkflowContext::all() does not allow an empty nested barrier because replay cannot identify it.',
                $exception->getMessage(),
            );
        }

        $workflow = static function (WorkflowContext $context): array {
            $operations = [];
            for ($index = 0; $index <= WorkflowContext::MAX_PARALLEL_OPERATIONS; ++$index) {
                $operations[] = $context->deferTimer(1);
            }

            return $context->all($operations);
        };

        $this->expectExceptionMessage('exceeds the deterministic limit');
        (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
    }

    /** @return iterable<string, array{string}> */
    public static function terminalChildOutcomeProvider(): iterable
    {
        yield 'failed' => ['ChildRunFailed'];
        yield 'cancelled' => ['ChildRunCancelled'];
        yield 'terminated' => ['ChildRunTerminated'];
    }

    /** @return iterable<string, array{string, string}> */
    public static function selectedExternalInputProvider(): iterable
    {
        yield 'signal event' => ['signal', 'SignalReceived'];
        yield 'workflow update' => ['update', 'UpdateApplied'];
        yield 'message stream event' => ['message', 'SignalReceived'];
    }

    /**
     * @param list<array{string, int, int, int}> $leaves
     * @return list<list<array<string, mixed>>>
     */
    private static function parallelPaths(array $leaves): array
    {
        $paths = [];
        foreach ($leaves as $leafIndex => [$innerKind, $innerBase, $innerSize, $innerIndex]) {
            $outerKind = count(array_unique(array_column($leaves, 0))) === 1 ? $innerKind : 'mixed';
            $outer = self::parallelEntry($outerKind, 1, count($leaves), $leafIndex);
            $inner = self::parallelEntry($innerKind, $innerBase, $innerSize, $innerIndex);
            $paths[] = $outer === $inner ? [$outer] : [$outer, $inner];
        }

        return $paths;
    }

    /** @return array<string, mixed> */
    private static function parallelEntry(string $kind, int $base, int $size, int $index): array
    {
        $prefix = match ($kind) {
            'activity' => 'parallel-activities',
            'child' => 'parallel-children',
            'timer' => 'parallel-timers',
            default => 'parallel-calls',
        };

        return [
            'parallel_group_id' => "{$prefix}:{$base}:{$size}",
            'parallel_group_kind' => $kind,
            'parallel_group_base_sequence' => $base,
            'parallel_group_size' => $size,
            'parallel_group_index' => $index,
        ];
    }

    /**
     * @param list<int|string> $keys
     * @param list<string> $kinds
     * @return list<list<array<string, mixed>>>
     */
    private static function selectionPaths(array $keys, array $kinds): array
    {
        $paths = [];
        $size = count($keys);
        $groupKind = count(array_unique($kinds)) === 1 ? $kinds[0] : 'mixed';
        foreach ($keys as $index => $key) {
            $paths[] = [[
                'parallel_group_id' => "select-calls:1:{$size}",
                'parallel_group_kind' => $groupKind,
                'parallel_group_mode' => 'select',
                'parallel_group_base_sequence' => 1,
                'parallel_group_size' => $size,
                'parallel_group_index' => $index,
                'selection_member_key' => $key,
                'selection_member_index' => $index,
                'selection_member_base_sequence' => 1 + $index,
                'selection_member_size' => 1,
                'selection_member_kind' => $kinds[$index],
            ]];
        }

        return $paths;
    }

    /** @return array<string, mixed> */
    private static function selectionResolved(
        int|string $key,
        int $index,
        int $baseSequence,
        string $kind,
        string $identity,
    ): array {
        return [
            'event_type' => 'SelectionResolved',
            'payload' => [
                'selection_group_id' => 'select-calls:1:2',
                'selection_group_base_sequence' => 1,
                'selection_group_size' => 2,
                'member_key' => $key,
                'member_index' => $index,
                'member_base_sequence' => $baseSequence,
                'member_size' => 1,
                'operation_kind' => $kind,
                'operation_identity' => $identity,
                'outcome' => 'completed',
                'resolution_event_id' => sprintf(
                    'event-%s-%d',
                    match ($kind) {
                        'activity' => 'ActivityCompleted',
                        'child' => 'ChildRunCompleted',
                        'condition' => 'ConditionWaitSatisfied',
                        default => 'TimerFired',
                    },
                    $baseSequence,
                ),
                'resolution_event_type' => match ($kind) {
                    'activity' => 'ActivityCompleted',
                    'child' => 'ChildRunCompleted',
                    'condition' => 'ConditionWaitSatisfied',
                    default => 'TimerFired',
                },
            ],
        ];
    }

    /**
     * @param list<array<string, mixed>> $path
     * @return array<string, mixed>
     */
    private static function parallelEvent(
        string $eventType,
        int $sequence,
        string $detail,
        array $path,
        mixed $result = null,
    ): array {
        $payload = [
            'sequence' => $sequence,
            ...$path[array_key_last($path)],
            'parallel_group_path' => $path,
        ];
        if (str_starts_with($eventType, 'Activity')) {
            $payload['activity_type'] = $detail;
            $payload['activity_execution_id'] = "activity-{$sequence}";
        } elseif (str_starts_with($eventType, 'Child')) {
            $payload['child_workflow_type'] = $detail;
            $payload['child_workflow_run_id'] = "child-run-{$sequence}";
        } else {
            $payload['delay_seconds'] = (int) $detail;
            $payload['timer_id'] = "timer-{$sequence}";
        }
        if ($result !== null) {
            $payload['result'] = $result;
        }

        return [
            'id' => "event-{$eventType}-{$sequence}",
            'event_type' => $eventType,
            'payload' => $payload,
        ];
    }

    private static function workflow(): callable
    {
        return static function (WorkflowContext $context, string $name): array {
            $message = $context->activity('greet', [$name]);

            return ['message' => $message];
        };
    }
}
