<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
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
        $workflow = static function (WorkflowContext $context): bool {
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
        $workflow = static function (WorkflowContext $context): bool {
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
        } elseif (str_starts_with($eventType, 'Child')) {
            $payload['child_workflow_type'] = $detail;
        } else {
            $payload['delay_seconds'] = (int) $detail;
        }
        if ($result !== null) {
            $payload['result'] = $result;
        }

        return ['event_type' => $eventType, 'payload' => $payload];
    }

    private static function workflow(): callable
    {
        return static function (WorkflowContext $context, string $name): array {
            $message = $context->activity('greet', [$name]);

            return ['message' => $message];
        };
    }
}
