<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
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
                        $outerContext?->activity('must-not-run');
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

    /** @return iterable<string, array{string}> */
    public static function terminalChildOutcomeProvider(): iterable
    {
        yield 'failed' => ['ChildRunFailed'];
        yield 'cancelled' => ['ChildRunCancelled'];
        yield 'terminated' => ['ChildRunTerminated'];
    }

    private static function workflow(): callable
    {
        return static function (WorkflowContext $context, string $name): array {
            $message = $context->activity('greet', [$name]);

            return ['message' => $message];
        };
    }
}
