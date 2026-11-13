<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * Workflow #1117 / #1121, Server #1193: when a run's own code closes it, each
 * activity it never awaited gets ActivityCancelled with cancellation_reason
 * run_closed (timers get TimerCancelled) before the terminal event, and an
 * abandon-policy activity is listed under abandoned_activities instead. The
 * workflow never observed those cancellations, so replay must reach the same
 * close as if they were absent.
 */
final class RunClosedSettlementReplayTest extends TestCase
{
    private const GREETING = 'Tests\\Fixtures\\V2\\TestGreetingActivity';
    private const FAILING = 'Tests\\Fixtures\\V2\\TestFailingActivity';
    private const TASK = ['workflow_id' => 'regression-workflow', 'run_id' => 'regression-inline'];

    public function testSelectionLoserCancelledAtCloseStillResolvesToTheRecordedWinner(): void
    {
        $history = self::fixtureHistory('selection-loser-settled-at-close');
        self::assertSame(['ActivityCancelled', 'WorkflowCompleted'], array_column(array_slice($history, -2), 'event_type'));
        self::assertSame('run_closed', $history[6]['payload']['cancellation_reason']);

        $closed = $this->replay('complete', $history);

        self::assertSame([['type' => 'complete_workflow', 'result' => self::fixtureExpected('selection-loser-settled-at-close')]], $closed);
        self::assertSame($this->replay('complete', array_slice($history, 0, 6)), $closed);
    }

    public function testCancellationAtCloseLeavesTheActivityOpenForReplay(): void
    {
        $history = self::fixtureHistory('selection-loser-settled-at-close');
        // A loser awaited after the selection could not have been cancelled at
        // close; if a history says it was, the activity is still open.
        $workflow = static function (WorkflowContext $context): string {
            $selected = $context->select([
                'work' => static fn () => $context->activity(self::GREETING, ['complete']),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);
            try {
                return 'loser: '.$selected->handles['work']->await();
            } catch (ActivityFailed $failure) {
                return 'loser failed: '.$failure->getMessage();
            }
        };

        $result = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers', self::TASK);

        self::assertSame([], $result->commands);
    }

    public function testHandledAllFailureIsNotReplacedByACancellationAtClose(): void
    {
        $history = self::fixtureHistory('parallel-handled-failure-settled-at-close');
        $failure = $history[4];
        $cancelled = $history[5];
        self::assertSame(['ActivityFailed', 'ActivityCancelled'], [$failure['event_type'], $cancelled['event_type']]);
        self::assertSame($failure['recorded_at'], $cancelled['recorded_at']);
        self::assertLessThan($failure['payload']['parallel_group_index'], $cancelled['payload']['activity']['parallel_group_index']);

        self::assertSame(
            [['type' => 'complete_workflow', 'result' => self::fixtureExpected('parallel-handled-failure-settled-at-close')]],
            $this->replay('all-failure', $history),
        );
    }

    public function testCaughtAllFailureKeepsItsReplayTimeWhenTheOpenMemberIsCancelledAtClose(): void
    {
        $history = self::fixtureHistory('parallel-handled-failure-settled-at-close');
        $workflow = static function (WorkflowContext $context): array {
            try {
                $context->all([
                    static fn () => $context->activity(self::GREETING, ['still-running']),
                    static fn () => $context->activity(self::FAILING),
                ]);
            } catch (ActivityFailed $failure) {
                try {
                    $now = $context->replayNow()->format('Y-m-d\TH:i:s.uP');
                } catch (ReplayClockUnavailable) {
                    $now = null;
                }

                return [$failure->getMessage(), $now];
            }
            self::fail('The handled failure must still be thrown.');
        };
        $codec = new AvroPayloadCodec();
        $replay = static fn (array $events): mixed => $codec->decodeEnvelope(
            (new Replayer($codec))->replay($workflow, $events, [], 'php-workers', self::TASK)->commands[0]['result'],
        );

        // Before the close, the failure is the only terminal member; the
        // close's same-instant, lower-index cancellation must not change that.
        $beforeClose = $replay(array_slice($history, 0, 5));
        self::assertSame(['boom', '2026-11-13T00:39:42.000000+00:00'], $beforeClose);
        self::assertSame($beforeClose, $replay($history));
    }

    public function testFailingAndContinuingAsNewWithAbandonedActivitiesReplayToTheSameClose(): void
    {
        $prefix = array_slice(self::fixtureHistory('selection-loser-settled-at-close'), 0, 6);
        $cancelled = self::fixtureHistory('selection-loser-settled-at-close')[6];
        $abandoned = [['activity_execution_id' => '01m795h49zm0xftwdqam8nqj81', 'sequence' => 1]];

        $failed = $this->replay('fail', [...$prefix, $cancelled, [
            'id' => '01m795h4a7fa11edw1thab4nd0',
            'sequence' => 8,
            'event_type' => 'WorkflowFailed',
            'payload' => [
                'failure_id' => '01m795h4a7fa11ure000000001',
                'source_kind' => 'workflow_run',
                'source_id' => 'regression-inline',
                'failure_category' => 'application',
                'non_retryable' => false,
                'exception_type' => null,
                'exception_class' => 'RuntimeException',
                'message' => 'Closing with an open select() loser.',
                'exception' => ['class' => 'RuntimeException', 'message' => 'Closing with an open select() loser.'],
                'failed_step_kind' => 'workflow_code',
                'abandoned_activities' => $abandoned,
            ],
            'recorded_at' => '2026-11-13T00:39:42.151204Z',
        ]]);
        self::assertSame(['fail_workflow'], array_column($failed, 'type'));
        self::assertSame('Closing with an open select() loser.', $failed[0]['message']);

        $continued = $this->replay('continue', [...$prefix, $cancelled, [
            'id' => '01m795h4a7c0nt1nuedas0new0',
            'sequence' => 8,
            'event_type' => 'WorkflowContinuedAsNew',
            'payload' => [
                'sequence' => 3,
                'continued_to_run_id' => '01m795h4a8next0run00000001',
                'continued_to_run_number' => 2,
                'workflow_link_id' => '01m795h4a8l1nk000000000001',
                'closed_reason' => 'continued',
                'abandoned_activities' => $abandoned,
            ],
            'recorded_at' => '2026-11-13T00:39:42.151204Z',
        ]]);
        self::assertSame(['continue_as_new'], array_column($continued, 'type'));
        self::assertSame(['continue', 1], $continued[0]['arguments']);
    }

    public function testAbandonedLoserListedOnWorkflowCompletedReplays(): void
    {
        $history = self::fixtureHistory('selection-loser-abandoned-at-close');
        $completed = $history[array_key_last($history)];
        self::assertSame('WorkflowCompleted', $completed['event_type']);
        self::assertSame(
            [['activity_execution_id' => '01m795h49zm0xftwdqam8nqj81', 'sequence' => 1]],
            $completed['payload']['abandoned_activities'],
        );

        self::assertSame(
            [['type' => 'complete_workflow', 'result' => self::fixtureExpected('selection-loser-abandoned-at-close')]],
            $this->replay('abandoned', $history),
        );
    }

    public function testFailedNestedWinnerKeepsItsFailureWhenItsOpenLeafAndTheTimerLoserAreCancelledAtClose(): void
    {
        $history = self::scheduledHistory('group-failure');
        $leaf = $history[1]['payload'];
        $history[] = self::event('ActivityFailed', 'nested-failed', [
            'activity_execution_id' => $leaf['activity_execution_id'],
            'activity_attempt_id' => 'nested-failed-attempt',
            'activity_type' => self::FAILING,
            'sequence' => 2,
            'attempt_number' => 1,
            'exception_class' => 'RuntimeException',
            'message' => 'boom',
            'non_retryable' => false,
            ...array_intersect_key($leaf, array_flip([
                'parallel_group_id', 'parallel_group_kind', 'parallel_group_mode', 'parallel_group_base_sequence',
                'parallel_group_size', 'parallel_group_index', 'parallel_group_path',
            ])),
        ]);
        $history[] = self::event('SelectionResolved', 'selection-resolved', [
            'selection_group_id' => 'select-calls:1:3',
            'selection_group_base_sequence' => 1,
            'selection_group_size' => 3,
            'member_key' => 'work',
            'member_index' => 0,
            'member_base_sequence' => 1,
            'member_size' => 2,
            'operation_kind' => 'group',
            'operation_identity' => 'group:1:2',
            'outcome' => 'failed',
            'resolution_event_id' => 'nested-failed',
            'resolution_event_type' => 'ActivityFailed',
        ]);
        $beforeClose = $history;
        $history[] = self::cancelledAtClose($history[0]['payload'], 'open-leaf-cancelled');
        $timer = $history[2]['payload'];
        $history[] = self::event('TimerCancelled', 'timer-loser-cancelled', [
            ...$timer,
            'cancelled_at' => '2026-11-13T00:39:42.151204Z',
        ]);
        $history[] = self::event('WorkflowCompleted', 'completed', ['output' => 'opaque', 'payload_codec' => 'avro']);

        $closed = $this->replay('group-failure', $history);

        self::assertSame([['type' => 'complete_workflow', 'result' => 'winner work caught: boom']], $closed);
        self::assertSame($this->replay('group-failure', $beforeClose), $closed);
    }

    public function testTimerLoserCancelledAtCloseReplaysToTheActivityWinner(): void
    {
        $codec = new AvroPayloadCodec();
        $history = self::scheduledHistory('timer-loser');
        $activity = $history[0]['payload'];
        $timer = $history[1]['payload'];
        $history[] = self::event('ActivityCompleted', 'work-completed', [
            ...$activity,
            'result' => $codec->envelope('hello'),
        ]);
        $history[] = self::event('SelectionResolved', 'selection-resolved', [
            'selection_group_id' => 'select-calls:1:2',
            'selection_group_base_sequence' => 1,
            'selection_group_size' => 2,
            'member_key' => 'work',
            'member_index' => 0,
            'member_base_sequence' => 1,
            'member_size' => 1,
            'operation_kind' => 'activity',
            'operation_identity' => $activity['activity_execution_id'],
            'outcome' => 'completed',
            'resolution_event_id' => 'work-completed',
            'resolution_event_type' => 'ActivityCompleted',
        ]);
        $history[] = self::event('TimerCancelled', 'timer-loser-cancelled', [
            ...$timer,
            'cancelled_at' => '2026-11-13T00:39:42.151204Z',
        ]);
        $history[] = self::event('WorkflowCompleted', 'completed', ['output' => 'opaque', 'payload_codec' => 'avro']);

        self::assertSame([['type' => 'complete_workflow', 'result' => 'closed-with-work']], $this->replay('timer-loser', $history));
    }

    public function testCancellationWithoutTheRunClosedReasonStillResolvesTheActivity(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            try {
                return $context->activity(self::GREETING, ['awaited']);
            } catch (ActivityFailed $failure) {
                return 'cancelled: '.$failure->historyEventType;
            }
        };
        $scheduled = ['sequence' => 1, 'activity_execution_id' => 'awaited-activity', 'activity_type' => self::GREETING];
        $codec = new AvroPayloadCodec();
        foreach ([null, 'scope_cancelled'] as $reason) {
            $cancelled = self::cancelledAtClose($scheduled, 'awaited-cancelled');
            unset($cancelled['payload']['cancellation_reason']);
            if ($reason !== null) {
                $cancelled['payload']['cancellation_reason'] = $reason;
            }
            $result = (new Replayer($codec))->replay($workflow, [
                self::event('ActivityScheduled', 'awaited-scheduled', $scheduled),
                $cancelled,
            ], [], 'php-workers', self::TASK);

            self::assertSame('cancelled: ActivityCancelled', $codec->decodeEnvelope($result->commands[0]['result']));
        }
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private function replay(string $mode, array $history): array
    {
        $codec = new AvroPayloadCodec();
        try {
            $result = (new Replayer($codec))->replay(
                self::selectionLoserAtCloseWorkflow(),
                $history,
                [$mode],
                'php-workers',
                self::TASK,
            );
        } catch (\RuntimeException $failure) {
            return [['type' => 'fail_workflow', 'message' => $failure->getMessage()]];
        }

        return array_map(static function (array $command) use ($codec): array {
            if (isset($command['result'])) {
                $command['result'] = $codec->decodeEnvelope($command['result']);
            }
            if (isset($command['arguments'])) {
                $command['arguments'] = $codec->decodeEnvelope($command['arguments']);
            }

            return array_intersect_key($command, array_flip(['type', 'result', 'message', 'arguments']));
        }, $result->commands);
    }

    /**
     * Mirrors Workflow's TestSelectionLoserAtCloseWorkflow (workflow#1121): the
     * run closes while an operation it never awaited is still open.
     *
     * @return callable(WorkflowContext, string, int=): mixed
     */
    private static function selectionLoserAtCloseWorkflow(): callable
    {
        return static function (WorkflowContext $context, string $mode, int $round = 0): mixed {
            if ($round > 0) {
                return 'continued';
            }
            if ($mode === 'all-failure') {
                // The second member fails first; the first is still open when
                // the workflow handles the failure and closes.
                try {
                    $context->all([
                        static fn () => $context->activity(self::GREETING, ['still-running']),
                        static fn () => $context->activity(self::FAILING),
                    ]);
                    $caught = null;
                } catch (ActivityFailed $failure) {
                    $caught = $failure->getMessage();
                }

                return 'caught: '.$caught;
            }
            if ($mode === 'group-failure') {
                // The winner is a nested group whose second leaf failed; its
                // first leaf is still open when the run closes.
                $selected = $context->select([
                    'work' => static fn () => $context->all([
                        static fn () => $context->activity(self::GREETING, ['still-running']),
                        static fn () => $context->activity(self::FAILING),
                    ]),
                    'deadline' => static function () use ($context): void {
                        $context->sleep(3600);
                    },
                ]);
                try {
                    $selected->result();
                    $caught = null;
                } catch (ActivityFailed $failure) {
                    $caught = $failure->getMessage();
                }

                return 'winner '.$selected->key.' caught: '.$caught;
            }
            $selected = $mode === 'timer-loser'
                ? $context->select([
                    'work' => static fn () => $context->activity(self::GREETING, ['timer-loser']),
                    'deadline' => static function () use ($context): void {
                        $context->sleep(3600);
                    },
                ])
                : $context->select([
                    'work' => static fn () => $context->activity(self::GREETING, [$mode],
                        $mode === 'abandoned' ? ['cancellation_policy' => 'abandon', 'schedule_to_close_timeout' => 3600] : []),
                    'deadline' => static function () use ($context): void {
                        $context->sleep(0);
                    },
                ]);

            return match ($mode) {
                'fail' => throw new \RuntimeException('Closing with an open select() loser.'),
                'continue' => $context->continueAsNew([$mode, 1]),
                default => 'closed-with-'.$selected->key,
            };
        };
    }

    /** @return list<array<string, mixed>> */
    private static function fixtureHistory(string $name): array
    {
        return self::fixture($name)['history'];
    }

    private static function fixtureExpected(string $name): string
    {
        return self::fixture($name)['expected_result'];
    }

    /** @return array<string, mixed> */
    private static function fixture(string $name): array
    {
        return json_decode(
            (string) file_get_contents(__DIR__."/fixtures/run-closed/{$name}.json"),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
    }

    /**
     * The scheduling rows the official Replayer itself asks for, so nested
     * selection paths are exactly what this SDK authors.
     *
     * @return list<array<string, mixed>>
     */
    private static function scheduledHistory(string $mode): array
    {
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay(
            self::selectionLoserAtCloseWorkflow(),
            [],
            [$mode],
            'php-workers',
            self::TASK,
        )->commands;
        $history = [];
        foreach ($commands as $offset => $command) {
            $sequence = $offset + 1;
            $path = $command['parallel_group_path'];
            $payload = ['sequence' => $sequence, ...$path[array_key_last($path)], 'parallel_group_path' => $path];
            if ($command['type'] === 'schedule_activity') {
                $history[] = self::event('ActivityScheduled', "scheduled-{$sequence}", [
                    'activity_execution_id' => "activity-{$sequence}",
                    'activity_class' => $command['activity_type'],
                    'activity_type' => $command['activity_type'],
                    ...$payload,
                ]);
            } else {
                $history[] = self::event('TimerScheduled', "scheduled-{$sequence}", [
                    'timer_id' => "timer-{$sequence}",
                    'delay_seconds' => $command['delay_seconds'],
                    ...$payload,
                ]);
            }
        }

        return $history;
    }

    /**
     * The row ActivityCancellation::record writes for an activity its run's
     * close cancelled (workflow#1121): parallel metadata only in the snapshot.
     *
     * @param array<string, mixed> $scheduled
     * @return array<string, mixed>
     */
    private static function cancelledAtClose(array $scheduled, string $id): array
    {
        $at = '2026-11-13T00:39:42.151204Z';
        $snapshot = array_filter(
            $scheduled,
            static fn (string $key): bool => str_starts_with($key, 'parallel_group_') || str_starts_with($key, 'selection_member_'),
            ARRAY_FILTER_USE_KEY,
        );

        return self::event('ActivityCancelled', $id, [
            'workflow_command_id' => null,
            'activity_execution_id' => $scheduled['activity_execution_id'],
            'activity_attempt_id' => null,
            'activity_class' => $scheduled['activity_type'],
            'activity_type' => $scheduled['activity_type'],
            'sequence' => $scheduled['sequence'],
            'attempt_number' => 0,
            'cancelled_at' => $at,
            'activity' => [
                'id' => $scheduled['activity_execution_id'],
                'sequence' => $scheduled['sequence'],
                'type' => $scheduled['activity_type'],
                'status' => 'cancelled',
                'closed_at' => $at,
                ...$snapshot,
            ],
            'activity_attempt' => null,
            'cancellation_reason' => 'run_closed',
        ]);
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function event(string $type, string $id, array $payload): array
    {
        return ['id' => $id, 'event_type' => $type, 'payload' => $payload, 'recorded_at' => '2026-11-13T00:39:42.151204Z'];
    }
}
