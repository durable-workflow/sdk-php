<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityGroupReplayTest extends TestCase
{
    public function test_immutable_group_fixture_skips_the_completed_member_and_recovers_the_started_sibling(): void
    {
        $commands = ReplayRegressionFixture::executeFile(__DIR__.'/fixtures/prepared-local/group-cold-replay.json');
        self::assertSame('prepare_local_activity_group', $commands[0]['type']);
        self::assertSame([2], array_column($commands[0]['local_activities'], 'sequence'));
        self::assertTrue($commands[0]['local_activities'][0]['recover']);
    }

    public function test_mixed_group_is_captured_completely_before_any_callback_runs(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->parallel([
            static fn () => $context->childWorkflow('python.child', ['child input'], ['queue' => 'python']),
            static fn () => $context->localActivity('php.local', ['local input'], ['schedule_to_close_timeout' => 30]),
        ]);
        $result = self::replay($workflow);
        self::assertSame([], $result->commands);
        self::assertNull($result->preparedLocalActivity);
        $group = $result->preparedLocalActivityGroup;
        self::assertFalse($group->committed);
        self::assertSame(1, $group->baseSequence);
        self::assertSame(2, $group->size);
        self::assertSame(['start_child_workflow', 'prepare_local_activity'], array_column($group->commands, 'type'));
        self::assertSame('python', $group->commands[0]['queue']);
        self::assertArrayNotHasKey('queue', $group->commands[1]);
        self::assertArrayNotHasKey('outcome', $group->commands[1]);
        self::assertArrayNotHasKey('result', $group->commands[1]);
        self::assertSame(['local input'], $codec->decodeEnvelope($group->commands[1]['arguments']));
        self::assertSame(30, $group->commands[1]['schedule_to_close_timeout']);
        self::assertSame(2, $group->calls[0]->sequence);
        self::assertFalse($group->calls[0]->recover);
        self::assertSame(self::path('mixed', 1, 2, 1), $group->commands[1]['parallel_group_path']);
    }

    public function test_committed_group_returns_only_unresolved_local_members_and_recovers_started_attempts(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = self::twoLocals();
        $history = [
            self::localEvent('ActivityScheduled', 1, 'first', 2, 0),
            self::localEvent('ActivityScheduled', 2, 'second', 2, 1),
            self::localEvent('ActivityCompleted', 1, 'first', 2, 0, ['result' => $codec->envelope('first durable result')]),
            self::localEvent('ActivityStarted', 2, 'second', 2, 1),
        ];
        $pending = self::replay($workflow, $history)->preparedLocalActivityGroup;
        self::assertTrue($pending->committed);
        self::assertSame([], $pending->commands);
        self::assertCount(1, $pending->calls);
        self::assertSame(2, $pending->calls[0]->sequence);
        self::assertTrue($pending->calls[0]->recover);
        $history[] = self::localEvent('ActivityRetryScheduled', 2, 'second', 2, 1);
        self::assertFalse(self::replay($workflow, $history)->preparedLocalActivityGroup->calls[0]->recover);
        $history[] = self::localEvent('ActivityCompleted', 2, 'second', 2, 1,
            ['result' => $codec->envelope('second durable result')]);
        $finished = self::replay($workflow, $history);
        self::assertNull($finished->preparedLocalActivityGroup);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame(['first durable result', 'second durable result'], $codec->decodeEnvelope($finished->commands[0]['result']));
    }

    public function test_scheduled_members_without_started_history_need_first_preparation(): void
    {
        $pending = self::replay(self::twoLocals(), [
            self::localEvent('ActivityScheduled', 1, 'first', 2, 0),
            self::localEvent('ActivityScheduled', 2, 'second', 2, 1),
        ])->preparedLocalActivityGroup;
        self::assertTrue($pending->committed);
        self::assertSame([1, 2], array_map(static fn ($call) => $call->sequence, $pending->calls));
        self::assertSame([false, false], array_map(static fn ($call) => $call->recover, $pending->calls));
    }

    public function test_a_partial_group_is_refused_before_callback_preparation(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('missing a declared member');
        self::replay(self::twoLocals(), [self::localEvent('ActivityScheduled', 1, 'first', 2, 0)]);
    }

    public function test_changed_group_history_cannot_be_prepared(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        self::replay(self::twoLocals(), [
            self::localEvent('ActivityScheduled', 1, 'first', 3, 0),
            self::localEvent('ActivityScheduled', 2, 'second', 3, 1),
        ]);
    }

    public function test_nested_all_preserves_every_authored_path_and_result_position(): void
    {
        $result = self::replay(static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->localActivity('first'),
            static fn () => $context->parallel([
                static fn () => $context->activity('remote'),
                static fn () => $context->localActivity('second'),
            ]),
        ]));
        $group = $result->preparedLocalActivityGroup;
        self::assertSame(3, $group->size);
        self::assertSame(['prepare_local_activity', 'schedule_activity', 'prepare_local_activity'], array_column($group->commands, 'type'));
        self::assertSame([1, 3], array_map(static fn ($call) => $call->sequence, $group->calls));
        self::assertCount(1, $group->commands[0]['parallel_group_path']);
        self::assertCount(2, $group->commands[2]['parallel_group_path']);
        self::assertSame(2, $group->commands[2]['parallel_group_path'][0]['parallel_group_index']);
        self::assertSame(1, $group->commands[2]['parallel_group_path'][1]['parallel_group_index']);
    }

    public function test_prefix_remains_separate_from_the_atomic_group(): void
    {
        $result = self::replay(static function (WorkflowContext $context): array {
            $value = $context->sideEffect(static fn (): string => 'prefix');
            return $context->all([
                static fn () => $context->localActivity('first', [$value]),
                static fn () => $context->localActivity('second'),
            ]);
        });
        self::assertSame(['record_side_effect'], array_column($result->commands, 'type'));
        self::assertSame(2, $result->preparedLocalActivityGroup->baseSequence);
        self::assertSame([2, 3], array_map(static fn ($call) => $call->sequence, $result->preparedLocalActivityGroup->calls));
    }

    public function test_selection_and_turn_closing_wait_groups_are_refused_before_application_code(): void
    {
        foreach ([
            static fn (WorkflowContext $context) => $context->select([static fn () => $context->localActivity('first')]),
            static fn (WorkflowContext $context) => $context->all([
                static fn () => $context->localActivity('first'),
                static fn () => $context->waitCondition(static fn (): bool => false, 'wait'),
            ]),
        ] as $workflow) {
            try {
                self::replay($workflow);
                self::fail('Unsupported prepared group was admitted.');
            } catch (WorkflowClaimAborted $error) {
                self::assertStringContainsString('admission_unavailable', $error->getMessage());
            }
        }
    }

    public function test_shielded_cleanup_group_keeps_one_delivery_and_original_root_budget_during_cold_replay(): void
    {
        $codec = new AvroPayloadCodec();
        $history = array_slice(json_decode((string) file_get_contents(__DIR__.'/fixtures/prepared-local/cleanup-cold-replay.json'),
            true, flags: JSON_THROW_ON_ERROR)['history'], 0, 2);
        $workflow = static function (WorkflowContext $context): void {
            try { $context->sleep(10); } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->all([
                    static fn () => $context->localActivity('first'),
                    static fn () => $context->localActivity('second'),
                ]));
                throw $cancelled;
            }
        };
        $fresh = self::replay($workflow, $history)->preparedLocalActivityGroup;
        self::assertSame(2, $fresh->baseSequence);
        foreach ($fresh->calls as $call) {
            self::assertSame(['request_id' => 'child-request', 'delivery_history_event_id' => 'canonical-delivery'],
                $call->descriptor($codec)['cancellation_cleanup']);
            self::assertSame('root-request', $call->cleanupSnapshot()['root_request_id']);
            self::assertSame('2026-10-02T00:00:30.000000Z', $call->cleanupSnapshot()['cleanup_deadline_at']);
        }
        $history[] = self::localEvent('ActivityScheduled', 2, 'first', 2, 0, base: 2);
        $history[] = self::localEvent('ActivityScheduled', 3, 'second', 2, 1, base: 2);
        $history[] = self::localEvent('ActivityCompleted', 2, 'first', 2, 0, ['result' => $codec->envelope('cleaned')], base: 2);
        $history[] = self::localEvent('ActivityStarted', 3, 'second', 2, 1, base: 2);
        $cold = self::replay($workflow, $history)->preparedLocalActivityGroup;
        self::assertCount(1, $cold->calls);
        self::assertSame(3, $cold->calls[0]->sequence);
        self::assertTrue($cold->calls[0]->recover);
        self::assertSame($fresh->calls[1]->cleanupSnapshot(), $cold->calls[0]->cleanupSnapshot());
        $history[] = self::localEvent('ActivityCompleted', 3, 'second', 2, 1, ['result' => $codec->envelope('cleaned')], base: 2);
        try {
            self::replay($workflow, $history);
            self::fail('Finished cleanup lost its original cancellation.');
        } catch (WorkflowCancelled $cancelled) {
            self::assertSame('child-request', $cancelled->requestId);
            self::assertSame('root-request', $cancelled->context->rootRequestId);
        }
    }

    private static function replay(callable $workflow, array $history = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'prepared',
            ['run_id' => 'regression-inline', 'workflow_id' => 'regression-workflow'],
            static function (): never { self::fail('Replay invoked application code without durable admission.'); },
            prepareLocalActivities: true, prepareLocalActivityGroups: true);
    }

    private static function twoLocals(): \Closure
    {
        return static fn (WorkflowContext $context): array => $context->all([
            static fn () => $context->localActivity('first'),
            static fn () => $context->localActivity('second'),
        ]);
    }

    private static function path(string $kind, int $base, int $size, int $index): array
    {
        return [[
            'parallel_group_id' => ($kind === 'activity' ? 'parallel-activities' : 'parallel-calls').":{$base}:{$size}",
            'parallel_group_kind' => $kind, 'parallel_group_base_sequence' => $base,
            'parallel_group_size' => $size, 'parallel_group_index' => $index,
        ]];
    }

    private static function localEvent(string $event, int $sequence, string $type, int $size, int $index, array $extra = [], int $base = 1): array
    {
        $path = self::path('activity', $base, $size, $index);
        return ['event_type' => $event, 'payload' => [
            'sequence' => $sequence, 'activity_type' => $type, 'execution_mode' => 'local',
            ...$path[0], 'parallel_group_path' => $path, ...$extra,
        ]];
    }
}
