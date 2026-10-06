<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\ParallelWorkflowCommand;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PopulatedScopeGroupReplayTest extends TestCase
{
    public static bool $satisfied = false;

    public static function condition(): bool { return self::$satisfied; }

    #[DataProvider('pendingGroups')]
    public function test_pending_group_waits_for_its_original_delivery_without_cleanup(string $layout, bool $prepared): void
    {
        $fixture = self::fixture($layout);
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] !== 'CancellationScopeDelivered'
            && ($prepared || $event['event_type'] !== 'CancellationScopeDeliveryPrepared')));
        $context = null; $prior = null;
        $workflow = self::workflow($fixture, $context, $prior);
        $replay = static fn () => (new Replayer(new AvroPayloadCodec()))->replay($workflow,
            $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        $result = $replay();
        self::assertSame('prior-value', $prior);
        self::assertNull($context);
        self::assertSame([], $result->commands);
        self::assertNotNull($result->cancellationScopeDelivery);
        $intent = $result->cancellationScopeDelivery;
        self::assertSame($fixture['scope_id'], $intent->context->scopeId);
        self::assertSame('parallel', $intent->boundary->callKind);
        self::assertSame(4, $intent->boundary->sequence);
        self::assertSame(4, $intent->boundary->sequenceSpan);
        self::assertSame($prepared ? $fixture['history'][array_key_last($fixture['history'])]['id'] : null, $intent->preparationHistoryEventId);
        $fixture['task'] += ['workflow_task_attempt' => 17, 'lease_owner' => 'replacement'];
        $again = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        self::assertEquals($intent, $again->cancellationScopeDelivery);
        self::assertNull($context);
    }

    public static function pendingGroups(): iterable
    {
        foreach (['flat', 'nested'] as $layout) {
            foreach ([false, true] as $prepared) { yield $layout.'-'.($prepared ? 'prepared' : 'requested') => [$layout, $prepared]; }
        }
    }

    #[DataProvider('pendingChanges')]
    public function test_changed_prepared_group_cannot_select_another_boundary_or_enter_cleanup(string $change): void
    {
        $fixture = self::fixture('nested');
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] !== 'CancellationScopeDelivered'));
        $context = null; $prior = null;
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(self::workflow($fixture, $context, $prior, $change),
                $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true,
                replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        } finally { self::assertNull($context); }
    }

    public static function pendingChanges(): iterable
    {
        foreach (self::changes() as $name => $case) { if ($name !== 'shield') { yield $name => $case; } }
    }

    public function test_pending_group_with_a_missing_original_admission_refuses_before_cleanup_or_effects(): void
    {
        $fixture = self::fixture('flat');
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            !in_array($event['event_type'], ['CancellationScopeDelivered', 'CancellationScopeDeliveryPrepared', 'ConditionWaitOpened'], true)));
        $context = null; $prior = null;
        $this->expectException(WorkflowClaimAborted::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(self::workflow($fixture, $context, $prior),
                $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true,
                replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        } finally { self::assertNull($context); }
    }

    public function test_completed_first_member_preserves_the_pending_original_group(): void
    {
        $fixture = self::fixture('flat');
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            !in_array($event['event_type'], ['CancellationScopeDelivered', 'CancellationScopeDeliveryPrepared'], true)));
        $requestIndex = array_key_last($fixture['history']);
        $event = $fixture['history'][6];
        $event['id'] = 'first-group-member-completed'; $event['event_type'] = 'ActivityCompleted';
        $event['payload']['result'] = (new AvroPayloadCodec())->envelope('first-result');
        array_splice($fixture['history'], $requestIndex, 0, [$event]);
        foreach ($fixture['history'] as $index => &$row) { $row['sequence'] = $index + 1; }
        unset($row);
        $context = null; $prior = null;
        $result = (new Replayer(new AvroPayloadCodec()))->replay(self::workflow($fixture, $context, $prior),
            $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        self::assertSame([], $result->commands);
        self::assertSame(4, $result->cancellationScopeDelivery->boundary->sequence);
        self::assertSame(4, $result->cancellationScopeDelivery->boundary->sequenceSpan);
        self::assertNull($context);
    }

    public function test_a_group_completed_before_request_preserves_results_and_selects_the_next_call(): void
    {
        $fixture = self::fixture('flat');
        $request = $fixture['history'][11];
        $fixture['history'] = array_slice($fixture['history'], 0, 6);
        foreach ([0, 1] as $index) {
            $path = ['parallel_group_id' => 'parallel-timers:4:2', 'parallel_group_kind' => 'timer',
                'parallel_group_base_sequence' => 4, 'parallel_group_size' => 2, 'parallel_group_index' => $index];
            $payload = ['sequence' => 4 + $index, 'timer_id' => 'completed-group-'.$index, 'delay_seconds' => 1,
                'fire_at' => '2026-10-04T00:00:03.123456Z', 'cancellation_scope_id' => $fixture['scope_id'],
                ...$path, 'parallel_group_path' => [$path]];
            foreach (['TimerScheduled' => '02', 'TimerFired' => '03'] as $type => $second) {
                $fixture['history'][] = ['id' => $type.'-'.$index, 'namespace' => 'sdk-scope-fixture',
                    'sequence' => count($fixture['history']) + 1, 'event_type' => $type, 'payload' => $payload,
                    'timestamp' => '2026-10-04T00:00:'.$second.'.123456Z'];
            }
        }
        $request['sequence'] = count($fixture['history']) + 1;
        $fixture['history'][] = $request;
        $values = null;
        $handler = static function (WorkflowContext $workflow) use (&$values): void {
            $workflow->cancellationScope(static function () use ($workflow, &$values): void {
                $workflow->cancellationScope(static function () use ($workflow, &$values): void {
                    self::assertSame('prior-value', $workflow->activity('prior-step'));
                    $values = $workflow->all([$workflow->deferTimer(1), $workflow->deferTimer(1)]);
                    $workflow->sleep(10);
                    self::fail('The next call must retain its pending cancellation boundary.');
                });
            });
        };
        $result = (new Replayer(new AvroPayloadCodec()))->replay($handler, $fixture['history'], [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        self::assertSame([null, null], $values);
        self::assertSame([], $result->commands);
        self::assertSame(6, $result->cancellationScopeDelivery->boundary->sequence);
        self::assertSame('timer', $result->cancellationScopeDelivery->boundary->callKind);
    }

    #[DataProvider('layouts')]
    public function test_complete_original_group_delivers_once_and_preserves_parent(string $layout): void
    {
        self::$satisfied = false;
        $fixture = self::fixture($layout);
        $context = null;
        $prior = null;
        $workflow = self::workflow($fixture, $context, $prior);
        $first = $this->replay($workflow, $fixture);
        self::assertSame('prior-value', $prior);
        self::assertInstanceOf(ScopedCancellationContext::class, $context);
        $request = array_values(array_filter($fixture['history'], static fn (array $event): bool => $event['event_type'] === 'CancellationScopeRequested'))[0];
        self::assertEquals($request['payload']['cancellation'], $context->toArray());
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));
        self::assertSame('unaffected-root', $first->commands[0]['activity_type']);
        self::assertArrayNotHasKey('cancellation_scope_id', $first->commands[0]);
        $last = $fixture['history'][array_key_last($fixture['history'])];
        $fixture['history'][] = ['id' => 'survivor-completed', 'namespace' => $last['namespace'],
            'sequence' => $last['sequence'] + 1, 'event_type' => 'ActivityCompleted',
            'timestamp' => '2026-10-04T00:00:20.123456Z', 'payload' => ['sequence' => 8,
                'activity_type' => 'unaffected-root', 'result' => (new AvroPayloadCodec())->envelope('survivor')]];
        $finished = $this->replay($workflow, $fixture);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame(['survivor', $context->requestId, 21.0], (new AvroPayloadCodec())->decodeEnvelope($finished->commands[0]['result']));
        $fixture['task'] += ['workflow_task_attempt' => 19, 'lease_owner' => 'replacement'];
        self::assertSame($finished->commands, $this->replay($workflow, $fixture)->commands);
    }

    public static function layouts(): iterable
    {
        yield 'flat' => ['flat'];
        yield 'nested' => ['nested'];
    }

    #[DataProvider('layouts')]
    public function test_committed_group_delivery_wins_over_now_satisfied_predicate(string $layout): void
    {
        self::$satisfied = true;
        $context = null;
        $prior = null;
        $fixture = self::fixture($layout);
        $result = $this->replay(self::workflow($fixture, $context, $prior), $fixture);
        self::assertInstanceOf(ScopedCancellationContext::class, $context);
        self::assertSame(['schedule_activity'], array_column($result->commands, 'type'));
        self::$satisfied = false;
    }

    #[DataProvider('changes')]
    public function test_changed_group_is_refused_before_cleanup(string $change): void
    {
        $context = null;
        $prior = null;
        $fixture = self::fixture('nested');
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            $this->replay(self::workflow($fixture, $context, $prior, $change), $fixture);
        } finally {
            self::assertNull($context);
        }
    }

    public static function changes(): iterable
    {
        foreach (['activity-type', 'activity-policy', 'child-type', 'child-policy', 'parent-policy',
            'timer', 'condition-key', 'condition-timeout', 'condition-predicate', 'layout', 'size', 'shield', 'prefix'] as $change) {
            yield $change => [$change];
        }
    }

    #[DataProvider('unsupportedGroups')]
    public function test_unqualified_group_shapes_remain_refused_before_entry(string $layout): void
    {
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, self::fixture($layout));
        } finally {
            self::assertFalse($entered);
        }
    }

    public static function unsupportedGroups(): iterable
    {
        foreach (['flat-local', 'flat-selection', 'flat-incomplete', 'flat-signal'] as $layout) { yield $layout => [$layout]; }
    }

    public function test_group_cannot_consume_a_sibling_scope_member(): void
    {
        $entered = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, self::fixture('flat-other-scope'));
        } finally {
            self::assertFalse($entered);
        }
    }

    #[DataProvider('layouts')]
    public function test_group_cleanup_retains_original_receipt_and_narrower_authority(string $layout): void
    {
        $fixture = self::fixture($layout);
        foreach ($fixture['history'] as &$event) {
            if (in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)) {
                $event['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.123456Z';
            }
        }
        unset($event);
        $context = null;
        $prior = null;
        $workflow = self::workflow($fixture, $context, $prior, 'cleanup');
        $replayer = new Replayer(new AvroPayloadCodec());
        $run = static fn () => $replayer->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            localActivityExecutor: static function (): never { self::fail('Unadmitted cleanup callback cannot run.'); },
            prepareLocalActivities: true, allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
        $result = $run();
        self::assertSame([], $result->commands);
        $call = $result->preparedLocalActivity;
        self::assertNotNull($call);
        self::assertSame(8, $call->sequence);
        $snapshot = $call->cleanupSnapshot();
        self::assertSame($context->requestId, $snapshot['request_id']);
        self::assertSame($context->rootRequestId, $snapshot['root_request_id']);
        self::assertSame($fixture['scope_id'], $snapshot['scope_id']);
        self::assertSame('2026-10-04T00:00:30.123456Z', $snapshot['cleanup_deadline_at']);
        self::assertSame('2026-10-04T00:00:26.123456Z', $snapshot['authority_deadline_at']);
        self::assertSame($fixture['history'][array_key_last($fixture['history'])]['id'], $snapshot['delivery_history_event_id']);
        $fixture['task'] += ['workflow_task_attempt' => 23, 'lease_owner' => 'replacement'];
        $replacement = $replayer->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            localActivityExecutor: static function (): never { self::fail('Replacement cannot execute an unadmitted cleanup callback.'); },
            prepareLocalActivities: true, allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
        self::assertSame($snapshot, $replacement->preparedLocalActivity->cleanupSnapshot());
    }

    private function replay(callable $workflow, array $fixture): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    public static function workflow(array $fixture, ?ScopedCancellationContext &$context, mixed &$prior, string $change = ''): callable
    {
        return static function (WorkflowContext $workflow) use ($fixture, &$context, &$prior, $change): array {
            $request = $workflow->cancellationScope(static function () use ($workflow, $fixture, &$context, &$prior, $change): string {
                return $workflow->cancellationScope(
                static function () use ($workflow, $fixture, &$context, &$prior, $change): string {
                    if ($change !== 'prefix') { $prior = $workflow->activity('prior-step'); }
                    $activity = $workflow->deferActivity($change === 'activity-type' ? 'changed' : 'original-activity', [], [
                        'cancellation_policy' => $change === 'activity-policy' ? 'abandon' : 'try_cancel', 'schedule_to_close_timeout' => 60]);
                    $timer = $workflow->deferTimer($change === 'timer' ? 42 : 3600);
                    $child = $workflow->deferChildWorkflow($change === 'child-type' ? 'changed' : 'original-child', [], [
                        'cancellation_policy' => $change === 'child-policy' ? 'abandon' : 'wait_cancellation_completed',
                        'parent_close_policy' => $change === 'parent-policy' ? 'abandon' : 'request_cancel']);
                    $condition = $workflow->deferCondition($change === 'condition-predicate' ? static fn () => false : [self::class, 'condition'],
                        $change === 'condition-key' ? 'changed' : 'ready', $change === 'condition-timeout' ? 42 : 30);
                    $nested = ($fixture['layout'] === 'nested') !== ($change === 'layout');
                    $members = $nested ? [$activity, new ParallelWorkflowCommand([$timer, $child]), $condition] : [$activity, $timer, $child, $condition];
                    if ($change === 'size') { array_pop($members); }
                    try {
                        $call = static fn () => $workflow->all($members);
                        if ($change === 'shield') { $workflow->cancellationShield($call); } else { $call(); }
                        self::fail('Original group must deliver its committed cancellation.');
                    } catch (WorkflowCancelled $error) {
                        self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                        $context = $error->context;
                        self::assertSame(21.0, $context->remaining());
                        $workflow->cancellationShield(static fn () => $workflow->throwIfCancellationRequested());
                        if ($change === 'cleanup') { $workflow->cancellationShield(static fn () => $workflow->localActivity('group-cleanup')); }
                        return $context->requestId;
                    }
                });
            });
            self::assertFalse($workflow->isCancellationRequested());
            self::assertNull($workflow->cancellationContext());
            $remaining = $context->remaining();
            return [$workflow->activity('unaffected-root'), $request, $remaining];
        };
    }

    private static function fixture(string $layout): array
    {
        return json_decode(file_get_contents(__DIR__.'/fixtures/populated-scope-groups.json'), true, flags: JSON_THROW_ON_ERROR)[$layout];
    }
}
