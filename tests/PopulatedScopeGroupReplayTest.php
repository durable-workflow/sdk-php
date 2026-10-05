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
