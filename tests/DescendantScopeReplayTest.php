<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\ParallelWorkflowCommand;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DescendantScopeReplayTest extends TestCase
{
    #[DataProvider('layouts')]
    public function test_ancestor_marker_delivers_active_descendant_and_preserves_each_scope_context(string $layout): void
    {
        $fixture = self::fixture($layout);
        $observed = [];
        $workflow = self::workflow($fixture, $observed);
        $first = $this->replay($workflow, $fixture);
        self::assertSame(['grandchild', 'child', 'parent'], array_keys($observed));
        foreach ($observed as $name => $context) {
            self::assertSame(ScopedCancellationContext::fromArray($fixture['contexts'][$name])->toArray(), $context->toArray());
        }
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));
        self::assertSame('unaffected-root', $first->commands[0]['activity_type']);
        self::assertArrayNotHasKey('cancellation_scope_id', $first->commands[0]);
        $last = $fixture['history'][array_key_last($fixture['history'])];
        $fixture['history'][] = ['id' => 'survivor-completed', 'namespace' => $last['namespace'],
            'sequence' => $last['sequence'] + 1, 'event_type' => 'ActivityCompleted',
            'timestamp' => '2026-10-04T00:00:20.123456Z', 'payload' => [
                'sequence' => $layout === 'timer' ? 9 : 12, 'activity_type' => 'unaffected-root',
                'result' => (new AvroPayloadCodec())->envelope('survivor')]];
        $finished = $this->replay($workflow, $fixture);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame('survivor', (new AvroPayloadCodec())->decodeEnvelope($finished->commands[0]['result']));
        $fixture['task'] += ['workflow_task_attempt' => 19, 'lease_owner' => 'replacement'];
        self::assertSame($finished->commands, $this->replay($workflow, $fixture)->commands);
    }

    public static function layouts(): iterable
    {
        yield 'timer' => ['timer'];
        yield 'group' => ['group'];
    }

    #[DataProvider('cleanupScopes')]
    public function test_cleanup_and_replacement_use_ancestor_receipt_and_original_scope_authority(string $layout, string $target): void
    {
        $fixture = self::fixture($layout);
        $observed = [];
        $workflow = self::workflow($fixture, $observed, cleanup: $target);
        $first = $this->replay($workflow, $fixture, prepare: true);
        self::assertSame([], $first->commands);
        self::assertNotNull($first->preparedLocalActivity);
        $snapshot = $first->preparedLocalActivity->cleanupSnapshot();
        self::assertSame($fixture['scopes']['parent'], $snapshot['scope_id']);
        self::assertSame($fixture['scopes'][$target], $snapshot['operation_scope_id']);
        self::assertSame($fixture['contexts']['parent']['lineage'][array_key_last($fixture['contexts']['parent']['lineage'])]['request_id'], $snapshot['request_id']);
        self::assertSame($fixture['contexts']['parent']['root_context']['root_request_id'], $snapshot['root_request_id']);
        self::assertSame('ancestor-prepared', $snapshot['preparation_history_event_id']);
        self::assertSame('ancestor-delivered', $snapshot['delivery_history_event_id']);
        self::assertSame('2026-10-04T00:00:30.123456Z', $snapshot['cleanup_deadline_at']);
        self::assertSame('2026-10-04T00:00:26.123456Z', $snapshot['authority_deadline_at']);
        $fixture['task'] += ['workflow_task_attempt' => 23, 'lease_owner' => 'replacement'];
        self::assertSame($snapshot, $this->replay($workflow, $fixture, prepare: true)->preparedLocalActivity->cleanupSnapshot());
    }

    public static function cleanupScopes(): iterable
    {
        foreach (['timer', 'group'] as $layout) {
            foreach (['grandchild', 'child', 'parent'] as $scope) { yield $layout.'-'.$scope => [$layout, $scope]; }
        }
    }

    #[DataProvider('changes')]
    public function test_changed_authored_subtree_is_refused_before_cleanup(string $change): void
    {
        $fixture = self::fixture('group');
        $observed = [];
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(self::workflow($fixture, $observed, $change), $fixture); }
        finally { self::assertSame([], $observed); }
    }

    public static function changes(): iterable
    {
        foreach (['shield', 'prefix', 'activity', 'activity-policy', 'timer', 'child', 'child-policy', 'layout', 'size'] as $change) {
            yield $change => [$change];
        }
    }

    #[DataProvider('historyChanges')]
    public function test_forged_frozen_subtree_is_refused_before_workflow_entry(string $change): void
    {
        $fixture = self::fixture('group');
        foreach ($fixture['history'] as &$event) {
            if ($event['event_type'] !== 'CancellationScopeDeliveryPrepared') { continue; }
            $members = &$event['payload']['descendant_members'];
            match ($change) {
                'omitted' => array_pop($members),
                'reordered' => $members = array_reverse($members),
                'parent' => $members[1]['parent_scope_id'] = $fixture['scopes']['parent'],
                'request' => $members[1]['request_id'] = 'substituted',
                'propagation' => $members[1]['propagation_history_event_id'] = 'substituted',
                'opening' => $members[1]['scope_history_event_id'] = 'substituted',
                'deadline' => $members[1]['authority_deadline_at'] = '2026-10-04T00:00:29.123456Z',
                'operation' => $members[1]['timer_members'] = [],
                'root' => $members[1]['cancellation']['root_context']['reason'] = 'changed',
            };
            unset($members);
        }
        unset($event);
        $entered = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(static function () use (&$entered): void { $entered = true; }, $fixture); }
        finally { self::assertFalse($entered); }
    }

    public static function historyChanges(): iterable
    {
        foreach (['omitted', 'reordered', 'parent', 'request', 'propagation', 'opening', 'deadline', 'operation', 'root'] as $change) {
            yield $change => [$change];
        }
    }

    public function test_pending_ancestor_delivery_cannot_run_cleanup(): void
    {
        $fixture = self::fixture('timer');
        array_pop($fixture['history']);
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try { $this->replay(static function () use (&$entered): void { $entered = true; }, $fixture); }
        finally { self::assertFalse($entered); }
    }

    #[DataProvider('pendingRequests')]
    public function test_pending_ancestor_prepares_before_inherited_requests_exist(string $layout, bool $inherited): void
    {
        $fixture = self::fixture($layout);
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            !in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)
            && ($inherited || $event['event_type'] !== 'CancellationScopeRequested'
                || $event['payload']['scope_id'] === $fixture['scopes']['parent'])));
        $observed = [];
        $workflow = self::workflow($fixture, $observed);
        $replay = static fn () => (new Replayer(new AvroPayloadCodec()))->replay($workflow,
            $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        $result = $replay();
        self::assertSame([], $observed);
        self::assertSame([], $result->commands);
        $intent = $result->cancellationScopeDelivery;
        self::assertNotNull($intent);
        self::assertSame(ScopedCancellationContext::fromArray($fixture['contexts']['parent'])->toArray(), $intent->context->toArray());
        self::assertSame(8, $intent->boundary->sequence);
        self::assertSame($layout === 'timer' ? 1 : 4, $intent->boundary->sequenceSpan);
        self::assertNull($intent->preparationHistoryEventId);
        $fixture['task']['lease_owner'] = 'replacement';
        $fixture['task']['workflow_task_attempt'] = 17;
        self::assertEquals($intent, $replay()->cancellationScopeDelivery);
        self::assertSame([], $observed);
    }

    public static function pendingRequests(): iterable
    {
        foreach (['timer', 'group'] as $layout) {
            foreach ([false, true] as $inherited) { yield $layout.'-'.($inherited ? 'inherited' : 'ancestor-only') => [$layout, $inherited]; }
        }
    }

    public function test_pending_ancestor_cannot_bypass_a_competing_intermediate_request(): void
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-operation-projections.json'), true, flags: JSON_THROW_ON_ERROR)['competing'];
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            !in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)
            && ($event['event_type'] !== 'CancellationScopeRequested' || $event['payload']['scope_id'] !== 'desc-grandchild')));
        $scopes = new CancellationScopeHistory($fixture['history'], $fixture['task']['run_id']);
        $committed = new CommittedCancellationScopeHistory($fixture['history'], $fixture['task']['run_id'], $fixture['task']['workflow_id'], $scopes, requireCommittedDelivery: false);
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('original ancestor lineage');
        $committed->pendingRequestForScope('desc-grandchild', $scopes);
    }

    public function test_partial_descendant_group_remains_refused(): void
    {
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try { $this->replay(static function () use (&$entered): void { $entered = true; }, self::fixture('partial-group')); }
        finally { self::assertFalse($entered); }
    }

    public function test_ancestor_cannot_deliver_into_a_shielded_descendant(): void
    {
        $entered = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(static function () use (&$entered): void { $entered = true; }, self::fixture('shielded-operation')); }
        finally { self::assertFalse($entered); }
    }

    private function replay(callable $workflow, array $fixture, bool $prepare = false): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            localActivityExecutor: static function (): never { self::fail('An unadmitted callback cannot execute.'); },
            prepareLocalActivities: $prepare, allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    private static function workflow(array $fixture, array &$observed, string $change = '', string $cleanup = ''): callable
    {
        return static function (WorkflowContext $workflow) use ($fixture, &$observed, $change, $cleanup): mixed {
            $capture = static function (string $name, WorkflowCancelled $error) use ($workflow, &$observed, $cleanup): void {
                self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                $observed[$name] = $error->context;
                self::assertTrue($workflow->isCancellationRequested());
                self::assertSame($error->context, $workflow->cancellationContext());
                self::assertSame(17.0, $error->context->remaining());
                if ($cleanup === $name) { $workflow->cancellationShield(static fn () => $workflow->localActivity('cleanup-'.$name)); }
            };
            $workflow->cancellationScope(static function () use ($workflow, $fixture, $change, $capture): void {
                $workflow->cancellationScope(static function () use ($workflow, $fixture, $change, $capture): void {
                    $workflow->cancellationScope(static function () use ($workflow): void {
                        $workflow->cancellationScope(static function () use ($workflow): void {
                            self::assertFalse($workflow->isCancellationRequested());
                        });
                    }, shieldParent: true);
                    try {
                        $workflow->cancellationScope(static function () use ($workflow, $fixture, $change, $capture): void {
                            try {
                                $workflow->cancellationScope(static function () use ($workflow, $fixture, $change, $capture): void {
                                    if ($change !== 'prefix') { self::assertSame('prior-value', $workflow->activity('prior-step')); }
                                    try {
                                        if ($fixture['layout'] === 'timer') { $workflow->sleep(3600); }
                                        else {
                                            $activity = $workflow->deferActivity($change === 'activity' ? 'changed' : 'original-activity', [], [
                                                'cancellation_policy' => $change === 'activity-policy' ? 'abandon' : 'try_cancel', 'schedule_to_close_timeout' => 60]);
                                            $timer = $workflow->deferTimer($change === 'timer' ? 42 : 3600);
                                            $child = $workflow->deferChildWorkflow($change === 'child' ? 'changed' : 'original-child', [], [
                                                'cancellation_policy' => $change === 'child-policy' ? 'abandon' : 'wait_cancellation_completed', 'parent_close_policy' => 'request_cancel']);
                                            $condition = $workflow->deferCondition([PopulatedScopeGroupReplayTest::class, 'condition'], 'ready', 30);
                                            $members = $change === 'layout' ? [$activity, $timer, $child, $condition]
                                                : [$activity, new ParallelWorkflowCommand([$timer, $child]), $condition];
                                            if ($change === 'size') { array_pop($members); }
                                            $workflow->all($members);
                                        }
                                        self::fail('The original boundary must deliver cancellation.');
                                    } catch (WorkflowCancelled $error) { $capture('grandchild', $error); }
                                }, shieldParent: $change === 'shield');
                                $workflow->throwIfCancellationRequested();
                                self::fail('The child must retain its accepted request.');
                            } catch (WorkflowCancelled $error) { $capture('child', $error); }
                        });
                        $workflow->throwIfCancellationRequested();
                        self::fail('The ancestor must retain its accepted request.');
                    } catch (WorkflowCancelled $error) { $capture('parent', $error); }
                });
                self::assertFalse($workflow->isCancellationRequested());
                self::assertNull($workflow->cancellationContext());
            });
            self::assertFalse($workflow->isCancellationRequested());
            self::assertNull($workflow->cancellationContext());

            return $workflow->activity('unaffected-root');
        };
    }

    private static function fixture(string $layout): array
    {
        return json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-descendants.json'), true, flags: JSON_THROW_ON_ERROR)[$layout];
    }
}
