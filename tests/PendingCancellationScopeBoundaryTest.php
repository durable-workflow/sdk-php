<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationScopeDeliveryIntent;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PendingCancellationScopeBoundaryTest extends TestCase
{
    #[DataProvider('requests')]
    public function test_pending_request_selects_original_call_without_exposing_cleanup(bool $prepared, bool $shielded): void
    {
        $fixture = self::directFixture($prepared, $shielded);
        $cleaned = false;
        $result = $this->replay(self::directWorkflow($cleaned, shielded: $shielded), $fixture);
        $intent = $result->cancellationScopeDelivery;
        self::assertInstanceOf(CancellationScopeDeliveryIntent::class, $intent);
        self::assertSame([], $result->commands);
        self::assertNull($result->preparedLocalActivity);
        self::assertNull($result->cancellationDelivery);
        self::assertNull($result->terminalFailure);
        self::assertFalse($cleaned);
        self::assertSame(ScopedCancellationContext::fromArray($fixture['history'][4]['payload']['cancellation'])->toArray(), $intent->context->toArray());
        self::assertSame(3, $intent->boundary->sequence);
        self::assertSame('timer', $intent->boundary->callKind);
        self::assertSame(1, $intent->boundary->sequenceSpan);
        self::assertSame($intent->context->requestId, $intent->boundary->requestId);
        self::assertSame($prepared ? $fixture['history'][5]['id'] : null, $intent->preparationHistoryEventId);
        self::assertSame($prepared ? '2026-10-04T00:00:26.000000Z' : null,
            $intent->authorityDeadline?->format('Y-m-d\TH:i:s.u\Z'));
        $fixture['task'] += ['lease_owner' => 'replacement', 'workflow_task_attempt' => 19];
        $again = $this->replay(self::directWorkflow($cleaned, shielded: $shielded), $fixture)->cancellationScopeDelivery;
        self::assertEquals($intent, $again);
        self::assertFalse($cleaned);
    }

    public static function requests(): iterable
    {
        foreach ([false, true] as $prepared) {
            foreach ([false, true] as $shielded) { yield ($prepared ? 'prepared' : 'requested').'-'.($shielded ? 'shielded' : 'plain') => [$prepared, $shielded]; }
        }
    }

    #[DataProvider('preparations')]
    public function test_pending_ancestor_selects_its_original_identity_after_completed_descendant_prefix(bool $prepared): void
    {
        $fixture = self::descendantFixture($prepared);
        $cleaned = false;
        $result = $this->replay(self::descendantWorkflow($cleaned), $fixture);
        $intent = $result->cancellationScopeDelivery;
        self::assertInstanceOf(CancellationScopeDeliveryIntent::class, $intent);
        self::assertSame([], $result->commands);
        self::assertFalse($cleaned);
        self::assertSame('parent-scope', $intent->context->scopeId);
        self::assertSame(ScopedCancellationContext::fromArray($fixture['contexts']['parent'])->toArray(), $intent->context->toArray());
        self::assertSame(8, $intent->boundary->sequence);
        self::assertSame('timer', $intent->boundary->callKind);
        self::assertSame($prepared ? 'ancestor-prepared' : null, $intent->preparationHistoryEventId);
        self::assertSame($prepared ? '2026-10-04T00:00:26.123456Z' : null,
            $intent->authorityDeadline?->format('Y-m-d\TH:i:s.u\Z'));
        $fixture['task'] += ['lease_owner' => 'replacement', 'workflow_task_attempt' => 29];
        self::assertEquals($intent, $this->replay(self::descendantWorkflow($cleaned), $fixture)->cancellationScopeDelivery);
    }

    public static function preparations(): iterable { yield 'requested' => [false]; yield 'prepared' => [true]; }

    public function test_committed_delivery_replays_cleanup_and_never_emits_another_pending_intent(): void
    {
        $fixture = self::directFixture(true);
        $complete = self::fixture('committed-scope-delivery.json')['unshielded']['history'];
        $fixture['history'][] = $complete[6];
        $fixture['history'][6]['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.000000Z';
        $cleaned = false;
        $result = $this->replay(self::directWorkflow($cleaned), $fixture);
        self::assertTrue($cleaned);
        self::assertNull($result->cancellationScopeDelivery);
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('cleaned', (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    #[DataProvider('preparedChanges')]
    public function test_prepared_call_cannot_be_replaced_before_cleanup(string $change): void
    {
        $fixture = self::directFixture(true);
        $cleaned = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(self::directWorkflow($cleaned, $change), $fixture); }
        finally { self::assertFalse($cleaned); }
    }

    public static function preparedChanges(): iterable
    {
        yield 'kind' => ['activity'];
        yield 'skip' => ['skip'];
    }

    public function test_changed_admitted_descendant_descriptor_cannot_emit_an_intent(): void
    {
        $fixture = self::descendantFixture(false);
        $cleaned = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(self::descendantWorkflow($cleaned, prior: 'changed'), $fixture); }
        finally { self::assertFalse($cleaned); }
    }

    public function test_shielded_active_descendant_never_borrows_an_ancestor_request(): void
    {
        $fixture = self::descendantFixture(false);
        // The independently produced tree already has a shielded branch without a request.
        $scopes = new CancellationScopeHistory($fixture['history'], $fixture['task']['run_id']);
        $pending = new CommittedCancellationScopeHistory($fixture['history'], $fixture['task']['run_id'],
            $fixture['task']['workflow_id'], $scopes, requireCommittedDelivery: false, inspectOperationProjections: true);
        self::assertNull($pending->pendingRequestForScope($fixture['scopes']['shield-child'], $scopes));
        self::assertSame('parent-scope', $pending->pendingRequestForScope($fixture['scopes']['grandchild'], $scopes)['context']->scopeId);
    }

    #[DataProvider('preparations')]
    public function test_explicit_cleanup_shield_keeps_original_pending_call_without_delivery(bool $prepared): void
    {
        $cleaned = false;
        $result = $this->replay(self::descendantWorkflow($cleaned, shield: true), self::descendantFixture($prepared));
        self::assertNull($result->cancellationScopeDelivery);
        self::assertNull($result->terminalFailure);
        self::assertSame([], $result->commands);
        self::assertFalse($cleaned);
    }

    public function test_result_committed_after_request_cannot_replace_its_earlier_cancellation_boundary(): void
    {
        $fixture = self::descendantFixture(false);
        $completion = array_splice($fixture['history'], 9, 1)[0];
        $fixture['history'][] = $completion;
        // The request and result retain actual canonical order, with monotonic history positions.
        foreach ($fixture['history'] as $index => &$event) { $event['sequence'] = $index + 1; }
        unset($event);
        $cleaned = false;
        $result = $this->replay(self::descendantWorkflow($cleaned), $fixture);
        self::assertSame(7, $result->cancellationScopeDelivery->boundary->sequence);
        self::assertSame('activity', $result->cancellationScopeDelivery->boundary->callKind);
        self::assertSame('parent-scope', $result->cancellationScopeDelivery->context->scopeId);
        self::assertSame([], $result->commands);
        self::assertFalse($cleaned);
    }

    #[DataProvider('preparations')]
    public function test_ancestor_group_selects_original_identity_without_entering_cleanup(bool $prepared): void
    {
        $fixture = self::fixture('committed-scope-descendants.json')['group'];
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] !== 'CancellationScopeDelivered'
            && ($prepared || $event['event_type'] !== 'CancellationScopeDeliveryPrepared')));
        $cleaned = false;
        $result = $this->replay(self::descendantWorkflow($cleaned, group: true), $fixture);
        self::assertFalse($cleaned);
        self::assertSame([], $result->commands);
        self::assertNotNull($result->cancellationScopeDelivery);
        self::assertSame('parent-scope', $result->cancellationScopeDelivery->context->scopeId);
        self::assertSame('parallel', $result->cancellationScopeDelivery->boundary->callKind);
        self::assertSame(8, $result->cancellationScopeDelivery->boundary->sequence);
        self::assertSame(4, $result->cancellationScopeDelivery->boundary->sequenceSpan);
    }

    public function test_committed_preparation_requires_its_accepted_descendant_lineage(): void
    {
        $fixture = self::descendantFixture(true);
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] !== 'CancellationScopeRequested' || $event['payload']['scope_id'] !== 'grandchild-scope'));
        $cleaned = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try { $this->replay(self::descendantWorkflow($cleaned), $fixture); }
        finally { self::assertFalse($cleaned); }
    }

    public function test_pending_selection_requires_explicit_internal_committed_replay(): void
    {
        $entered = false;
        $this->expectException(LogicException::class);
        try { (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; }, [], [],
            'php-workers', prepareCancellationScopeDelivery: true); }
        finally { self::assertFalse($entered); }
    }

    public function test_independent_descendant_request_cannot_borrow_ancestor_identity(): void
    {
        $fixture = self::descendantFixture(false);
        foreach ($fixture['history'] as &$event) {
            if ($event['event_type'] !== 'CancellationScopeRequested' || $event['payload']['scope_id'] !== 'grandchild-scope') { continue; }
            $context = &$event['payload']['cancellation'];
            $last = $context['lineage'][array_key_last($context['lineage'])];
            $context['lineage'] = [$last];
            $context['root_context']['request_id'] = $last['request_id'];
            $context['root_context']['root_request_id'] = $last['request_id'];
            $context['root_context']['lineage'] = [array_intersect_key($last,
                array_flip(['request_id', 'workflow_instance_id', 'workflow_run_id']))];
            $event['payload']['parent_scope_id'] = null;
            unset($context);
        }
        unset($event);
        $cleaned = false;
        $this->expectException(WorkflowClaimAborted::class);
        try { $this->replay(self::descendantWorkflow($cleaned), $fixture); }
        finally { self::assertFalse($cleaned); }
    }

    #[DataProvider('preparations')]
    public function test_default_replay_still_refuses_pending_history_before_workflow_entry(bool $prepared): void
    {
        $fixture = self::directFixture($prepared);
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try { (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; },
            $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true); }
        finally { self::assertFalse($entered); }
    }

    private function replay(callable $workflow, array $fixture): ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            localActivityExecutor: static function (): never { self::fail('Unadmitted callback must not run.'); },
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
    }

    private static function directWorkflow(bool &$cleaned, string $change = '', bool $shielded = false): callable
    {
        return static function (WorkflowContext $workflow) use (&$cleaned, $change, $shielded): string {
            return $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $change, $shielded): string {
                return $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $change): string {
                    self::assertFalse($workflow->isCancellationRequested());
                    self::assertNull($workflow->cancellationContext());
                    try {
                        if ($change === 'activity') { $workflow->activity('changed'); }
                        elseif ($change !== 'skip') { $workflow->sleep(10); }
                        return 'uninterrupted';
                    } catch (WorkflowCancelled $error) {
                        self::assertSame($error->context, $workflow->cancellationContext());
                        $cleaned = true;
                        return 'cleaned';
                    }
                }, shieldParent: $shielded);
            });
        };
    }

    private static function descendantWorkflow(bool &$cleaned, string $prior = 'prior-step', bool $group = false, bool $shield = false): callable
    {
        return static function (WorkflowContext $workflow) use (&$cleaned, $prior, $group, $shield): mixed {
            return $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $prior, $group, $shield): mixed {
                return $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $prior, $group, $shield): void {
                    $workflow->cancellationScope(static fn () => $workflow->cancellationScope(static function (): void {}), shieldParent: true);
                    $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $prior, $group, $shield): void {
                        $workflow->cancellationScope(static function () use ($workflow, &$cleaned, $prior, $group, $shield): void {
                            self::assertSame('prior-value', $workflow->activity($prior));
                            try {
                                if ($shield) { $workflow->cancellationShield(static fn () => $workflow->sleep(3600)); }
                                elseif (!$group) { $workflow->sleep(3600); }
                                else {
                                    $workflow->all([
                                        $workflow->deferActivity('original-activity', [], ['cancellation_policy' => 'try_cancel', 'schedule_to_close_timeout' => 60]),
                                        new \DurableWorkflow\Worker\ParallelWorkflowCommand([
                                            $workflow->deferTimer(3600),
                                            $workflow->deferChildWorkflow('original-child', [], ['cancellation_policy' => 'wait_cancellation_completed', 'parent_close_policy' => 'request_cancel']),
                                        ]),
                                        $workflow->deferCondition([PopulatedScopeGroupReplayTest::class, 'condition'], 'ready', 30),
                                    ]);
                                }
                            } catch (WorkflowCancelled) { $cleaned = true; }
                        });
                    });
                });
            });
        };
    }

    private static function directFixture(bool $prepared, bool $shielded = false): array
    {
        $fixture = self::fixture('committed-scope-delivery.json')[$shielded ? 'shielded' : 'unshielded'];
        $fixture['history'] = array_slice($fixture['history'], 0, $prepared ? 6 : 5);
        if ($prepared) { $fixture['history'][5]['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.000000Z'; }
        return $fixture;
    }

    private static function descendantFixture(bool $prepared): array
    {
        $fixture = self::fixture('committed-scope-descendants.json')['timer'];
        $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] !== 'CancellationScopeDelivered'
            && ($prepared || $event['event_type'] !== 'CancellationScopeDeliveryPrepared')));
        return $fixture;
    }

    private static function fixture(string $name): array
    {
        return json_decode(file_get_contents(__DIR__.'/fixtures/'.$name), true, flags: JSON_THROW_ON_ERROR);
    }
}
