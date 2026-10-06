<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CancellationScopeOperationProjection;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeCleanupTimerTest extends TestCase
{
    public function test_cleanup_timer_keeps_original_proof_and_replacement_clock_without_cancelling_parent(): void
    {
        $fixture = self::fixture();
        $expected = ['type' => 'start_timer', 'delay_seconds' => 1,
            'cancellation_cleanup' => array_intersect_key(self::snapshot($fixture),
                array_flip(['scope_id', 'request_id', 'delivery_history_event_id'])),
            'cancellation_scope_id' => $fixture['scope_id']];
        self::assertEquals([$expected], $this->replay($fixture)->commands);
        self::scheduleCleanup($fixture);
        self::assertSame([], $this->replay($fixture)->commands);
        // Shielded cleanup must not be fenced again by a later ancestor inventory.
        self::assertSame([], CancellationScopeOperationProjection::fromHistoryPrefix('timer_members',
            $fixture['history'], $fixture['scope_id'], $fixture['task']['run_id']));
        self::append($fixture, 'TimerFired', ['sequence' => 4, 'timer_id' => 'cleanup-timer',
            'delay_seconds' => 1, 'cancellation_scope_id' => $fixture['scope_id']], '09');
        self::assertSame([['type' => 'start_timer', 'delay_seconds' => 1]], $this->replay($fixture)->commands);
        self::append($fixture, 'TimerScheduled', ['sequence' => 5, 'timer_id' => 'parent-timer',
            'delay_seconds' => 1, 'fire_at' => '2026-10-04T00:00:10.123456Z'], '09');
        self::append($fixture, 'TimerFired', ['sequence' => 5, 'timer_id' => 'parent-timer', 'delay_seconds' => 1], '10');
        $codec = new AvroPayloadCodec();
        foreach ([1, 17] as $attempt) {
            $fixture['task'] += ['lease_owner' => 'replacement'];
            $fixture['task']['workflow_task_attempt'] = $attempt;
            $result = $this->replay($fixture);
            self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
            self::assertSame(['request' => self::snapshot($fixture)['request_id'], 'remaining' => 20.0],
                $codec->decodeEnvelope($result->commands[0]['result']));
        }
    }

    #[DataProvider('invalidHistory')]
    public function test_changed_cleanup_authority_is_refused_before_workflow_entry(string $change): void
    {
        $fixture = self::fixture();
        self::scheduleCleanup($fixture);
        $last = array_key_last($fixture['history']);
        $event = &$fixture['history'][$last];
        if ($change === 'extra') { $event['payload']['cancellation_cleanup']['lease_owner'] = 'replacement'; }
        elseif ($change === 'missing') { unset($event['payload']['cancellation_cleanup']); }
        elseif ($change === 'null') { $event['payload']['cancellation_cleanup'] = null; }
        elseif ($change === 'earlier') { $event['sequence'] = 1; }
        elseif ($change === 'position') { $event['payload']['sequence'] = 3; }
        elseif ($change === 'timeout') { $event['payload']['timer_kind'] = 'condition_timeout'; }
        elseif ($change === 'recorded late') { $event['timestamp'] = '2026-10-04T00:00:26.123456Z'; }
        elseif ($change === 'fire late') { $event['payload']['fire_at'] = '2026-10-04T00:00:26.123456Z'; }
        elseif ($change === 'membership') { $event['payload']['cancellation_scope_id'] = $fixture['outer_scope_id']; }
        else { $event['payload']['cancellation_cleanup'][$change] = 'changed'; }
        unset($event);
        $entered = false;
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            $this->replay($fixture, static function () use (&$entered): void { $entered = true; });
        } finally { self::assertFalse($entered); }
    }

    public static function invalidHistory(): iterable
    {
        foreach (['scope_id', 'operation_scope_id', 'request_id', 'root_request_id',
            'delivery_history_event_id', 'preparation_history_event_id', 'cleanup_deadline_at', 'authority_deadline_at',
            'extra', 'missing', 'null', 'earlier', 'position', 'timeout', 'recorded late', 'fire late', 'membership'] as $change) {
            yield $change => [$change];
        }
    }

    public function test_removing_the_cleanup_shield_cannot_replay_an_admitted_timer(): void
    {
        $fixture = self::fixture();
        self::scheduleCleanup($fixture);
        $this->expectException(WorkflowClaimAborted::class);
        $this->replay($fixture, self::workflow(shield: false));
    }

    #[DataProvider('descendants')]
    public function test_descendant_cleanup_uses_original_ancestor_receipt_and_its_narrower_authority(string $target): void
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-descendants.json'), true,
            flags: JSON_THROW_ON_ERROR)['timer'];
        $context = ScopedCancellationContext::fromArray($fixture['contexts'][$target]);
        self::append($fixture, 'TimerScheduled', ['sequence' => 9, 'timer_id' => 'descendant-cleanup',
            'delay_seconds' => 1, 'fire_at' => '2026-10-04T00:00:13.123456Z',
            'cancellation_scope_id' => $fixture['scopes'][$target], 'cancellation_cleanup' => [
                'scope_id' => $fixture['scopes'][$target], 'operation_scope_id' => $fixture['scopes'][$target],
                'request_id' => $context->requestId, 'root_request_id' => $context->rootRequestId,
                'delivery_history_event_id' => 'ancestor-delivered', 'preparation_history_event_id' => 'ancestor-prepared',
                'cleanup_deadline_at' => '2026-10-04T00:00:30.123456Z', 'authority_deadline_at' => '2026-10-04T00:00:26.123456Z',
            ]], '12');
        $history = new CommittedCancellationScopeHistory($fixture['history'], $fixture['task']['run_id'],
            $fixture['task']['workflow_id'], new CancellationScopeHistory($fixture['history'], $fixture['task']['run_id']),
            inspectOperationProjections: true);
        self::assertCount(1, $history->deliveries);
    }

    public static function descendants(): array { return [['parent'], ['child'], ['grandchild']]; }

    private function replay(array $fixture, ?callable $workflow = null): ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow ?? self::workflow(), $fixture['history'], [],
            'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    private static function workflow(bool $shield = true): callable
    {
        return static function (WorkflowContext $workflow) use ($shield): array {
            $context = $workflow->cancellationScope(static fn () => $workflow->cancellationScope(
                static function () use ($workflow, $shield): ScopedCancellationContext {
                    try { $workflow->sleep(10); self::fail('Original call must receive cancellation.'); }
                    catch (WorkflowCancelled $error) {
                        self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                        if ($shield) { $workflow->cancellationShield(static fn () => $workflow->sleep(1)); }
                        else { $workflow->sleep(1); }
                        return $error->context;
                    }
                },
            ));
            self::assertFalse($workflow->isCancellationRequested());
            self::assertNull($workflow->cancellationContext());
            $workflow->sleep(1);
            return ['request' => $context->requestId, 'remaining' => $context->remaining()];
        };
    }

    private static function fixture(): array
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true,
            flags: JSON_THROW_ON_ERROR)['unshielded'];
        foreach ($fixture['history'] as &$event) {
            if (in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)) {
                $event['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.123456Z';
            }
        }
        return $fixture;
    }

    private static function snapshot(array $fixture): array
    {
        $delivery = array_values(array_filter($fixture['history'],
            static fn (array $event): bool => $event['event_type'] === 'CancellationScopeDelivered'))[0];
        $context = ScopedCancellationContext::fromArray($delivery['payload']['cancellation']);
        return ['scope_id' => $context->scopeId, 'operation_scope_id' => $context->scopeId,
            'request_id' => $context->requestId, 'root_request_id' => $context->rootRequestId,
            'delivery_history_event_id' => $delivery['id'],
            'preparation_history_event_id' => $delivery['payload']['preparation_history_event_id'],
            'cleanup_deadline_at' => '2026-10-04T00:00:30.123456Z', 'authority_deadline_at' => '2026-10-04T00:00:26.123456Z'];
    }

    private static function scheduleCleanup(array &$fixture): void
    {
        self::append($fixture, 'TimerScheduled', ['sequence' => 4, 'timer_id' => 'cleanup-timer', 'delay_seconds' => 1,
            'fire_at' => '2026-10-04T00:00:09.123456Z', 'cancellation_scope_id' => $fixture['scope_id'],
            'cancellation_cleanup' => self::snapshot($fixture)], '08');
    }

    private static function append(array &$fixture, string $type, array $payload, string $second): void
    {
        $history = &$fixture['history'];
        $last = $history[array_key_last($history)];
        $history[] = ['id' => 'timer-cleanup-'.count($history), 'namespace' => $last['namespace'],
            'sequence' => $last['sequence'] + 1, 'event_type' => $type, 'payload' => $payload,
            'timestamp' => '2026-10-04T00:00:'.$second.'.123456Z'];
    }
}
