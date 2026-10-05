<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PopulatedCancellationScopeReplayTest extends TestCase
{
    public static bool $conditionSatisfied = false;

    public static function condition(): bool
    {
        return self::$conditionSatisfied;
    }

    #[DataProvider('operations')]
    public function test_original_admitted_call_is_consumed_without_reissuing_or_cancelling_parent(string $scenario): void
    {
        self::$conditionSatisfied = false;
        $fixture = self::fixture($scenario);
        $context = null;
        $prior = null;
        $workflow = self::workflow($fixture, $context, $prior);
        $first = $this->replay($workflow, $fixture);
        self::assertSame('prior-value', $prior);
        self::assertInstanceOf(ScopedCancellationContext::class, $context);
        self::assertSame($fixture['scope_id'], $context->scopeId);
        $request = array_values(array_filter($fixture['history'], static fn (array $event): bool => $event['event_type'] === 'CancellationScopeRequested'))[0];
        self::assertEquals($request['payload']['cancellation'], $context->toArray());
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));
        self::assertSame('unaffected-root', $first->commands[0]['activity_type']);
        self::assertArrayNotHasKey('cancellation_scope_id', $first->commands[0]);
        self::assertNull($first->cancellationDelivery);

        $fixture['history'][] = self::event($fixture, 'ActivityCompleted', [
            'sequence' => 5, 'activity_type' => 'unaffected-root', 'result' => (new AvroPayloadCodec())->envelope('survivor'),
        ]);
        $finished = $this->replay($workflow, $fixture);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame(['survivor', $context->requestId, 21.0], (new AvroPayloadCodec())->decodeEnvelope($finished->commands[0]['result']));
        $fixture['task'] += ['task_id' => 'replacement-task', 'lease_owner' => 'replacement', 'workflow_task_attempt' => 17];
        self::assertSame($finished->commands, $this->replay($workflow, $fixture)->commands);
    }

    public static function operations(): iterable
    {
        foreach (['activity-try_cancel', 'activity-wait_cancellation_completed', 'activity-abandon',
            'child-try_cancel', 'child-wait_cancellation_completed', 'child-abandon', 'timer', 'condition', 'condition-untimed'] as $scenario) {
            yield $scenario => [$scenario];
        }
    }

    public function test_committed_condition_delivery_wins_when_the_same_predicate_is_now_satisfied(): void
    {
        self::$conditionSatisfied = true;
        $fixture = self::fixture('condition');
        $context = null;
        $prior = null;
        $result = $this->replay(self::workflow($fixture, $context, $prior), $fixture);
        self::assertInstanceOf(ScopedCancellationContext::class, $context);
        self::assertSame(['schedule_activity'], array_column($result->commands, 'type'));
        self::$conditionSatisfied = false;
    }

    #[DataProvider('operations')]
    public function test_shielded_cleanup_keeps_original_populated_delivery_and_narrower_authority(string $scenario): void
    {
        self::$conditionSatisfied = false;
        $fixture = self::fixture($scenario);
        foreach ($fixture['history'] as &$event) {
            if (in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)) {
                $event['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.123456Z';
            }
        }
        unset($event);
        $context = null;
        $prior = null;
        $workflow = self::workflow($fixture, $context, $prior, 'cleanup');
        $result = $this->replay($workflow, $fixture, true);
        $call = $result->preparedLocalActivity;
        self::assertNotNull($call);
        self::assertSame(5, $call->sequence);
        self::assertFalse($call->recover);
        self::assertSame([], $result->commands);
        self::assertSame('original-cleanup', $call->command->attributes['activity_type']);
        self::assertSame($fixture['scope_id'], $call->command->attributes['cancellation_scope_id']);
        $snapshot = $call->cleanupSnapshot();
        self::assertSame($context->requestId, $snapshot['request_id']);
        self::assertSame($context->rootRequestId, $snapshot['root_request_id']);
        self::assertSame('2026-10-04T00:00:30.123456Z', $snapshot['cleanup_deadline_at']);
        self::assertSame('2026-10-04T00:00:26.123456Z', $snapshot['authority_deadline_at']);
        self::assertSame($fixture['history'][array_key_last($fixture['history'])]['id'], $snapshot['delivery_history_event_id']);
        self::assertSame($fixture['history'][array_key_last($fixture['history'])]['payload']['preparation_history_event_id'], $snapshot['preparation_history_event_id']);
        $fixture['task'] += ['lease_owner' => 'replacement', 'workflow_task_attempt' => 17];
        self::assertSame($snapshot, $this->replay($workflow, $fixture, true)->preparedLocalActivity->cleanupSnapshot());
    }

    #[DataProvider('changedCalls')]
    public function test_changed_admitted_call_is_refused_before_cleanup_or_parent_work(string $scenario, string $change): void
    {
        $fixture = self::fixture($scenario);
        $context = null;
        $prior = null;
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            $this->replay(self::workflow($fixture, $context, $prior, $change), $fixture);
        } finally {
            self::assertNull($context);
        }
    }

    public static function changedCalls(): iterable
    {
        yield 'Activity type' => ['activity-try_cancel', 'type'];
        yield 'Activity policy' => ['activity-try_cancel', 'policy'];
        yield 'timer delay' => ['timer', 'delay'];
        yield 'child type' => ['child-try_cancel', 'type'];
        yield 'child policy' => ['child-try_cancel', 'policy'];
        yield 'child parent policy' => ['child-try_cancel', 'parent-policy'];
        yield 'condition key' => ['condition', 'key'];
        yield 'condition timeout' => ['condition', 'timeout'];
        yield 'condition predicate' => ['condition', 'predicate'];
        yield 'omitted completed prefix' => ['timer', 'prefix'];
        yield 'changed call kind' => ['timer', 'kind'];
        yield 'new shield at delivery' => ['timer', 'shield'];
    }

    public function test_worker_default_still_refuses_populated_scope_delivery_before_entry(): void
    {
        $entered = false;
        $fixture = self::fixture('activity-try_cancel');
        $this->expectException(WorkflowClaimAborted::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; },
                $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true);
        } finally {
            self::assertFalse($entered);
        }
    }

    public function test_descendant_execution_remains_refused_before_entry(): void
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-operation-projections.json'), true, flags: JSON_THROW_ON_ERROR)['descendants'];
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, $fixture);
        } finally {
            self::assertFalse($entered);
        }
    }

    private function replay(callable $workflow, array $fixture, bool $prepared = false): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            localActivityExecutor: $prepared ? static function (): never { self::fail('Replay cannot execute an unadmitted cleanup callback.'); } : null,
            prepareLocalActivities: $prepared, allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    private static function workflow(array $fixture, ?ScopedCancellationContext &$context, mixed &$prior, string $change = ''): callable
    {
        return static function (WorkflowContext $workflow) use ($fixture, &$context, &$prior, $change): array {
            $request = $workflow->cancellationScope(static function () use ($workflow, $fixture, &$context, &$prior, $change): string {
                return $workflow->cancellationScope(static function () use ($workflow, $fixture, &$context, &$prior, $change): string {
                if ($change !== 'prefix') { $prior = $workflow->activity('prior-step'); }
                $call = static function () use ($workflow, $fixture, $change): void {
                    $policy = $change === 'policy' ? 'abandon' : ($fixture['policy'] ?? 'try_cancel');
                    match ($change === 'kind' ? 'activity' : $fixture['kind']) {
                        'activity' => $workflow->activity($change === 'type' ? 'changed' : 'original-activity', [], [
                            'cancellation_policy' => $policy, 'schedule_to_close_timeout' => 60,
                        ]),
                        'child' => $workflow->childWorkflow($change === 'type' ? 'changed' : 'original-child', [], [
                            'cancellation_policy' => $policy, 'parent_close_policy' => $change === 'parent-policy' ? 'abandon' : 'request_cancel',
                        ]),
                        'timer' => $workflow->sleep($change === 'delay' ? 42 : 3600),
                        'condition' => $workflow->waitCondition($change === 'predicate' ? static fn () => false : [self::class, 'condition'],
                            $change === 'key' ? 'changed' : 'ready', $change === 'timeout' ? 42 : $fixture['timeout']),
                    };
                    self::fail('A committed scoped call must deliver its original request.');
                };
                try {
                    if ($change === 'shield') { $workflow->cancellationShield($call); } else { $call(); }
                } catch (WorkflowCancelled $error) {
                    self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                    $context = $error->context;
                    self::assertSame(21.0, $context->remaining());
                    self::assertTrue($workflow->isCancellationRequested());
                    $workflow->cancellationShield(static fn () => $workflow->throwIfCancellationRequested());
                    if ($change === 'cleanup') {
                        $workflow->cancellationShield(static fn () => $workflow->localActivity('original-cleanup'));
                    }
                    return $context->requestId;
                }
                return '';
                });
            });
            self::assertFalse($workflow->isCancellationRequested());
            self::assertNull($workflow->cancellationContext());
            $remaining = $context->remaining();
            return [$workflow->activity('unaffected-root'), $request, $remaining];
        };
    }

    private static function fixture(string $scenario): array
    {
        return json_decode(file_get_contents(__DIR__.'/fixtures/populated-scope-single-calls.json'), true, flags: JSON_THROW_ON_ERROR)[$scenario];
    }

    private static function event(array $fixture, string $kind, array $payload): array
    {
        $last = $fixture['history'][array_key_last($fixture['history'])];
        return ['id' => 'survivor-completed', 'namespace' => $last['namespace'], 'sequence' => $last['sequence'] + 1,
            'event_type' => $kind, 'payload' => $payload, 'timestamp' => '2026-10-04T00:00:20.123456Z'];
    }
}
