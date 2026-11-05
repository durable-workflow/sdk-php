<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\RedriveFixtureRegistry;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use JsonSerializable;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A recorded SearchAttributesUpserted binds replay by value, the way a
 * recorded memo does: workflow code that upserts different values no longer
 * matches its history. This matters most for redrive, where the successor
 * reuses the failed run's upsert instead of running it again.
 *
 * These legacy one-argument calls bind values. Explicit type declarations
 * and their replay identity are covered in TypedSearchAttributeReplayTest.
 */
final class SearchAttributeReplayTest extends TestCase
{
    public function testRedriveReusedSearchAttributesRejectAChangedValue(): void
    {
        $path = RedriveFixtureRegistry::path('redrive-reused-search-attributes-changed');
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(3, $fixture['history'][0]['payload']['resume_step_sequence']);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, ['MemoUpserted', 'SearchAttributesUpserted']));
        $reused = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] === 'SearchAttributesUpserted'));
        self::assertCount(1, $reused);
        self::assertSame(['Stage' => 'greeting'], $reused[0]['payload']['attributes']);
        self::assertSame($fixture['history'][0]['payload']['continued_from_run_id'], $reused[0]['payload']['reused_from_run_id']);
        self::assertIsString($reused[0]['payload']['reused_recorded_at']);

        $tasks = ReplayRegressionFixture::executeFile($path);

        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];
        self::assertCount(1, $commands);
        self::assertSame('replay_error', $commands[0]['type']);
        self::assertSame('search_attribute_value_mismatch', $commands[0]['reason']);
        self::assertSame(2, $commands[0]['sequence']);
    }

    /** @param array<string, mixed> $attributes */
    #[DataProvider('changedCalls')]
    public function testChangedSearchAttributeValuesAreRejected(array $attributes): void
    {
        $exception = $this->refusal($attributes, ['Stage' => 'greeting', 'Count' => 2]);

        self::assertSame('search_attribute_value_mismatch', $exception->reason);
        self::assertSame(1, $exception->sequence);
        // Diagnostics stay payload-free.
        self::assertSame('the recorded search attributes', $exception->expected);
        foreach (['greeting', 'approved', 'eu'] as $value) {
            self::assertStringNotContainsString($value, $exception->getMessage().$exception->actual);
        }
    }

    /** @return array<string, array{array<string, mixed>}> */
    public static function changedCalls(): array
    {
        return [
            'changed value' => [['Stage' => 'approved', 'Count' => 2]],
            'changed PHP value type' => [['Stage' => 'greeting', 'Count' => '2']],
            'added key' => [['Stage' => 'greeting', 'Count' => 2, 'Region' => 'eu']],
            'removed key' => [['Stage' => 'greeting']],
            'deleted key' => [['Stage' => 'greeting', 'Count' => null]],
        ];
    }

    public function testCallMatchesTheValuesCoreNormalizedIntoHistory(): void
    {
        $result = $this->replay(
            ['Tags' => [' urgent ', 'urgent'], 'Stage' => " greeting\n", 'Gone' => '', 'Ratio' => 2.0, 'Count' => 9_007_199_254_740_993, 'Flag' => false],
            ['Count' => 9_007_199_254_740_993, 'Flag' => false, 'Gone' => null, 'Ratio' => 2.0, 'Stage' => 'greeting', 'Tags' => ['urgent', 'urgent']],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * Values outside Core's portable set refuse replay with a typed reason
     * instead of being coerced into a matching JSON form.
     *
     * @param array<array-key, mixed> $authored
     * @param array<array-key, mixed> $recorded
     */
    #[DataProvider('unportableAttributes')]
    public function testUnportableSearchAttributesRefuseReplay(array $authored, array $recorded, string $origin): void
    {
        $exception = $this->refusal($authored, $recorded);

        self::assertSame('search_attributes_unportable', $exception->reason);
        self::assertSame(1, $exception->sequence);
        self::assertStringContainsString("The {$origin} search attributes", $exception->getMessage());
    }

    /** @return array<string, array{array<array-key, mixed>, array<array-key, mixed>, string}> */
    public static function unportableAttributes(): array
    {
        $greeting = new class implements JsonSerializable {
            public function jsonSerialize(): string
            {
                return 'greeting';
            }
        };

        return [
            'numeric key' => [[1 => 'x', 'Stage' => 'greeting'], [1 => 'x', 'Stage' => 'greeting'], 'recorded'],
            'authored numeric key' => [[1 => 'x', 'Stage' => 'greeting'], ['1x' => 'x', 'Stage' => 'greeting'], 'authored'],
            'key with a space' => [['Stage name' => 'x'], ['Stage' => 'x'], 'authored'],
            'object with the same JSON form' => [['Stage' => $greeting], ['Stage' => 'greeting'], 'authored'],
            'object with the same shape as a map' => [['Stage' => (object) ['a' => 'x']], ['Stage' => ['x']], 'authored'],
            'associative array' => [['Stage' => ['a' => 'x']], ['Stage' => ['a' => 'x']], 'recorded'],
            'nested list' => [['Tags' => [['x']]], ['Tags' => [['x']]], 'recorded'],
            'non-string list members' => [['Tags' => [1, 2]], ['Tags' => [1, 2]], 'recorded'],
            'authored non-string list member' => [['Tags' => ['x', true]], ['Tags' => ['x', '1']], 'authored'],
            'not a number' => [['Ratio' => NAN], ['Ratio' => 1.0], 'authored'],
            'infinity' => [['Ratio' => INF], ['Ratio' => 1.0], 'authored'],
            'negative infinity' => [['Ratio' => -INF], ['Ratio' => 1.0], 'authored'],
        ];
    }

    /** @param list<array<string, mixed>> $history */
    #[DataProvider('collidingHistories')]
    public function testOneSequenceCannotHoldTwoSearchAttributeUpsertsOrAnotherCommand(array $history, string $reason): void
    {
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static function (WorkflowContext $context): mixed {
                    $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return $context->activity('golden.activity');
                },
                $history,
                [],
                'search-attribute-replay',
            );
            self::fail('A sequence must hold one durable command.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame($reason, $exception->reason);
            self::assertSame(1, $exception->sequence);
        }
    }

    /** @return array<string, array{list<array<string, mixed>>, string}> */
    public static function collidingHistories(): array
    {
        $upsert = static fn (string $stage): array => ['event_type' => 'SearchAttributesUpserted',
            'payload' => ['sequence' => 1, 'attributes' => ['Stage' => $stage], 'merged' => ['Stage' => $stage]]];
        $activity = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'golden.activity',
            'result' => (new AvroPayloadCodec())->envelope('done')]];

        return [
            'two upserts' => [[$upsert('greeting'), $upsert('approved')], 'duplicate_search_attribute_update'],
            'two equal upserts' => [[$upsert('greeting'), $upsert('greeting')], 'duplicate_search_attribute_update'],
            'upsert after another command' => [[$activity, $upsert('greeting')], 'durable_command_sequence_collision'],
            'another command after an upsert' => [[$upsert('greeting'), $activity], 'durable_command_sequence_collision'],
            'upsert on a signal wait step' => [[['event_type' => 'SignalWaitOpened',
                'payload' => ['sequence' => 1, 'signal_name' => 'approve', 'signal_wait_id' => 'wait-1']], $upsert('greeting')],
                'durable_command_sequence_collision'],
            'signal applied on an upsert step' => [[$upsert('greeting'), ['event_type' => 'SignalApplied',
                'payload' => ['sequence' => 1, 'signal_name' => 'approve', 'signal_wait_id' => 'wait-1']]],
                'durable_command_sequence_collision'],
        ];
    }

    /** @param callable(list<array<string, mixed>>): list<array<string, mixed>> $mutate */
    #[DataProvider('collidingRedriveHistories')]
    public function testReusedSearchAttributeUpsertCannotShareItsStep(callable $mutate, string $reason): void
    {
        $path = RedriveFixtureRegistry::path('redrive-reused-upserts-clock');
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        $fixture['history'] = $mutate($fixture['history']);
        $fixture['expected'] = ['workflow_tasks' => [['command_sequence' => [['type' => 'replay_error', 'reason' => $reason]]]]];

        $tasks = ReplayRegressionFixture::execute($fixture);

        self::assertSame('replay_error', $tasks[0]['command_sequence'][0]['type']);
        self::assertSame($reason, $tasks[0]['command_sequence'][0]['reason']);
    }

    /** @return array<string, array{callable(list<array<string, mixed>>): list<array<string, mixed>>, string}> */
    public static function collidingRedriveHistories(): array
    {
        return [
            'reused upsert recorded twice' => [static function (array $history): array {
                $copy = $history[2];
                $copy['payload']['attributes'] = ['Stage' => 'approved'];
                array_splice($history, 3, 0, [$copy]);
                foreach ($history as $index => $event) {
                    $history[$index]['sequence'] = $index + 1;
                }

                return $history;
            }, 'duplicate_search_attribute_update'],
            'reused upsert on the memo step' => [static function (array $history): array {
                $history[2]['payload']['sequence'] = 1;

                return $history;
            }, 'durable_command_sequence_collision'],
        ];
    }

    /**
     * Each terminal or internal-timeout row claims its owning command's
     * sequence even when that command's opener is absent. With an upsert at
     * the same sequence, replay refuses in either order.
     *
     * @param array<string, mixed> $row
     */
    #[DataProvider('lifecycleRowsWithoutTheirOpener')]
    public function testALifecycleRowWithoutItsOpenerCannotShareAnUpsertsSequence(array $row, string $upsert, bool $upsertFirst): void
    {
        $codec = new AvroPayloadCodec();
        $upsertEvent = $upsert === 'memo'
            ? ['event_type' => 'MemoUpserted', 'payload' => ['sequence' => 1, 'entries' => $codec->envelope(['stage' => 'greeting']),
                'merged' => $codec->envelope(['stage' => 'greeting'])]]
            : ['event_type' => 'SearchAttributesUpserted', 'payload' => ['sequence' => 1, 'attributes' => ['Stage' => 'greeting'],
                'merged' => ['Stage' => 'greeting']]];
        $history = $upsertFirst ? [$upsertEvent, $row] : [$row, $upsertEvent];
        try {
            (new Replayer($codec))->replay(
                static function (WorkflowContext $context) use ($upsert): string {
                    $upsert === 'memo'
                        ? $context->upsertMemo(['stage' => 'greeting'])
                        : $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return 'done';
                },
                $history,
                [],
                'search-attribute-replay',
            );
            self::fail('A lifecycle row must not share an upsert sequence.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('durable_command_sequence_collision', $exception->reason);
            self::assertSame(1, $exception->sequence);
        }
    }

    /** @return array<string, array{array<string, mixed>, string, bool}> */
    public static function lifecycleRowsWithoutTheirOpener(): array
    {
        $rows = [
            'TimerCancelled' => ['event_type' => 'TimerCancelled', 'payload' => ['sequence' => 1, 'timer_id' => 'timer-1']],
            'ConditionWaitCancelled' => ['event_type' => 'ConditionWaitCancelled', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'wait-1']],
            'ConditionWaitTimedOut' => ['event_type' => 'ConditionWaitTimedOut', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'wait-1']],
            'condition timeout TimerScheduled' => ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_kind' => 'condition_timeout',
                'condition_wait_id' => 'wait-1', 'delay_seconds' => 5]],
            'condition timeout TimerFired' => ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_kind' => 'condition_timeout',
                'condition_wait_id' => 'wait-1']],
            'signal timeout TimerScheduled' => ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_kind' => 'signal_timeout',
                'signal_wait_id' => 'wait-1', 'delay_seconds' => 5]],
            'signal timeout TimerFired' => ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_kind' => 'signal_timeout',
                'signal_wait_id' => 'wait-1']],
            'SignalWaitTimedOut' => ['event_type' => 'SignalWaitTimedOut', 'payload' => ['sequence' => 1, 'signal_wait_id' => 'wait-1']],
            'SignalWaitCancelled' => ['event_type' => 'SignalWaitCancelled', 'payload' => ['sequence' => 1, 'signal_wait_id' => 'wait-1']],
            'ActivityFailed' => ['event_type' => 'ActivityFailed', 'payload' => ['sequence' => 1, 'activity_type' => 'golden.activity', 'message' => 'failed']],
            'ActivityCancelled' => ['event_type' => 'ActivityCancelled', 'payload' => ['sequence' => 1, 'activity_type' => 'golden.activity']],
            'ChildRunTerminated' => ['event_type' => 'ChildRunTerminated', 'payload' => ['sequence' => 1, 'workflow_type' => 'golden.child']],
            'ServiceCallCancelled' => ['event_type' => 'ServiceCallCancelled', 'payload' => ['sequence' => 1, 'service_call_id' => 'call-1']],
        ];
        $codec = new AvroPayloadCodec();
        foreach (['ActivityScheduled', 'ActivityStarted', 'ActivityCompleted', 'ActivityTimedOut', 'ActivityRetryScheduled',
            'ActivityHeartbeatRecorded', 'ActivityCancellationAcknowledged'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'activity_type' => 'golden.activity',
                'activity_execution_id' => 'activity-1', 'result' => $codec->envelope('done')]];
        }
        foreach (['TimerScheduled', 'TimerFired'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'timer_id' => 'timer-1', 'delay_seconds' => 5]];
        }
        foreach (['ChildWorkflowScheduled', 'ChildRunStarted', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'workflow_type' => 'golden.child',
                'child_workflow_run_id' => 'child-run-1', 'result' => $codec->envelope('done')]];
        }
        foreach (['ConditionWaitOpened', 'ConditionWaitSatisfied'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'condition_wait_id' => 'wait-1', 'condition_key' => 'ready']];
        }
        foreach (['SignalWaitOpened', 'SignalWaitReceived', 'SignalApplied'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'signal_name' => 'approve', 'signal_wait_id' => 'wait-1']];
        }
        foreach (['ServiceCallStarted', 'ServiceCallCompleted', 'ServiceCallFailed', 'ServiceCallTimedOut'] as $type) {
            $rows[$type] = ['event_type' => $type, 'payload' => ['sequence' => 1, 'service_call_id' => 'call-1']];
        }
        $rows['SideEffectRecorded'] = ['event_type' => 'SideEffectRecorded', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('x')]];
        $rows['VersionMarkerRecorded'] = ['event_type' => 'VersionMarkerRecorded', 'payload' => ['sequence' => 1, 'change_id' => 'change',
            'version' => 1, 'min_supported' => 1, 'max_supported' => 1]];
        $rows['WorkflowContinuedAsNew'] = ['event_type' => 'WorkflowContinuedAsNew', 'payload' => ['sequence' => 1]];
        $cases = [];
        foreach ($rows as $label => $row) {
            foreach (['search attributes' => 'search_attributes', 'memo' => 'memo'] as $upsertLabel => $upsert) {
                $cases["{$label} after {$upsertLabel}"] = [$row, $upsert, true];
                $cases["{$upsertLabel} after {$label}"] = [$row, $upsert, false];
            }
        }

        return $cases;
    }

    /**
     * An authored cancellation scope opening claims its sequence too. Scope
     * history needs canonical event ids, sequences and namespace.
     */
    #[DataProvider('upsertOrders')]
    public function testAScopeOpeningCannotShareAnUpsertsSequence(string $upsert, bool $upsertFirst): void
    {
        $codec = new AvroPayloadCodec();
        $upsertPayload = $upsert === 'memo'
            ? ['sequence' => 1, 'entries' => $codec->envelope(['stage' => 'greeting']), 'merged' => $codec->envelope(['stage' => 'greeting'])]
            : ['sequence' => 1, 'attributes' => ['Stage' => 'greeting'], 'merged' => ['Stage' => 'greeting']];
        $upsertEvent = ['event_type' => $upsert === 'memo' ? 'MemoUpserted' : 'SearchAttributesUpserted', 'payload' => $upsertPayload];
        $scope = ['event_type' => 'CancellationScopeOpened', 'payload' => ['schema' => 'durable-workflow.cancellation-scope/v1',
            'workflow_run_id' => 'run-1', 'sequence' => 1, 'scope_id' => 'scope-1', 'parent_scope_id' => 'root', 'shield_parent' => false]];
        $history = [];
        foreach ($upsertFirst ? [$upsertEvent, $scope] : [$scope, $upsertEvent] as $index => $event) {
            $history[] = ['id' => "event-{$index}", 'sequence' => $index + 1, 'namespace' => 'default', ...$event];
        }
        try {
            (new Replayer($codec))->replay(
                static function (WorkflowContext $context): string {
                    $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return 'done';
                },
                $history,
                [],
                'search-attribute-replay',
                ['run_id' => 'run-1'],
                allowCancellationScopeAuthoring: true,
            );
            self::fail('A scope opening must not share an upsert sequence.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('durable_command_sequence_collision', $exception->reason);
            self::assertSame(1, $exception->sequence);
        }
    }

    /** @return array<string, array{string, bool}> */
    public static function upsertOrders(): array
    {
        return [
            'scope after search attributes' => ['search_attributes', true],
            'search attributes after scope' => ['search_attributes', false],
            'scope after memo' => ['memo', true],
            'memo after scope' => ['memo', false],
        ];
    }

    /**
     * ActivityOutcomeRejected is a payload-free audit fact: it neither resolves
     * an activity nor consumes a command position, with or without a sequence.
     *
     * @param array<string, mixed> $payload
     */
    #[DataProvider('outcomeRejections')]
    public function testAnActivityOutcomeRejectionClaimsNoSequence(array $payload, bool $upsertFirst): void
    {
        $upsert = ['event_type' => 'SearchAttributesUpserted', 'payload' => ['sequence' => 1, 'attributes' => ['Stage' => 'greeting'],
            'merged' => ['Stage' => 'greeting']]];
        $rejection = ['event_type' => 'ActivityOutcomeRejected', 'payload' => $payload];
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context): string {
                $context->upsertSearchAttributes(['Stage' => 'greeting']);

                return 'done';
            },
            $upsertFirst ? [$upsert, $rejection] : [$rejection, $upsert],
            [],
            'search-attribute-replay',
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /** @return array<string, array{array<string, mixed>, bool}> */
    public static function outcomeRejections(): array
    {
        $canonical = ['activity_execution_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'reason' => 'stale_attempt'];

        return [
            'canonical, after the upsert' => [$canonical, true],
            'canonical, before the upsert' => [$canonical, false],
            'with a sequence, after the upsert' => [[...$canonical, 'sequence' => 1], true],
            'with a sequence, before the upsert' => [[...$canonical, 'sequence' => 1], false],
        ];
    }

    /**
     * A delivered cooperative cancellation consumes the interrupted call even
     * with no opener, and its sequence_span reserves the whole range,
     * inclusive. An upsert anywhere in it collides, in either order.
     */
    #[DataProvider('deliveryCollisions')]
    public function testACooperativeDeliveryClaimsItsWholeSpan(int $deliverySequence, int $span, int $upsertSequence, bool $upsertFirst): void
    {
        $snapshot = json_decode((string) file_get_contents(__DIR__.'/fixtures/cooperative-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
        $request = ['event_type' => 'CooperativeCancellationRequested', 'timestamp' => '2026-10-01T00:00:05Z', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_instance_id' => 'child-instance', 'workflow_command_id' => 'request-1',
            'cleanup_deadline_at' => '2026-10-01T00:00:30.123456Z', 'reason' => 'maintenance', 'cancellation' => $snapshot]];
        $delivery = ['event_type' => 'CooperativeCancellationDelivered', 'timestamp' => '2026-10-01T00:00:08Z', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'sequence' => $deliverySequence,
            'sequence_span' => $span, 'call_kind' => $span > 1 ? 'parallel' : 'timer', 'cancellation' => $snapshot]];
        $upsert = ['event_type' => 'SearchAttributesUpserted', 'timestamp' => '2026-10-01T00:00:06Z', 'payload' => [
            'sequence' => $upsertSequence, 'attributes' => ['Stage' => 'greeting'], 'merged' => ['Stage' => 'greeting']]];
        $history = $upsertFirst ? [$request, $upsert, $delivery] : [$request, $delivery, $upsert];

        $exception = $this->replayRefusal($history, ['run_id' => 'run-1']);

        self::assertSame('durable_command_sequence_collision', $exception->reason);
        self::assertSame($upsertSequence, $exception->sequence);
    }

    /** @return array<string, array{int, int, int, bool}> */
    public static function deliveryCollisions(): array
    {
        return [
            'upsert, then a single delivery' => [1, 1, 1, true],
            'single delivery, then an upsert' => [1, 1, 1, false],
            'upsert, then a span over its interior' => [1, 3, 2, true],
            'span, then an upsert on its interior' => [1, 3, 2, false],
            'upsert, then a span ending on it' => [1, 3, 3, true],
            'span, then an upsert on its last member' => [1, 3, 3, false],
        ];
    }

    public function testAnUpsertJustPastADeliverySpanIsNotInIt(): void
    {
        $snapshot = json_decode((string) file_get_contents(__DIR__.'/fixtures/cooperative-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
        $history = [
            ['event_type' => 'CooperativeCancellationRequested', 'timestamp' => '2026-10-01T00:00:05Z', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_instance_id' => 'child-instance', 'workflow_command_id' => 'request-1',
                'cleanup_deadline_at' => '2026-10-01T00:00:30.123456Z', 'reason' => 'maintenance', 'cancellation' => $snapshot]],
            ['event_type' => 'CooperativeCancellationDelivered', 'timestamp' => '2026-10-01T00:00:08Z', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'sequence' => 1, 'sequence_span' => 3,
                'call_kind' => 'parallel', 'cancellation' => $snapshot]],
            ['event_type' => 'SearchAttributesUpserted', 'timestamp' => '2026-10-01T00:00:09Z', 'payload' => [
                'sequence' => 4, 'attributes' => ['Stage' => 'greeting'], 'merged' => ['Stage' => 'greeting']]],
        ];

        // Step 4 is past the inclusive range 1..3, so whatever replay decides
        // about the cancelled call, it is not a sequence collision.
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static function (WorkflowContext $context): string {
                    $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return 'done';
                },
                $history,
                [],
                'search-attribute-replay',
                ['run_id' => 'run-1'],
            );
        } catch (NonDeterministicWorkflow $exception) {
            self::assertNotSame('durable_command_sequence_collision', $exception->reason);
        }
        self::addToAssertionCount(1);
    }

    /** @param callable(array<string, mixed>): array<string, mixed> $mutate */
    #[DataProvider('reusedDeliveryCollisions')]
    public function testACooperativeDeliveryCannotShareAReusedUpsertsStep(callable $mutate): void
    {
        $path = RedriveFixtureRegistry::path('redrive-successor-reused-upserts-cancellation-clock');
        $fixture = $mutate(json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR));
        $fixture['expected'] = ['workflow_tasks' => [['command_sequence' => [['type' => 'replay_error', 'reason' => 'durable_command_sequence_collision']]]]];

        $tasks = ReplayRegressionFixture::execute($fixture);

        self::assertSame('durable_command_sequence_collision', $tasks[0]['command_sequence'][0]['reason']);
    }

    /** @return array<string, array{callable(array<string, mixed>): array<string, mixed>}> */
    public static function reusedDeliveryCollisions(): array
    {
        // History rows: 0 start, 1 memo (step 1), 2 search attributes (step 2),
        // 3 timer (step 3), 4 request, 5 delivery. The timer opener is removed.
        $withoutOpener = static function (array $fixture, callable $change): array {
            array_splice($fixture['history'], 3, 1);
            $fixture['history'] = $change($fixture['history']);
            foreach ($fixture['history'] as $index => $event) {
                $fixture['history'][$index]['sequence'] = $index + 1;
            }

            return $fixture;
        };

        return [
            'delivery after the reused upsert, on its step' => [static fn (array $fixture): array => $withoutOpener($fixture, static function (array $history): array {
                $history[4]['payload']['sequence'] = 2;

                return $history;
            })],
            'delivery span over the reused upsert' => [static fn (array $fixture): array => $withoutOpener($fixture, static function (array $history): array {
                $history[4]['payload']['sequence'] = 1;
                $history[4]['payload']['sequence_span'] = 3;
                $history[4]['payload']['call_kind'] = 'parallel';

                return $history;
            })],
            'delivery before the reused upsert, on its step' => [static fn (array $fixture): array => $withoutOpener($fixture, static function (array $history): array {
                $history[4]['payload']['sequence'] = 2;
                [$request, $delivery] = [$history[3], $history[4]];

                return [$history[0], $request, $delivery, $history[1], $history[2]];
            })],
        ];
    }

    /**
     * A committed scope delivery claims its position the same way. The
     * canonical fixture delivers to a timer at step 3 that was never
     * scheduled. Its span is 1; the scope history validator only accepts a
     * span that matches a recorded call, so interior-span coverage is proved
     * with the cooperative delivery above, which shares this claim path.
     */
    #[DataProvider('scopeDeliveryCollisions')]
    public function testAScopeDeliveryCannotShareAnUpsertsSequence(int $upsertSequence, bool $upsertFirst): void
    {
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR)['unshielded'];
        $history = $fixture['history'];
        self::assertSame('CancellationScopeDelivered', $history[6]['event_type']);
        self::assertSame(3, $history[6]['payload']['sequence']);
        $upsert = ['id' => 'scope-upsert', 'namespace' => $history[0]['namespace'], 'event_type' => 'SearchAttributesUpserted',
            'timestamp' => $history[6]['timestamp'], 'payload' => ['sequence' => $upsertSequence, 'attributes' => ['Stage' => 'greeting'],
                'merged' => ['Stage' => 'greeting']]];
        array_splice($history, $upsertFirst ? 6 : 7, 0, [$upsert]);
        foreach ($history as $index => $event) {
            $history[$index]['sequence'] = $index + 1;
        }

        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static fn (WorkflowContext $context): string => 'done',
                $history,
                [],
                'php-workers',
                $fixture['task'],
                allowCancellationScopeAuthoring: true,
                replayCommittedCancellationScopes: true,
            );
            self::fail('A scope delivery must not share an upsert sequence.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('durable_command_sequence_collision', $exception->reason);
            self::assertSame($upsertSequence, $exception->sequence);
        }
    }

    /** @return array<string, array{int, bool}> */
    public static function scopeDeliveryCollisions(): array
    {
        return [
            'upsert, then a scope delivery' => [3, true],
            'scope delivery, then an upsert' => [3, false],
        ];
    }

    /** @param list<array<string, mixed>> $history @param array<string, mixed> $task */
    private function replayRefusal(array $history, array $task): NonDeterministicWorkflow
    {
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static function (WorkflowContext $context): string {
                    $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return 'done';
                },
                $history,
                [],
                'search-attribute-replay',
                $task,
            );
        } catch (NonDeterministicWorkflow $exception) {
            return $exception;
        }
        self::fail('Replay must refuse this history.');
    }

    /** @param array<string, mixed> $row */
    #[DataProvider('pointerRows')]
    public function testARowThatOnlyPointsAtAStepClaimsNoSequence(array $row): void
    {
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context): string {
                $context->upsertSearchAttributes(['Stage' => 'greeting']);

                return 'done';
            },
            [['event_type' => 'SearchAttributesUpserted', 'payload' => ['sequence' => 1, 'attributes' => ['Stage' => 'greeting'],
                'merged' => ['Stage' => 'greeting']]], $row],
            [],
            'search-attribute-replay',
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /** @return array<string, array{array<string, mixed>}> */
    public static function pointerRows(): array
    {
        return [
            'SignalReceived' => [['event_type' => 'SignalReceived', 'payload' => ['sequence' => 1, 'signal_name' => 'approve',
                'arguments' => (new AvroPayloadCodec())->envelope([])]]],
            'SignalRejected' => [['event_type' => 'SignalRejected', 'payload' => ['sequence' => 1, 'signal_name' => 'approve']]],
            'UpdateAccepted' => [['event_type' => 'UpdateAccepted', 'payload' => ['sequence' => 1, 'update_name' => 'approve']]],
            'UpdateApplied' => [['event_type' => 'UpdateApplied', 'payload' => ['sequence' => 1, 'update_name' => 'approve']]],
            'MessageCursorAdvanced' => [['event_type' => 'MessageCursorAdvanced', 'payload' => ['sequence' => 1, 'stream' => 'inbox']]],
        ];
    }

    #[DataProvider('unsequencedUpserts')]
    public function testRecordedUpsertMustNameItsOwnSequence(mixed $sequence, bool $present, string $reason): void
    {
        $payload = ['attributes' => ['Stage' => 'greeting'], 'merged' => ['Stage' => 'greeting']];
        if ($present) {
            $payload['sequence'] = $sequence;
        }
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static function (WorkflowContext $context): string {
                    $context->upsertSearchAttributes(['Stage' => 'greeting']);

                    return 'done';
                },
                [['event_type' => 'SearchAttributesUpserted', 'payload' => $payload]],
                [],
                'search-attribute-replay',
            );
            self::fail('Replay must not synthesize a search-attribute sequence.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame($reason, $exception->reason);
        }
    }

    /** @return array<string, array{mixed, bool, string}> */
    public static function unsequencedUpserts(): array
    {
        return [
            'absent' => [null, false, 'durable_command_sequence_missing'],
            'null' => [null, true, 'durable_command_sequence_missing'],
            'zero' => [0, true, 'durable_command_sequence_invalid'],
            'negative' => [-1, true, 'durable_command_sequence_invalid'],
            'numeric string' => ['1', true, 'durable_command_sequence_invalid'],
            'float' => [1.0, true, 'durable_command_sequence_invalid'],
        ];
    }

    public function testAnUpsertBesideAnotherCommandsLifecycleStillReplays(): void
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay(
            static function (WorkflowContext $context): mixed {
                $context->upsertSearchAttributes(['Stage' => 'greeting']);

                return $context->activity('golden.activity');
            },
            [
                ['event_type' => 'SearchAttributesUpserted', 'payload' => ['sequence' => 1, 'attributes' => ['Stage' => 'greeting'], 'merged' => ['Stage' => 'greeting']]],
                ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'golden.activity', 'activity_execution_id' => 'a-1']],
                ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 2, 'activity_type' => 'golden.activity', 'activity_execution_id' => 'a-1', 'result' => $codec->envelope('done')]],
            ],
            [],
            'search-attribute-replay',
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    public function testRedriveReusedUpsertWithoutAttributesFailsClosed(): void
    {
        $path = RedriveFixtureRegistry::path('redrive-reused-search-attributes-missing');
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, ['MemoUpserted', 'SearchAttributesUpserted']));
        self::assertArrayNotHasKey('attributes', $fixture['history'][2]['payload']);

        $tasks = ReplayRegressionFixture::executeFile($path);

        self::assertSame('replay_error', $tasks[0]['command_sequence'][0]['type']);
        self::assertSame('search_attribute_update_missing', $tasks[0]['command_sequence'][0]['reason']);
        self::assertSame(2, $tasks[0]['command_sequence'][0]['sequence']);
    }

    public function testRecordedUpsertWithoutAttributesFailsClosed(): void
    {
        // One reason across SDKs: sdk-rust reports search_attribute_update_missing.
        $exception = $this->refusal(['Stage' => 'greeting'], null);

        self::assertSame('search_attribute_update_missing', $exception->reason);
    }

    public function testFloatIdentityDoesNotDependOnSerializePrecision(): void
    {
        $previous = ini_get('serialize_precision');
        ini_set('serialize_precision', '3');
        try {
            $exception = $this->refusal(['Ratio' => 1.2344], ['Ratio' => 1.2345]);
        } finally {
            ini_set('serialize_precision', (string) $previous);
        }

        self::assertSame('search_attribute_value_mismatch', $exception->reason);
        self::assertStringNotContainsString('1.23', $exception->getMessage());
    }

    public function testSignedZeroMatchesZero(): void
    {
        $result = $this->replay(['Ratio' => -0.0], ['Ratio' => 0.0]);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    public function testIntAndFloatAreDifferentValues(): void
    {
        self::assertSame('search_attribute_value_mismatch', $this->refusal(['Count' => 2], ['Count' => 2.0])->reason);
        self::assertSame('search_attribute_value_mismatch', $this->refusal(['Count' => 2.0], ['Count' => 2])->reason);
    }

    public function testValuesAtCoresLengthBoundsMatch(): void
    {
        $text = str_repeat('é', 2048);
        $keyword = str_repeat('é', 255);
        $result = $this->replay(['Text' => " {$text} ", 'Tags' => [" {$keyword} "]], ['Text' => $text, 'Tags' => [$keyword]]);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * @param array<string, mixed> $authored
     * @param array<string, mixed> $recorded
     */
    #[DataProvider('overLengthValues')]
    public function testValuesOverCoresLengthBoundsRefuseReplay(array $authored, array $recorded, string $origin): void
    {
        $exception = $this->refusal($authored, $recorded);

        self::assertSame('search_attributes_unportable', $exception->reason);
        self::assertStringContainsString("The {$origin} search attributes", $exception->getMessage());
        self::assertStringNotContainsString('é', $exception->getMessage());
    }

    /** @return array<string, array{array<string, mixed>, array<string, mixed>, string}> */
    public static function overLengthValues(): array
    {
        $text = str_repeat('é', 2049);
        $keyword = str_repeat('é', 256);

        return [
            'recorded string over 2048' => [['Text' => $text], ['Text' => $text], 'recorded'],
            'authored string over 2048' => [['Text' => $text], ['Text' => 'x'], 'authored'],
            'recorded list item over 255' => [['Tags' => [$keyword]], ['Tags' => [$keyword]], 'recorded'],
            'authored list item over 255' => [['Tags' => [$keyword]], ['Tags' => ['x']], 'authored'],
        ];
    }

    /**
     * Value-only: the recorded effective type is not compared. Here Server
     * recorded a namespace-registered "string" type for a value it would infer
     * as "keyword", and replay still matches on the value. Typed reuse needs a
     * command that declares types, which this SDK does not have.
     */
    public function testRecordedAttributeTypesAreNotCompared(): void
    {
        $result = $this->replay(['Stage' => 'greeting'], ['Stage' => 'greeting'], ['Stage' => 'string']);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * @param array<array-key, mixed> $attributes
     * @param array<array-key, mixed>|null $recorded
     */
    private function refusal(array $attributes, ?array $recorded): NonDeterministicWorkflow
    {
        try {
            $this->replay($attributes, $recorded);
        } catch (NonDeterministicWorkflow $exception) {
            return $exception;
        }
        self::fail('Replay must refuse this search-attribute upsert.');
    }

    /**
     * @param array<array-key, mixed> $attributes
     * @param array<array-key, mixed>|null $recorded
     * @param array<string, string>|null $types
     */
    private function replay(array $attributes, ?array $recorded, ?array $types = null): ReplayResult
    {
        $payload = ['sequence' => 1, 'merged' => $recorded ?? []];
        if ($recorded !== null) {
            $payload['attributes'] = $recorded;
        }
        if ($types !== null) {
            $payload['attribute_types'] = $types;
        }

        return (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use ($attributes): string {
                $context->upsertSearchAttributes($attributes);

                return 'done';
            },
            [['event_type' => 'SearchAttributesUpserted', 'payload' => $payload]],
            [],
            'search-attribute-replay',
        );
    }
}
