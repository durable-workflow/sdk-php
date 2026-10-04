<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeAuthoringTest extends TestCase
{
    public function test_new_scope_body_waits_for_canonical_opening(): void
    {
        $entered = false;
        $result = $this->replay(static function (WorkflowContext $context) use (&$entered): void {
            $context->cancellationScope(static function () use (&$entered): void { $entered = true; });
        }, []);
        self::assertFalse($entered);
        self::assertSame([], $result->commands);
        self::assertSame(1, $result->cancellationScopeOpening?->sequence);
        self::assertSame('root', $result->cancellationScopeOpening?->parentScopeId);
        self::assertFalse($result->cancellationScopeOpening?->shieldParent);
    }

    public function test_cold_replay_enters_original_scope_and_preserves_deferred_membership_after_exit(): void
    {
        $codec = new AvroPayloadCodec();
        $handler = static function (WorkflowContext $context): array {
            $leaf = $context->cancellationScope(static fn () => $context->deferActivity('scoped'));
            return $context->all([$leaf, $context->deferActivity('root')]);
        };
        $result = $this->replay($handler, [self::opening(1)]);
        self::assertCount(2, $result->commands);
        self::assertSame('scope-one', $result->commands[0]['cancellation_scope_id']);
        self::assertArrayNotHasKey('cancellation_scope_id', $result->commands[1]);
        self::assertSame('parallel-activities:2:2', $result->commands[0]['parallel_group_id']);
        $history = [self::opening(1)];
        foreach ($result->commands as $offset => $command) {
            $history[] = self::event(3 + $offset, 'ActivityCompleted', [
                ...$command, 'sequence' => 2 + $offset, 'result' => $codec->envelope($offset ? 'root-result' : 'scoped-result'),
            ]);
        }
        $cold = $this->replay($handler, $history);
        self::assertSame(['scoped-result', 'root-result'], $codec->decodeEnvelope($cold->commands[0]['result']));
    }

    public function test_prefix_must_commit_before_opening_and_side_effect_is_not_repeated(): void
    {
        $calls = 0;
        $entered = 0;
        $handler = static function (WorkflowContext $context) use (&$calls, &$entered): string {
            $context->sideEffect(static function () use (&$calls): string { ++$calls; return 'original'; });
            return $context->cancellationScope(static function () use (&$entered): string { ++$entered; return 'body'; });
        };
        $result = $this->replay($handler, []);
        self::assertSame(1, $calls);
        self::assertSame(0, $entered);
        self::assertSame('record_side_effect', $result->commands[0]['type']);
        self::assertSame(2, $result->cancellationScopeOpening?->sequence);
        $prefix = self::event(2, 'SideEffectRecorded', ['sequence' => 1, 'result' => $result->commands[0]['result']]);
        $pending = $this->replay($handler, [$prefix]);
        self::assertSame([], $pending->commands);
        self::assertSame(2, $pending->cancellationScopeOpening?->sequence);
        $cold = $this->replay($handler, [$prefix, self::opening(2)]);
        self::assertSame(1, $calls);
        self::assertSame(1, $entered);
        self::assertSame('body', (new AvroPayloadCodec())->decodeEnvelope($cold->commands[0]['result']));
    }

    public function test_nested_shield_and_exception_restore_the_canonical_parent(): void
    {
        $handler = static function (WorkflowContext $context): mixed {
            $context->cancellationScope(static function () use ($context): void {
                try {
                    $context->cancellationScope(static function (): never { throw new \RuntimeException('application'); }, shieldParent: true);
                } catch (\RuntimeException) {
                }
                $context->cancellationScope(static fn () => $context->deferTimer(10));
            });
            return $context->cancellationScope(static fn (): string => 'root-sibling');
        };
        $history = [self::opening(1), self::opening(2, 'scope-two', 'scope-one', true)];
        $pending = $this->replay($handler, $history);
        self::assertSame(3, $pending->cancellationScopeOpening?->sequence);
        self::assertSame('scope-one', $pending->cancellationScopeOpening?->parentScopeId);
        $history[] = self::opening(3, 'scope-three', 'scope-one');
        $sibling = $this->replay($handler, $history);
        self::assertSame(4, $sibling->cancellationScopeOpening?->sequence);
        self::assertSame('root', $sibling->cancellationScopeOpening?->parentScopeId);
        $history[] = self::opening(4, 'scope-four');
        self::assertSame('root-sibling', (new AvroPayloadCodec())->decodeEnvelope($this->replay($handler, $history)->commands[0]['result']));
    }

    #[DataProvider('changedOpenings')]
    public function test_changed_authoring_fails_before_entering_changed_body(string $parent, bool $shield): void
    {
        $entered = false;
        $history = [self::opening(1), self::opening(2, 'scope-two', $parent, $shield)];
        try {
            $this->replay(static fn (WorkflowContext $context) => $context->cancellationScope(static fn () =>
                $context->cancellationScope(static function () use (&$entered): void { $entered = true; }, shieldParent: true)), $history);
            self::fail('Changed scope must not enter its body.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('cancellation_scope_opening_changed', $error->reason);
            self::assertFalse($entered);
        }
    }

    public static function changedOpenings(): array
    {
        return ['parent' => ['root', true], 'shield' => ['scope-one', false]];
    }

    #[DataProvider('invalidOpenings')]
    public function test_malformed_canonical_tree_is_rejected_before_workflow_entry(array $changes, array $eventChanges = []): void
    {
        $event = self::opening(1);
        $event['payload'] = array_replace($event['payload'], $changes);
        $event = array_replace($event, $eventChanges);
        $entered = false;
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, [$event]);
            self::fail('Invalid tree must not enter workflow code.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('invalid_cancellation_scope_history', $error->reason);
            self::assertFalse($entered);
        }
    }

    public static function invalidOpenings(): array
    {
        return [
            'foreign run' => [['workflow_run_id' => 'foreign']], 'root identity' => [['scope_id' => 'root']],
            'unknown parent' => [['parent_scope_id' => 'future']], 'self parent' => [['parent_scope_id' => 'scope-one']],
            'zero sequence' => [['sequence' => 0]], 'string sequence' => [['sequence' => '1']],
            'null shield' => [['shield_parent' => null]], 'schema' => [['schema' => 'other']],
            'empty scope' => [['scope_id' => '']], 'invalid utf8' => [['scope_id' => "\xff"]],
            'missing event identity' => [[], ['id' => null]], 'missing namespace' => [[], ['namespace' => null]],
        ];
    }

    #[DataProvider('invalidMemberships')]
    public function test_invalid_operation_membership_is_refused_before_workflow_entry(array $payload): void
    {
        $entered = false;
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, [
                self::opening(1), self::event(3, 'ActivityScheduled', $payload),
            ]);
            self::fail('Invalid operation membership must not enter workflow code.');
        } catch (NonDeterministicWorkflow) {
            self::assertFalse($entered);
        }
    }

    public static function invalidMemberships(): array
    {
        return [
            'unknown' => [['sequence' => 2, 'cancellation_scope_id' => 'unknown']],
            'same boundary' => [['sequence' => 1, 'cancellation_scope_id' => 'scope-one']],
            'missing boundary' => [['cancellation_scope_id' => 'scope-one']],
            'contradictory snapshot' => [['sequence' => 2, 'cancellation_scope_id' => 'scope-one', 'activity' => ['cancellation_scope_id' => 'root']]],
            'null' => [['sequence' => 2, 'cancellation_scope_id' => null]],
        ];
    }

    public function test_resolution_omission_inherits_original_membership_but_explicit_reparenting_is_refused(): void
    {
        $codec = new AvroPayloadCodec();
        $handler = static fn (WorkflowContext $context) => $context->cancellationScope(static fn () => $context->activity('scoped'));
        $history = [self::opening(1), self::event(3, 'ActivityScheduled', [
            'sequence' => 2, 'activity_type' => 'scoped', 'cancellation_scope_id' => 'scope-one',
        ]), self::event(4, 'ActivityCompleted', ['sequence' => 2, 'result' => $codec->envelope('same')])];
        self::assertSame('same', $codec->decodeEnvelope($this->replay($handler, $history)->commands[0]['result']));
        $history[2]['payload']['cancellation_scope_id'] = 'root';
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($handler, $history);
    }

    public function test_opening_and_ordinary_command_cannot_share_an_authored_sequence(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay(static fn (): string => 'must not enter', [self::opening(1), self::event(3, 'SideEffectRecorded', [
            'sequence' => 1, 'result' => (new AvroPayloadCodec())->encode('collision'),
        ])]);
    }

    #[DataProvider('unsupportedCancellationHistories')]
    public function test_authoring_candidate_still_refuses_unqualified_delivery_before_workflow_entry(?string $eventKind, array $task): void
    {
        $history = [self::opening(1)];
        if ($eventKind !== null) {
            $history[] = self::event(3, $eventKind, []);
        }
        $entered = false;
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; },
                $history, [], 'php-workers', ['run_id' => 'run-one', ...$task], allowCancellationScopeAuthoring: true);
            self::fail('Authored scopes do not qualify cancellation delivery.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('cancellation_scope_execution_not_supported', $error->getMessage());
            self::assertFalse($entered);
        }
    }

    public static function unsupportedCancellationHistories(): array
    {
        return [
            'scope request' => ['CancellationScopeRequested', []], 'scope delivery' => ['CancellationScopeDelivered', []],
            'scope conflict' => ['CancellationScopeRequestConflicted', []],
            'run request' => ['CooperativeCancellationRequested', []], 'run delivery' => ['CooperativeCancellationDelivered', []],
            'task terminal observation' => [null, ['cancel_requested' => true]],
            'task cooperative observation' => [null, ['cancellation_request' => []]],
        ];
    }

    #[DataProvider('booleanValues')]
    public function test_local_callback_in_a_run_with_scopes_is_refused_before_execution(bool $insideScope): void
    {
        $executed = false;
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context) use ($insideScope): mixed {
                $context->cancellationScope(static fn () => $insideScope ? $context->localActivity('local') : null);
                return $context->localActivity('local');
            }, [self::opening(1)], [], 'php-workers',
                ['run_id' => 'run-one'], localActivityExecutor: static function () use (&$executed): array { $executed = true; return []; },
                allowCancellationScopeAuthoring: true);
            self::fail('Scoped local work needs selective supervision.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('cancellation_scope_local_activity_not_supported', $error->getMessage());
            self::assertFalse($executed);
        }
    }

    public static function booleanValues(): array
    {
        return [[true], [false]];
    }

    public function test_scope_cannot_open_inside_atomic_operation_capture(): void
    {
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('cancellation_scope_opening_inside_group_not_supported');
        $this->replay(static fn (WorkflowContext $context) => $context->all([
            static fn () => $context->cancellationScope(static fn () => $context->activity('unsafe')),
        ]), []);
    }

    #[DataProvider('scopedOperationKinds')]
    public function test_each_supported_authored_operation_carries_its_immediate_scope(string $kind): void
    {
        $result = $this->replay(static fn (WorkflowContext $context) => $context->cancellationScope(static fn () => match ($kind) {
            'activity' => $context->activity('remote'), 'timer' => $context->sleep(10),
            'child' => $context->childWorkflow('child'), 'condition' => $context->waitCondition(static fn (): bool => false, 'waiting', 10),
        }), [self::opening(1)]);
        self::assertCount(1, $result->commands);
        self::assertSame('scope-one', $result->commands[0]['cancellation_scope_id']);
    }

    public static function scopedOperationKinds(): array
    {
        return [['activity'], ['timer'], ['child'], ['condition']];
    }

    public function test_mixed_group_cannot_swap_recorded_scope_memberships(): void
    {
        $first = $this->replay(static function (WorkflowContext $context): array {
            $scoped = $context->cancellationScope(static fn () => $context->deferActivity('same'));
            return $context->all([$scoped, $context->deferActivity('same')]);
        }, [self::opening(1)]);
        $history = [self::opening(1)];
        foreach ($first->commands as $offset => $command) {
            $history[] = self::event(3 + $offset, 'ActivityScheduled', [...$command, 'sequence' => 2 + $offset]);
        }
        try {
            $this->replay(static function (WorkflowContext $context): array {
                $scoped = $context->cancellationScope(static fn () => $context->deferActivity('same'));
                return $context->all([$context->deferActivity('same'), $scoped]);
            }, $history);
            self::fail('Equal activity types cannot hide changed scope membership.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('cancellation_scope_membership_changed', $error->reason);
        }
    }

    #[DataProvider('invalidOrderedTrees')]
    public function test_a_future_or_repeated_opening_cannot_authorize_earlier_work(array $history): void
    {
        $entered = false;
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, $history);
            self::fail('Invalid history must be rejected before workflow entry.');
        } catch (NonDeterministicWorkflow) {
            self::assertFalse($entered);
        }
    }

    public static function invalidOrderedTrees(): array
    {
        $duplicateId = self::opening(2, 'other');
        $duplicateId['id'] = 'event-2';
        return [
            'repeated scope' => [[self::opening(1), self::opening(2)]],
            'repeated event identity' => [[self::opening(1), $duplicateId]],
            'reversed history' => [[self::opening(2, 'other'), self::opening(1)]],
            'cross namespace' => [[self::event(1, 'WorkflowStarted', []), array_replace(self::opening(1), ['namespace' => 'foreign'])]],
            'future parent' => [[self::opening(1, 'child', 'parent'), self::opening(2, 'parent')]],
            'future operation owner' => [[self::event(1, 'ActivityScheduled', ['sequence' => 2, 'cancellation_scope_id' => 'scope-one']), self::opening(1)]],
            'repeated authored sequence' => [[self::opening(1), array_replace(self::opening(1, 'other'), ['id' => 'other-event', 'sequence' => 3])]],
        ];
    }

    /** @param callable(WorkflowContext): mixed $handler
     * @param list<array<string, mixed>> $history
     */
    private function replay(callable $handler, array $history): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($handler, $history, [], 'php-workers',
            ['run_id' => 'run-one'], allowCancellationScopeAuthoring: true);
    }

    /** @return array<string, mixed> */
    private static function opening(int $sequence, string $scopeId = 'scope-one', string $parent = 'root', bool $shield = false): array
    {
        return self::event($sequence + 1, 'CancellationScopeOpened', [
            'schema' => 'durable-workflow.cancellation-scope/v1', 'workflow_run_id' => 'run-one',
            'sequence' => $sequence, 'scope_id' => $scopeId, 'parent_scope_id' => $parent, 'shield_parent' => $shield,
        ]);
    }

    /** @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function event(int $sequence, string $type, array $payload): array
    {
        return ['id' => 'event-'.$sequence, 'sequence' => $sequence, 'namespace' => 'tenant', 'event_type' => $type, 'payload' => $payload];
    }
}
