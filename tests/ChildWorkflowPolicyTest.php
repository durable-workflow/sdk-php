<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationPolicy;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ChildWorkflowPolicyTest extends TestCase
{
    #[DataProvider('policies')]
    public function testTypedAndStringPoliciesProduceTheSamePortableCommand(ParentClosePolicy $parent, CancellationPolicy $operation): void
    {
        $codec = new AvroPayloadCodec();
        $typed = (new Replayer($codec))->replay(
            self::workflow('sequential', ['parent_close_policy' => $parent, 'cancellation_policy' => $operation]),
            [], [], 'php-workers',
        )->commands[0];
        $strings = WorkflowCommand::childWorkflow('child', ['argument'], [
            'parent_close_policy' => $parent->value,
            'cancellation_policy' => $operation->value,
        ])->toWire($codec, 'php-workers');

        self::assertSame($strings, $typed);
        self::assertSame($parent->value, $typed['parent_close_policy']);
        self::assertSame($operation->value, $typed['cancellation_policy']);
        self::assertSame(['argument'], $codec->decodeEnvelope($typed['arguments']));

        $history = self::scheduledHistory([$typed]);
        $history[] = ['event_type' => 'ChildRunCompleted', 'payload' => [
            'sequence' => 1, 'child_workflow_type' => 'child', 'result' => $codec->envelope('recorded-result'),
        ]];
        $replayed = (new Replayer($codec))->replay(
            self::workflow('sequential', ['parent_close_policy' => $parent, 'cancellation_policy' => $operation]),
            $history, [], 'php-workers',
        );
        self::assertSame('recorded-result', $codec->decodeEnvelope($replayed->commands[0]['result']));
    }

    public static function policies(): iterable
    {
        foreach (ParentClosePolicy::cases() as $parent) {
            foreach (CancellationPolicy::cases() as $operation) {
                yield $parent->value.'/'.$operation->value => [$parent, $operation];
            }
        }
    }

    #[DataProvider('invalidPolicies')]
    public function testInvalidPoliciesAreRejectedBeforeScheduling(string $field, mixed $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        WorkflowCommand::childWorkflow('child', [], [$field => $value]);
    }

    public static function invalidPolicies(): iterable
    {
        foreach (['parent_close_policy', 'cancellation_policy'] as $field) {
            foreach (['unknown', '', true, 1, [], ['type' => 'abandon']] as $value) {
                yield [$field, $value];
            }
        }
        yield ['parent_close_policy', CancellationPolicy::Abandon];
        yield ['cancellation_policy', ParentClosePolicy::Abandon];
    }

    public function testOmittedAndNullOptionsKeepTheHistoricalWireAndReplayDefaults(): void
    {
        $codec = new AvroPayloadCodec();
        $wire = WorkflowCommand::childWorkflow('child', ['argument'], [
            'parent_close_policy' => null, 'cancellation_policy' => null,
        ])->toWire($codec, 'php-workers');
        self::assertArrayNotHasKey('parent_close_policy', $wire);
        self::assertArrayNotHasKey('cancellation_policy', $wire);
        $history = self::scheduledHistory([$wire]);
        $replayed = (new Replayer($codec))->replay(self::workflow('sequential', [
            'parent_close_policy' => ParentClosePolicy::Abandon,
            'cancellation_policy' => CancellationPolicy::Abandon,
        ]), $history, [], 'php-workers');
        self::assertSame([], $replayed->commands);
    }

    #[DataProvider('changedPolicies')]
    public function testHistoricalDefaultsCannotBeChangedToCooperativePolicies(string $mode, string $field): void
    {
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay(self::workflow($mode, []), [], [], 'php-workers')->commands;
        try {
            (new Replayer($codec))->replay(self::workflow($mode, [$field => self::cooperativeOptions()[$field]]), self::scheduledHistory($commands), [], 'php-workers');
            self::fail('Old histories must retain their original defaults.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('child_workflow_policy_changed', $error->reason);
            self::assertSame('abandon', $error->expected);
        }
    }

    #[DataProvider('changedPolicies')]
    public function testColdReplayRejectsChangedPoliciesInEveryAuthoringForm(string $mode, string $field): void
    {
        $codec = new AvroPayloadCodec();
        $options = self::cooperativeOptions();
        $commands = (new Replayer($codec))->replay(self::workflow($mode, $options), [], [], 'php-workers')->commands;
        $options[$field] = 'abandon';

        try {
            (new Replayer($codec))->replay(self::workflow($mode, $options), self::scheduledHistory($commands), [], 'php-workers');
            self::fail('A restarted workflow must retain its recorded child policy.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('child_workflow_policy_changed', $error->reason);
            self::assertSame(1, $error->sequence);
            self::assertSame('abandon', $error->actual);
        }
    }

    public static function changedPolicies(): iterable
    {
        foreach (['sequential', 'deferred', 'parallel', 'selection'] as $mode) {
            foreach (['parent_close_policy', 'cancellation_policy'] as $field) {
                yield $mode.'/'.$field => [$mode, $field];
            }
        }
    }

    #[DataProvider('changedPolicies')]
    public function testCancellationDeliveryAlsoRejectsChangedChildPolicies(string $mode, string $field): void
    {
        $codec = new AvroPayloadCodec();
        $options = self::cooperativeOptions();
        $commands = (new Replayer($codec))->replay(self::workflow($mode, $options), [], [], 'php-workers')->commands;
        $history = self::scheduledHistory($commands);
        $history[] = ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-10-01T00:00:00Z', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'cleanup_deadline_at' => '2026-10-01T00:00:30Z',
        ]];
        $history[] = ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1',
            'sequence' => 1, 'call_kind' => $mode === 'sequential' ? 'child' : 'parallel', 'sequence_span' => count($commands),
        ]];
        $handler = static function (WorkflowContext $context) use ($mode, $options): string {
            try {
                (self::workflow($mode, $options))($context);
            } catch (WorkflowCancelled $cancelled) {
                return $cancelled->requestId ?? 'missing';
            }
            return 'not-cancelled';
        };
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', ['run_id' => 'run-1']);
        self::assertSame('request-1', $codec->decodeEnvelope($result->commands[0]['result']));

        $options[$field] = 'abandon';
        try {
            (new Replayer($codec))->replay(self::workflow($mode, $options), $history, [], 'php-workers', ['run_id' => 'run-1']);
            self::fail('Cancellation must not bypass recorded policy comparison.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('child_workflow_policy_changed', $error->reason);
        }
    }

    public function testAbsentPoliciesInLaterStartAndTerminalEventsKeepTheScheduledSnapshot(): void
    {
        $codec = new AvroPayloadCodec();
        $handler = self::workflow('sequential', self::cooperativeOptions());
        $commands = (new Replayer($codec))->replay($handler, [], [], 'php-workers')->commands;
        $history = self::scheduledHistory($commands);
        $history[] = ['event_type' => 'ChildRunStarted', 'payload' => ['sequence' => 1, 'child_workflow_type' => 'child']];
        $history[] = ['event_type' => 'ChildRunCompleted', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('done')]];
        self::assertSame('done', $codec->decodeEnvelope((new Replayer($codec))->replay($handler, $history, [], 'php-workers')->commands[0]['result']));
    }

    public function testSelectionHandleKeepsTheChildPolicyAfterTheWinnerResolves(): void
    {
        $codec = new AvroPayloadCodec();
        $options = self::cooperativeOptions();
        $handler = static function (WorkflowContext $context) use ($options): mixed {
            $selected = (self::workflow('selection', $options))($context);
            return $selected->handles['second']->await();
        };
        $commands = (new Replayer($codec))->replay($handler, [], [], 'php-workers')->commands;
        $history = self::scheduledHistory($commands);
        $history[0]['payload']['child_workflow_run_id'] = 'child-run-1';
        $history[1]['payload']['child_workflow_run_id'] = 'child-run-2';
        $history[] = ['id' => 'child-completed', 'event_type' => 'ChildRunCompleted', 'payload' => [
            ...$history[0]['payload'], 'result' => $codec->envelope('first-result'),
        ]];
        $history[] = ['event_type' => 'SelectionResolved', 'payload' => [
            'selection_group_id' => 'select-calls:1:2', 'selection_group_base_sequence' => 1, 'selection_group_size' => 2,
            'member_key' => 'first', 'member_index' => 0, 'member_base_sequence' => 1, 'member_size' => 1,
            'operation_kind' => 'child', 'operation_identity' => 'child-run-1', 'outcome' => 'completed',
            'resolution_event_id' => 'child-completed', 'resolution_event_type' => 'ChildRunCompleted',
        ]];
        self::assertSame([], (new Replayer($codec))->replay($handler, $history, [], 'php-workers')->commands);

        $changed = static function (WorkflowContext $context) use ($options): mixed {
            $selected = $context->select([
                'first' => static fn () => $context->childWorkflow('child', ['argument'], $options),
                'second' => static fn () => $context->childWorkflow('other-child', [], [
                    ...$options, 'cancellation_policy' => CancellationPolicy::TryCancel,
                ]),
            ]);
            return $selected->handles['second']->await();
        };
        try {
            (new Replayer($codec))->replay($changed, $history, [], 'php-workers');
            self::fail('A handle must retain the child policy captured by its selection.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('child_workflow_policy_changed', $error->reason);
            self::assertSame(2, $error->sequence);
        }
    }

    #[DataProvider('historyConflicts')]
    public function testConflictingOrInvalidHistoryIsRejected(string $event, mixed $policy, string $reason): void
    {
        $codec = new AvroPayloadCodec();
        $handler = self::workflow('sequential', self::cooperativeOptions());
        $commands = (new Replayer($codec))->replay($handler, [], [], 'php-workers')->commands;
        $history = self::scheduledHistory($commands);
        $history[] = ['event_type' => $event, 'payload' => ['sequence' => 1, 'cancellation_policy' => $policy]];
        try {
            (new Replayer($codec))->replay($handler, $history, [], 'php-workers');
            self::fail('Child policy history must remain coherent.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame($reason, $error->reason);
        }
    }

    public static function historyConflicts(): iterable
    {
        foreach (['ChildRunStarted', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'] as $event) {
            yield $event.'/conflict' => [$event, 'try_cancel', 'child_workflow_policy_history_conflict'];
            yield $event.'/invalid' => [$event, ['type' => 'try_cancel'], 'invalid_child_workflow_policy_history'];
        }
    }

    /** @return array<string, mixed> */
    private static function cooperativeOptions(): array
    {
        return [
            'parent_close_policy' => ParentClosePolicy::RequestCancellation,
            'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
        ];
    }

    /** @param array<string, mixed> $options */
    private static function workflow(string $mode, array $options): Closure
    {
        return static function (WorkflowContext $context) use ($mode, $options): mixed {
            return match ($mode) {
                'sequential' => $context->childWorkflow('child', ['argument'], $options),
                'deferred' => $context->parallel([$context->deferChildWorkflow('child', ['argument'], $options)]),
                'parallel' => $context->all([static fn () => $context->childWorkflow('child', ['argument'], $options)]),
                'selection' => $context->select([
                    'first' => static fn () => $context->childWorkflow('child', ['argument'], $options),
                    'second' => static fn () => $context->childWorkflow('other-child', [], $options),
                ]),
                default => throw new \LogicException('Unknown authoring mode.'),
            };
        };
    }

    /** @param list<array<string, mixed>> $commands
     *  @return list<array<string, mixed>>
     */
    private static function scheduledHistory(array $commands): array
    {
        return array_map(static fn (array $command, int $index): array => [
            'event_type' => 'ChildWorkflowScheduled',
            'payload' => ['sequence' => $index + 1, 'child_workflow_type' => $command['workflow_type'], ...$command],
        ], $commands, array_keys($commands));
    }
}
