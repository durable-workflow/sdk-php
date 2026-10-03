<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\CancellationPolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityCancellationPolicyTest extends TestCase
{
    public static function policies(): array
    {
        return [[CancellationPolicy::TryCancel], [CancellationPolicy::WaitCancellationCompleted]];
    }

    #[DataProvider('policies')]
    public function test_fresh_admission_and_cold_recovery_preserve_the_authored_policy_without_invoking_a_callback(CancellationPolicy $policy): void
    {
        $codec = new AvroPayloadCodec();
        $handler = static fn (WorkflowContext $context) => $context->localActivity('effect', [], ['cancellation_policy' => $policy]);
        $fresh = $this->replay($handler, [], $policy);
        self::assertSame($policy->value, $fresh->preparedLocalActivity->descriptor($codec)['cancellation_policy']);
        self::assertFalse($fresh->preparedLocalActivity->recover);
        $history = $this->history($policy);
        $recovered = $this->replay($handler, $history, $policy);
        self::assertTrue($recovered->preparedLocalActivity->recover);
        self::assertSame($fresh->preparedLocalActivity->descriptor($codec), $recovered->preparedLocalActivity->descriptor($codec));
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => [
            'sequence' => 1, 'execution_mode' => 'local', 'cancellation_policy' => $policy->value,
            'result' => $codec->envelope('durable-result'),
        ]];
        $finished = $this->replay($handler, $history, $policy);
        self::assertNull($finished->preparedLocalActivity);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame('durable-result', $codec->decodeEnvelope($finished->commands[0]['result']));
    }

    #[DataProvider('policies')]
    public function test_changed_policy_is_refused_during_cold_replay(CancellationPolicy $recorded): void
    {
        $changed = $recorded === CancellationPolicy::TryCancel ? CancellationPolicy::WaitCancellationCompleted : CancellationPolicy::TryCancel;
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('cancellation_policy changed');
        $this->replay(static fn (WorkflowContext $context) => $context->localActivity('effect', [], ['cancellation_policy' => $changed]),
            $this->history($recorded), $changed);
    }

    #[DataProvider('policies')]
    public function test_legacy_inline_mode_refuses_an_explicit_policy_before_callback_execution(CancellationPolicy $policy): void
    {
        $this->expectException(InvalidArgumentException::class);
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->localActivity('effect', [], ['cancellation_policy' => $policy]),
            [], [], 'prepared', localActivityExecutor: static function (): never { self::fail('An unsupported local policy executed application code.'); },
        );
    }

    #[DataProvider('policies')]
    public function test_prepared_mode_requires_the_discovered_policy_before_admission(CancellationPolicy $policy): void
    {
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('prepared_local_activity_cancellation_policy_not_supported');
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->localActivity('effect', [], ['cancellation_policy' => $policy]),
            [], [], 'prepared', localActivityExecutor: static function (): never { self::fail('Undiscovered policy executed application code.'); },
            prepareLocalActivities: true,
        );
    }

    public static function invalidPolicies(): array
    {
        return [[CancellationPolicy::Abandon], ['abandon'], [null], ['invalid'], [true]];
    }

    #[DataProvider('invalidPolicies')]
    public function test_abandon_and_malformed_policies_are_refused_even_with_a_finite_timeout(mixed $policy): void
    {
        $this->expectException(InvalidArgumentException::class);
        WorkflowCommand::localActivity('effect', [], ['cancellation_policy' => $policy, 'schedule_to_close_timeout' => 30],
            static function (): never { self::fail('Invalid policy executed application code.'); }, prepared: true);
    }

    #[DataProvider('policies')]
    public function test_enum_and_string_authoring_make_the_same_descriptor(CancellationPolicy $policy): void
    {
        $executor = static function (): never { self::fail('Authoring must not run callbacks.'); };
        $typed = WorkflowCommand::localActivity('effect', [], ['cancellation_policy' => $policy], $executor, prepared: true);
        $string = WorkflowCommand::localActivity('effect', [], ['cancellation_policy' => $policy->value], $executor, prepared: true);
        self::assertSame($typed->attributes, $string->attributes);
        self::assertSame($policy->value, $typed->attributes['cancellation_policy']);
    }

    private function replay(callable $handler, array $history, CancellationPolicy $policy): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($handler, $history, [], 'prepared',
            localActivityExecutor: static function (): never { self::fail('Prepared admission or replay ran an unadmitted callback.'); },
            prepareLocalActivities: true, localActivityCancellationPolicies: ['try_cancel', 'wait_cancellation_completed']);
    }

    private function history(CancellationPolicy $policy): array
    {
        return [['event_type' => 'ActivityScheduled', 'payload' => [
            'sequence' => 1, 'activity_type' => 'effect', 'execution_mode' => 'local',
            'activity' => ['cancellation_policy' => $policy->value],
        ]], ['event_type' => 'ActivityStarted', 'payload' => [
            'sequence' => 1, 'execution_mode' => 'local', 'activity' => ['cancellation_policy' => $policy->value],
        ]]];
    }
}
