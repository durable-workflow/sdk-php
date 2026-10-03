<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\CancellationPolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ActivityCancellationPolicyTest extends TestCase
{
    public function testChangedRemotePolicyIsRejectedDuringReplay(): void
    {
        $history = [self::event('ActivityScheduled', 'wait_cancellation_completed')];
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($history, ['cancellation_policy' => 'try_cancel']);
    }

    #[DataProvider('policyProvider')]
    public function testTypedAndStringPoliciesProduceTheSamePortableCommand(CancellationPolicy $policy): void
    {
        $options = ['schedule_to_close_timeout' => 60, 'cancellation_policy' => $policy];
        $typed = WorkflowCommand::activity('work', [], $options);
        $options['cancellation_policy'] = $policy->value;
        self::assertSame($options, WorkflowCommand::canonicalRemoteActivityOptions($options));
        self::assertSame(WorkflowCommand::activity('work', [], $options)->attributes, $typed->attributes);
        self::assertSame($policy->value, $typed->attributes['cancellation_policy']);
    }

    public static function policyProvider(): array
    {
        return array_map(static fn (CancellationPolicy $policy): array => [$policy], CancellationPolicy::cases());
    }

    #[DataProvider('policyProvider')]
    public function testMatchingCanonicalPolicyReplays(CancellationPolicy $policy): void
    {
        self::assertSame([], $this->replay([self::event('ActivityScheduled', $policy->value)], [
            'cancellation_policy' => $policy,
            'schedule_to_close_timeout' => 60,
        ])->commands);
    }

    public function testOmittedPolicyPreservesHistoricalWireAndReplay(): void
    {
        self::assertArrayNotHasKey('cancellation_policy', WorkflowCommand::activity('work', [])->attributes);
        $history = [self::event('ActivityScheduled')];
        self::assertSame([], $this->replay($history)->commands);
        self::assertSame([], $this->replay($history, ['cancellation_policy' => CancellationPolicy::TryCancel])->commands);
    }

    public function testOriginalPolicySurvivesLaterEventsWithoutOptions(): void
    {
        $history = [
            self::event('ActivityScheduled', 'wait_cancellation_completed'),
            self::event('ActivityStarted'),
            self::event('ActivityCompleted'),
        ];
        $history[2]['payload']['result'] = (new AvroPayloadCodec())->envelope('done');
        $result = $this->replay($history, ['cancellation_policy' => CancellationPolicy::WaitCancellationCompleted]);
        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame('done', (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));

        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($history);
    }

    public function testConflictingCanonicalPoliciesAreRejected(): void
    {
        $history = [self::event('ActivityScheduled', 'try_cancel'), self::event('ActivityStarted', 'wait_cancellation_completed')];
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($history);
    }

    #[DataProvider('invalidPolicyProvider')]
    public function testInvalidPolicyIsRefusedBeforeSuspension(mixed $policy): void
    {
        $this->expectException(InvalidArgumentException::class);
        WorkflowCommand::activity('work', [], ['cancellation_policy' => $policy]);
    }

    public static function invalidPolicyProvider(): array
    {
        return [['unknown'], [null], [false], [1], [[]]];
    }

    #[DataProvider('invalidPolicyProvider')]
    public function testMalformedRecordedPolicyIsRejected(mixed $policy): void
    {
        $event = self::event('ActivityScheduled');
        $event['payload']['activity']['cancellation_policy'] = $policy;
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay([$event]);
    }

    #[DataProvider('unboundedAbandonProvider')]
    public function testAbandonRequiresAFinitePositiveTotalTimeout(array $options): void
    {
        $this->expectException(InvalidArgumentException::class);
        WorkflowCommand::activity('work', [], ['cancellation_policy' => CancellationPolicy::Abandon, ...$options]);
    }

    public static function unboundedAbandonProvider(): array
    {
        return [[[]], [['schedule_to_close_timeout' => null]], [['schedule_to_close_timeout' => 0]],
            [['schedule_to_close_timeout' => -1]], [['schedule_to_close_timeout' => 1.5]],
            [['schedule_to_close_timeout' => '60']], [['schedule_to_close_timeout' => true]]];
    }

    private static function event(string $type, ?string $policy = null): array
    {
        $activity = ['type' => 'work'];
        if ($policy !== null) {
            $activity['cancellation_policy'] = $policy;
        }

        return ['event_type' => $type, 'payload' => ['sequence' => 1, 'activity' => $activity]];
    }

    private function replay(array $history, array $options = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): mixed => $context->activity('work', [], $options),
            $history,
            [],
            'php-workers',
        );
    }
}
