<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\AppliedSignals;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * sdk-php#434: workflow code observes a signal only once the Server has applied
 * it, or when the current workflow task delivers it, so a received signal the
 * Server later rejects never changes a workflow decision.
 */
final class AppliedSignalsTest extends TestCase
{
    private AvroPayloadCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new AvroPayloadCodec();
    }

    public function testAQueuedUpdateIsAppliedBeforeAReceivedSignalClosesTheRun(): void
    {
        // The update was queued before the release signal. The update's task
        // must not see the release signal, so the run stays open for it.
        $history = [
            $this->conditionOpened(),
            ['event_type' => 'UpdateAccepted', 'payload' => ['update_id' => 'update-1', 'update_name' => 'change',
                'arguments' => $this->codec->envelope([1])]],
            $this->received('signal-release', 'command-release', 'release'),
        ];

        $updateTask = $this->replay($history, ['workflow_update_id' => 'update-1', 'update_name' => 'change']);
        self::assertNotContains('complete_workflow', array_column($updateTask->commands, 'type'));

        // The signal's own task delivers it: now the condition is satisfied.
        $signalTask = $this->replay($history, ['resume_source_kind' => 'workflow_signal',
            'workflow_signal_id' => 'signal-release', 'workflow_command_id' => 'command-release', 'signal_name' => 'release']);
        self::assertSame('complete_workflow', $signalTask->commands[0]['type']);
        self::assertSame(['released' => [['go']]], $this->codec->decodeEnvelope($signalTask->commands[0]['result']));
    }

    public function testARejectedSignalIsNeverObserved(): void
    {
        $history = [
            $this->conditionOpened(),
            $this->received('signal-release', 'command-release', 'release'),
            ['event_type' => 'SignalRejected', 'payload' => ['signal_id' => 'signal-release',
                'workflow_command_id' => 'command-release', 'signal_name' => 'release',
                'rejection_reason' => 'run_closed_before_signal_applied']],
        ];

        self::assertNotContains('complete_workflow', array_column($this->replay($history)->commands, 'type'));
    }

    public function testAppliedSignalsAreObservedInReceiptOrder(): void
    {
        $history = [
            $this->received('signal-1', 'command-1', 'release', 'first'),
            $this->received('signal-2', 'command-2', 'release', 'second'),
            $this->received('signal-3', 'command-3', 'release', 'third', workflowSequence: 4),
            $this->received('signal-4', 'command-4', 'release', 'fourth'),
            ['event_type' => 'SignalApplied', 'payload' => ['signal_id' => 'signal-2', 'workflow_command_id' => 'command-2',
                'signal_name' => 'release', 'value' => $this->codec->envelope(['second'])]],
            ['event_type' => 'SignalApplied', 'payload' => ['workflow_command_id' => 'command-1', 'signal_name' => 'release',
                'value' => $this->codec->envelope(['first'])]],
            ['event_type' => 'SignalReceived', 'payload' => ['signal_name' => 'release',
                'arguments' => $this->codec->envelope(['imported'])]],
        ];
        $workflow = static fn (WorkflowContext $context): array => $context->signals('release');

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        // Applied by event (by signal or command identity), consumed with a
        // sequence, and an identity-less imported record; never the unapplied one.
        self::assertSame([['first'], ['second'], ['third'], ['imported']],
            $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testALegacyHistoryKeepsTheSignalsItsTasksObservedButHidesLaterOnes(): void
    {
        // Recorded by an SDK that showed every received signal: the activity was
        // chosen while signal-observed was received but not yet applied.
        $history = [
            $this->received('signal-observed', 'command-observed', 'approve', 'first'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'approved']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'approved',
                'result' => $this->codec->envelope('ok')]],
            $this->received('signal-later', 'command-later', 'approve', 'later'),
        ];
        $workflow = static function (WorkflowContext $context): array {
            $result = $context->activity($context->signals('approve') !== [] ? 'approved' : 'waiting');

            return ['result' => $result, 'signals' => $context->signals('approve')];
        };

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['result' => 'ok', 'signals' => [['first']]],
            $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testAGenuinelyChangedWorkflowStillReportsTheAppliedOnlyDivergence(): void
    {
        $history = [
            $this->received('signal-observed', 'command-observed', 'approve'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'removed']],
        ];
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->activity($context->signals('approve') !== [] ? 'approved' : 'waiting');
        };

        try {
            (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');
            self::fail('A changed workflow must remain nondeterministic.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame(1, $error->sequence);
            self::assertStringContainsString('waiting', $error->getMessage());
        }
    }

    public function testFreshDecisionsWithoutContradictionUseOnlyAppliedSignals(): void
    {
        $history = [
            $this->received('signal-pending', 'command-pending', 'approve'),
        ];
        $workflow = static fn (WorkflowContext $context): mixed
            => $context->activity($context->signals('approve') !== [] ? 'approved' : 'waiting');

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('schedule_activity', $result->commands[0]['type']);
        self::assertSame('waiting', $result->commands[0]['activity_type']);
    }

    public function testRecordedObservationsCoverOnlySignalsBeforeTheLastDecision(): void
    {
        $history = [
            $this->received('signal-1', 'command-1', 'approve'),
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1]],
            $this->received('signal-2', 'command-2', 'approve'),
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1]],
            $this->received('signal-3', 'command-3', 'approve'),
        ];
        $applied = AppliedSignals::fromHistory($history, []);
        $recorded = $applied->withRecordedObservations($history);

        self::assertFalse($applied->observes($history[0], 0));
        self::assertTrue($recorded->observes($history[0], 0));
        self::assertFalse($recorded->observes($history[2], 2));
        self::assertFalse($recorded->observes($history[4], 4));
        self::assertTrue($recorded->showsMoreThan($applied, $history));
        self::assertTrue($recorded->hidesAny($history));
    }

    /** @return array<string, mixed> */
    private function conditionOpened(): array
    {
        return ['event_type' => 'ConditionWaitOpened', 'payload' => ['sequence' => 1,
            'condition_wait_id' => 'wait-release', 'condition_key' => 'release', 'timeout_seconds' => 300]];
    }

    /** @return array<string, mixed> */
    private function received(string $signalId, string $commandId, string $name, string $value = 'go', ?int $workflowSequence = null): array
    {
        $payload = ['signal_id' => $signalId, 'workflow_command_id' => $commandId, 'signal_name' => $name,
            'arguments' => $this->codec->envelope([$value])];
        if ($workflowSequence !== null) {
            $payload['workflow_sequence'] = $workflowSequence;
        }

        return ['event_type' => 'SignalReceived', 'payload' => $payload];
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    private function replay(array $history, array $task = []): \DurableWorkflow\Worker\ReplayResult
    {
        $workflow = static function (WorkflowContext $context): array {
            $context->waitCondition(static fn (): bool => $context->signals('release') !== [], key: 'release', timeout: 300);

            return ['released' => $context->signals('release')];
        };

        return (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers',
            ['workflow_id' => 'workflow-434', 'run_id' => 'run-434'] + $task);
    }
}
