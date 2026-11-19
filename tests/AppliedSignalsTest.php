<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\AppliedSignals;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * sdk-php#434: workflow code observes a signal only once the Server has applied
 * it, or when the current workflow task delivers it. The applied-only view is a
 * versioned change: the first live decision it changes records a version
 * marker, and histories without the marker keep the earlier view.
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

        $updateTask = $this->replayCondition($history, ['workflow_update_id' => 'update-1', 'update_name' => 'change']);
        self::assertSame('record_version_marker', $updateTask->commands[0]['type']);
        self::assertSame(AppliedSignals::CHANGE_ID, $updateTask->commands[0]['change_id']);
        self::assertSame(1, $updateTask->commands[0]['version']);
        self::assertNotContains('complete_workflow', array_column($updateTask->commands, 'type'));

        // The update task recorded the marker; the signal's own task delivers it.
        $history[] = $this->marker(2);
        $signalTask = $this->replayCondition($history, ['resume_source_kind' => 'workflow_signal',
            'workflow_signal_id' => 'signal-release', 'workflow_command_id' => 'command-release', 'signal_name' => 'release']);
        self::assertSame(['complete_workflow'], array_column($signalTask->commands, 'type'));
        self::assertSame(['released' => [['go']]], $this->codec->decodeEnvelope($signalTask->commands[0]['result']));
    }

    public function testARejectedSignalIsNeverObservedByANewDecision(): void
    {
        $history = [
            $this->conditionOpened(),
            $this->marker(2),
            $this->received('signal-release', 'command-release', 'release'),
            ['event_type' => 'SignalRejected', 'payload' => ['signal_id' => 'signal-release',
                'workflow_command_id' => 'command-release', 'signal_name' => 'release',
                'rejection_reason' => 'run_closed_before_signal_applied']],
        ];

        self::assertNotContains('complete_workflow', array_column($this->replayCondition($history)->commands, 'type'));
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
        self::assertSame(['record_version_marker', 'complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame([['first'], ['second'], ['third'], ['imported']],
            $this->codec->decodeEnvelope($result->commands[1]['result']));
    }

    public function testAHistoryThatNeverReadsSignalsReplaysUnchanged(): void
    {
        $history = [
            $this->received('signal-pending', 'command-pending', 'approve'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'work']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'work',
                'result' => $this->codec->envelope('done')]],
            $this->received('signal-later', 'command-later', 'approve'),
        ];
        $workflow = static fn (WorkflowContext $context): mixed => $context->activity('work');

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        self::assertCount(1, $result->commands);
        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame('done', $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testReadingSignalsThatAreAllAppliedRecordsNoMarker(): void
    {
        $history = [
            $this->received('signal-1', 'command-1', 'approve', 'first', workflowSequence: 1),
            ['event_type' => 'SignalReceived', 'payload' => ['signal_name' => 'approve',
                'arguments' => $this->codec->envelope(['imported'])]],
        ];
        $workflow = static fn (WorkflowContext $context): array => $context->signals('approve');

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame([['first'], ['imported']], $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testAHistoryWithoutTheMarkerKeepsTheEarlierViewForRecordedDecisions(): void
    {
        // Recorded by an earlier release: the activity was chosen while
        // signal-observed was received but not yet applied.
        $history = [
            $this->received('signal-observed', 'command-observed', 'approve', 'first'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'approved']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'approved',
                'result' => $this->codec->envelope('ok')]],
        ];

        $result = (new Replayer($this->codec))->replay(self::decision(), $history, [], 'php-workers');

        // The recorded decision replays unchanged; the first live decision the
        // applied-only view changes records the marker before it.
        self::assertSame(['record_version_marker', 'schedule_activity'], array_column($result->commands, 'type'));
        self::assertSame('second-waiting', $result->commands[1]['activity_type']);
    }

    public function testTheMarkerSelectsTheAppliedViewFromItsPositionOn(): void
    {
        $history = [
            $this->received('signal-observed', 'command-observed', 'approve', 'first'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'approved']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'approved',
                'result' => $this->codec->envelope('ok')]],
            $this->marker(2),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 3, 'activity_type' => 'second-waiting']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 3, 'activity_type' => 'second-waiting',
                'result' => $this->codec->envelope('later')]],
        ];

        $result = (new Replayer($this->codec))->replay(self::decision(), $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(['ok', 'later', []], $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testAChangedWorkflowIsReportedWithoutTryingTheEarlierView(): void
    {
        // The marker selects the applied view, so the recorded "approved" choice
        // cannot be explained by the unapplied signal: it is nondeterministic.
        $history = [
            $this->marker(1),
            $this->received('signal-observed', 'command-observed', 'approve'),
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'approved']],
        ];

        try {
            (new Replayer($this->codec))->replay(self::decision(), $history, [], 'php-workers');
            self::fail('A changed workflow must remain nondeterministic.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame(2, $error->sequence);
            self::assertStringContainsString('waiting', $error->getMessage());
        }
    }

    public function testACompletedHistoryRecordsNoMarker(): void
    {
        $history = [
            $this->received('signal-observed', 'command-observed', 'approve', 'first'),
            ['event_type' => 'WorkflowCompleted', 'payload' => []],
        ];
        $workflow = static fn (WorkflowContext $context): array => $context->signals('approve');

        $result = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame([['first']], $this->codec->decodeEnvelope($result->commands[0]['result']));
    }

    private static function decision(): \Closure
    {
        return static function (WorkflowContext $context): array {
            $first = $context->activity($context->signals('approve') !== [] ? 'approved' : 'waiting');
            $second = $context->activity($context->signals('approve') !== [] ? 'second-approved' : 'second-waiting');

            return [$first, $second, $context->signals('approve')];
        };
    }

    /** @return array<string, mixed> */
    private function conditionOpened(): array
    {
        return ['event_type' => 'ConditionWaitOpened', 'payload' => ['sequence' => 1,
            'condition_wait_id' => 'wait-release', 'condition_key' => 'release', 'timeout_seconds' => 300]];
    }

    /** @return array<string, mixed> */
    private function marker(int $sequence): array
    {
        return ['event_type' => 'VersionMarkerRecorded', 'payload' => ['sequence' => $sequence,
            'change_id' => AppliedSignals::CHANGE_ID, 'version' => 1, 'min_supported' => -1, 'max_supported' => 1]];
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
    private function replayCondition(array $history, array $task = []): ReplayResult
    {
        $workflow = static function (WorkflowContext $context): array {
            $context->waitCondition(static fn (): bool => $context->signals('release') !== [], key: 'release', timeout: 300);

            return ['released' => $context->signals('release')];
        };

        return (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers',
            ['workflow_id' => 'workflow-434', 'run_id' => 'run-434'] + $task);
    }
}
