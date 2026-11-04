<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ChildWorkflowHandle;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;

/** startChildWorkflow(): a committed child start handle (#185, Server child-start-turn contract). */
final class ChildWorkflowHandleTest extends TestCase
{
    private AvroPayloadCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new AvroPayloadCodec();
    }

    public function testTheStartEndsTheTurnWithAStartedAwaitAndNoOtherCommand(): void
    {
        $commands = $this->replay([]);

        self::assertCount(1, $commands);
        self::assertSame('start_child_workflow', $commands[0]['type']);
        self::assertSame('billing.reconcile', $commands[0]['workflow_type']);
        self::assertSame('started', $commands[0]['await']);
        self::assertSame('abandon', $commands[0]['parent_close_policy']);
    }

    public function testTheFreshTurnReturnsTheCommittedIdentityWhileTheChildRuns(): void
    {
        $commands = $this->replay($this->started());

        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(['call-1', 'child-instance', 'child-run-1'], $this->codec->decodeEnvelope($commands[0]['arguments']));
    }

    public function testAwaitingAnOpenChildEndsTheTurnWithoutACommand(): void
    {
        self::assertSame([], $this->replay([...$this->started(), ...$this->notified()]));
    }

    public function testAwaitReturnsTheChildResultWithoutANewSequence(): void
    {
        $commands = $this->replay([...$this->started(), ...$this->notified(), $this->resolved('ChildRunCompleted', ['result' => $this->codec->envelope('reconciled')])]);

        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame('reconciled', $this->codec->decodeEnvelope($commands[0]['result']));
    }

    public function testAwaitThrowsTheChildFailure(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            $child = $context->startChildWorkflow('billing.reconcile', ['acct-1']);
            try {
                return (string) $child->await();
            } catch (ChildWorkflowFailed $failure) {
                return 'failed: '.$failure->getMessage();
            }
        };
        $commands = (new Replayer($this->codec))->replay($workflow, [
            ...$this->started(),
            $this->resolved('ChildRunFailed', ['message' => 'Ledger mismatch.']),
        ], [], 'php-workers')->commands;

        self::assertSame('failed: Ledger mismatch.', $this->codec->decodeEnvelope($commands[0]['result']));
    }

    public function testTheInitialRunStaysFixedThroughChildContinueAsNew(): void
    {
        $workflow = static function (WorkflowContext $context): array {
            $child = $context->startChildWorkflow('billing.reconcile', ['acct-1']);
            $result = $child->await();

            return [$child->identity()->callId, $child->identity()->workflowId, $child->identity()->initialRunId, $result];
        };
        $continued = ['event_type' => 'ChildRunStarted', 'payload' => [
            'sequence' => 1, 'child_call_id' => 'call-1', 'child_workflow_instance_id' => 'child-instance', 'child_workflow_run_id' => 'child-run-2',
        ]];
        $commands = (new Replayer($this->codec))->replay($workflow, [
            ...$this->started(), $continued,
            $this->resolved('ChildRunCompleted', ['child_workflow_run_id' => 'child-run-2', 'result' => $this->codec->envelope('done')]),
        ], [], 'php-workers')->commands;

        self::assertSame(['call-1', 'child-instance', 'child-run-1', 'done'], $this->codec->decodeEnvelope($commands[0]['result']));
    }

    public function testColdReplayRebuildsTheSameHandleWithoutAnotherStart(): void
    {
        $history = [...$this->started(), ...$this->notified()];

        self::assertSame($this->replay($history), $this->replay($history));
        self::assertNotContains('start_child_workflow', array_column($this->replay($history), 'type'));
    }

    public function testACommittedStartWithoutIdentityIsRefused(): void
    {
        $history = $this->started();
        unset($history[0]['payload']['child_call_id'], $history[1]['payload']['child_call_id']);

        try {
            $this->replay($history);
            self::fail('A start without its committed identity must be refused.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('child_start_identity_missing', $exception->reason);
        }
    }

    public function testALaterRunCannotSupplyTheMissingInitialIdentity(): void
    {
        $history = $this->started();
        unset($history[0]['payload']['child_call_id'], $history[1]['payload']['child_call_id']);
        $history[] = ['event_type' => 'ChildRunStarted', 'payload' => [
            'sequence' => 1, 'child_call_id' => 'call-1', 'child_workflow_instance_id' => 'child-instance',
            'child_workflow_run_id' => 'child-run-2', 'child_workflow_type' => 'billing.reconcile',
        ]];

        try {
            $this->replay($history);
            self::fail('A later run must not become the initial child identity.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('child_start_identity_missing', $exception->reason);
        }
    }

    public function testTheExistingChildCallKeepsReturningTheResult(): void
    {
        $workflow = static fn (WorkflowContext $context): mixed => $context->childWorkflow('billing.reconcile', ['acct-1']);
        $commands = (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers')->commands;

        self::assertSame('start_child_workflow', $commands[0]['type']);
        self::assertArrayNotHasKey('await', $commands[0]);
    }

    public function testAnAwaitOptionIsNotAcceptedOnTheResultCall(): void
    {
        $workflow = static fn (WorkflowContext $context): mixed => $context->childWorkflow('billing.reconcile', [], ['await' => 'started']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('use startChildWorkflow()');

        (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers');
    }

    public function testAStartedChildCannotJoinAGroup(): void
    {
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn (): ChildWorkflowHandle => $context->startChildWorkflow('billing.reconcile'),
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('deferChildWorkflow()');

        (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers');
    }

    /**
     * A runtime without child_workflow_start_turn ignores the await field and
     * gives no parent turn, so the worker refuses before submitting the start.
     */
    public function testTheWorkerSubmitsAStartedChildOnlyWhenTheRuntimeAdvertisesTheTurn(): void
    {
        foreach ([false, true] as $supported) {
            $commands = null;
            $failure = null;
            $transport = new FakeTransport(handler: static function (
                string $method, string $uri, array $headers, ?array $body,
            ) use ($supported, &$commands, &$failure): array {
                if (str_ends_with($uri, '/workflow-tasks/poll')) {
                    return ['server_capabilities' => ['child_workflow_start_turn' => $supported],
                        'poll_status' => 'leased', 'task' => ['task_id' => 'parent-task', 'workflow_task_attempt' => 1,
                            'lease_owner' => 'worker-1', 'workflow_id' => 'parent', 'run_id' => 'parent-run',
                            'workflow_type' => 'billing.parent', 'payload_codec' => 'avro', 'history_events' => []]];
                }
                if (str_ends_with($uri, '/parent-task/heartbeat')) {
                    return ['task_id' => 'parent-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true];
                }
                if (str_ends_with($uri, '/parent-task/complete')) {
                    $commands = $body['commands'];

                    return ['completed' => true];
                }
                if (str_ends_with($uri, '/parent-task/fail')) {
                    $failure = $body['failure']['message'] ?? null;

                    return ['recorded' => true];
                }
                if (str_ends_with($uri, '/activity-tasks/poll')) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }
                self::fail("Unexpected request {$method} {$uri}");
            });
            $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'parents', workerId: 'worker-1');
            $worker->registerWorkflow('billing.parent', static fn (WorkflowContext $context): mixed =>
                $context->startChildWorkflow('billing.reconcile', ['acct-1'])->await());

            self::assertTrue($worker->tick(0));
            if ($supported) {
                self::assertNull($failure);
                self::assertSame('started', $commands[0]['await'] ?? null);
            } else {
                self::assertNull($commands, 'nothing is submitted without the advertised turn');
                self::assertStringContainsString('child_workflow_start_turn_unavailable', (string) $failure);
            }
        }
    }

    /** @param list<array<string, mixed>> $history
     *  @return list<array<string, mixed>>
     */
    private function replay(array $history): array
    {
        $workflow = static function (WorkflowContext $context): mixed {
            $child = $context->startChildWorkflow('billing.reconcile', ['acct-1'], [
                'parent_close_policy' => ParentClosePolicy::Abandon,
            ]);
            $identity = $child->identity();
            $context->activity('notify.child-started', [$identity->callId, $identity->workflowId, $identity->initialRunId]);

            return $child->await();
        };

        return (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers')->commands;
    }

    /** @return list<array<string, mixed>> */
    private function started(): array
    {
        $identity = ['sequence' => 1, 'child_call_id' => 'call-1', 'workflow_link_id' => 'call-1',
            'child_workflow_instance_id' => 'child-instance', 'child_workflow_run_id' => 'child-run-1', 'child_workflow_type' => 'billing.reconcile'];

        return [
            ['event_type' => 'ChildWorkflowScheduled', 'payload' => $identity],
            ['event_type' => 'ChildRunStarted', 'payload' => $identity],
        ];
    }

    /** @return list<array<string, mixed>> */
    private function notified(): array
    {
        return [
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'notify.child-started', 'activity_execution_id' => 'act-2']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 2, 'activity_type' => 'notify.child-started', 'activity_execution_id' => 'act-2', 'result' => $this->codec->envelope(null)]],
        ];
    }

    /** @param array<string, mixed> $payload
     *  @return array<string, mixed>
     */
    private function resolved(string $type, array $payload): array
    {
        return ['event_type' => $type, 'payload' => [
            'sequence' => 1, 'child_call_id' => 'call-1', 'child_workflow_instance_id' => 'child-instance',
            'child_workflow_run_id' => 'child-run-1', 'child_workflow_type' => 'billing.reconcile', ...$payload,
        ]];
    }
}
