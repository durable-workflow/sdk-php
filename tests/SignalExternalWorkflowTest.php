<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Model\ExternalSignalOutcome;
use DurableWorkflow\Model\ExternalSignalResult;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** signalExternalWorkflow(): a durable signal to another workflow (.github#223, Server #1134 contract). */
final class SignalExternalWorkflowTest extends TestCase
{
    private AvroPayloadCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new AvroPayloadCodec();
    }

    public function testTheSignalIsTheOnlyCommandOfItsTurnInTheContractShape(): void
    {
        $commands = $this->replay([]);

        self::assertCount(1, $commands);
        self::assertSame(['type', 'workflow_id', 'signal_name', 'arguments'], array_keys($commands[0]));
        self::assertSame('signal_external_workflow', $commands[0]['type']);
        self::assertSame('order-42', $commands[0]['workflow_id']);
        self::assertSame('approve', $commands[0]['signal_name']);
        self::assertSame('avro', $commands[0]['arguments']['codec']);
        self::assertSame(['Ada', 3], $this->codec->decodeEnvelope($commands[0]['arguments']));
        self::assertSame($this->codec->envelope(['Ada', 3]), $commands[0]['arguments']);
    }

    public function testARunTargetAndNoArgumentsKeepTheirWireFields(): void
    {
        $workflow = static fn (WorkflowContext $context): ExternalSignalResult
            => $context->signalExternalWorkflow('order-42', 'approve', runId: 'run-7');
        $commands = (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers')->commands;

        self::assertSame('run-7', $commands[0]['run_id']);
        self::assertSame($this->codec->envelope([]), $commands[0]['arguments'], 'no arguments is an encoded empty array');
        self::assertSame([], $this->codec->decodeEnvelope($commands[0]['arguments']));
    }

    public function testTheDeliveredOutcomeResumesTheWorkflowWithTheRecordedResult(): void
    {
        $commands = $this->replay($this->recorded($this->delivered()));

        self::assertCount(1, $commands);
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame([
            'outcome' => 'delivered', 'delivered' => true, 'reason' => null, 'target_workflow_id' => 'order-42',
            'target_run_id' => 'order-42-run-1', 'target_command_id' => 'cmd-9', 'target_signal_id' => 'sig-9',
            'retained' => false,
        ], $this->codec->decodeEnvelope($commands[0]['result']));
    }

    /** @return iterable<string, array{string, string, ?string, ?string}> */
    public static function failedOutcomes(): iterable
    {
        yield 'missing instance' => ['target_not_found', 'instance_not_found', null, null];
        yield 'missing run' => ['target_not_found', 'run_not_found', null, null];
        yield 'closed run' => ['target_closed', 'run_not_active', 'order-42-run-1', 'cmd-9'];
        yield 'replaced run' => ['target_closed', 'historical_run', 'order-42-run-1', null];
        yield 'unknown signal' => ['rejected', 'unknown_signal', 'order-42-run-1', 'cmd-9'];
        yield 'self target' => ['rejected', 'self_target', null, null];
    }

    #[DataProvider('failedOutcomes')]
    public function testAFailedOutcomeIsAReturnedValueNotAnException(string $outcome, string $reason, ?string $runId, ?string $commandId): void
    {
        $commands = $this->replay($this->recorded(['event_type' => 'ExternalSignalFailed', 'payload' => [
            ...$this->identity(), 'target_workflow_run_id' => $runId, 'outcome' => $outcome, 'reason' => $reason,
            'message' => 'Not delivered.', 'target_command_id' => $commandId,
        ]]));

        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame([
            'outcome' => $outcome, 'delivered' => false, 'reason' => $reason, 'target_workflow_id' => 'order-42',
            'target_run_id' => $runId, 'target_command_id' => $commandId, 'target_signal_id' => null, 'retained' => false,
        ], $this->codec->decodeEnvelope($commands[0]['result']));
    }

    public function testColdReplayReturnsTheSameResultWithoutSendingTheSignalAgain(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            $first = $context->signalExternalWorkflow('order-42', 'approve', ['Ada', 3]);

            return $context->activity('notify', [$first->outcome->value]);
        };
        $history = $this->recorded($this->delivered());
        $first = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers')->commands;
        $second = (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers')->commands;

        self::assertSame($first, $second);
        self::assertSame(['schedule_activity'], array_column($first, 'type'));
        self::assertSame(['delivered'], $this->codec->decodeEnvelope($first[0]['arguments']));
    }

    public function testTheResultObjectCarriesEveryRecordedField(): void
    {
        $captured = null;
        $workflow = static function (WorkflowContext $context) use (&$captured): string {
            $captured = $context->signalExternalWorkflow('order-42', 'approve', ['Ada', 3]);

            return 'done';
        };
        (new Replayer($this->codec))->replay($workflow, $this->recorded(['event_type' => 'ExternalSignalDelivered', 'payload' => [
            ...$this->identity(), 'target_workflow_run_id' => 'order-42-run-1', 'outcome' => 'delivered',
            'target_command_id' => 'cmd-9', 'target_signal_id' => 'sig-9', 'retained' => true,
        ]]), [], 'php-workers');

        self::assertInstanceOf(ExternalSignalResult::class, $captured);
        self::assertSame(ExternalSignalOutcome::Delivered, $captured->outcome);
        self::assertTrue($captured->delivered());
        self::assertTrue($captured->retained);
        self::assertSame('approve', $captured->signalName);
        self::assertSame('workflow-signal:sender-run:1', $captured->requestId);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function changedReplays(): iterable
    {
        yield 'target workflow' => [['target_workflow_instance_id' => 'order-43']];
        yield 'target run' => [['target_workflow_run_id' => 'run-7']];
        yield 'signal name' => [['signal_name' => 'reject']];
    }

    /** @param array<string, mixed> $recorded */
    #[DataProvider('changedReplays')]
    public function testAChangedTargetOrNameIsNondeterministic(array $recorded): void
    {
        $history = $this->recorded($this->delivered());
        $history[0]['payload'] = array_replace($history[0]['payload'], $recorded);
        $history[1]['payload'] = array_replace($history[1]['payload'], array_diff_key($recorded, ['target_workflow_run_id' => true]));

        try {
            $this->replay($history);
            self::fail('A changed signal must not replay.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('external_signal_mismatch', $exception->reason);
            self::assertSame(1, $exception->sequence);
        }
    }

    public function testChangedArgumentsAreNondeterministicWithoutNamingTheValues(): void
    {
        $history = $this->recorded($this->delivered());
        $history[0]['payload']['arguments'] = $this->codec->envelope(['Bob', 3]);

        try {
            $this->replay($history);
            self::fail('Changed arguments must not replay.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('external_signal_mismatch', $exception->reason);
            self::assertStringNotContainsString('Bob', $exception->getMessage().$exception->expected.$exception->actual);
        }
    }

    public function testAnotherCommandAtTheRecordedSequenceIsNondeterministic(): void
    {
        $workflow = static fn (WorkflowContext $context): mixed => $context->activity('notify');

        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('History contains external_signal but workflow scheduled activity');

        (new Replayer($this->codec))->replay($workflow, $this->recorded($this->delivered()), [], 'php-workers');
    }

    public function testAnOutcomeWithoutItsRequestIsRefused(): void
    {
        try {
            $this->replay([$this->delivered()]);
            self::fail('An outcome without its request must be refused.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('external_signal_history_invalid', $exception->reason);
        }
    }

    public function testAnUnknownOutcomeIsRefused(): void
    {
        $history = $this->recorded(['event_type' => 'ExternalSignalFailed', 'payload' => [...$this->identity(), 'outcome' => 'lost']]);

        try {
            $this->replay($history);
            self::fail('An unknown outcome must be refused.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('external_signal_history_invalid', $exception->reason);
        }
    }

    public function testInvalidTargetsAndArgumentsAreRefusedBeforeAnyCommand(): void
    {
        foreach ([
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow('', 'approve'),
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow(str_repeat('w', 192), 'approve'),
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow('order-42', ' '),
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow('order-42', str_repeat('s', 256)),
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow('order-42', 'approve', ['a' => 1]),
            static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow('order-42', 'approve', runId: ''),
        ] as $index => $workflow) {
            try {
                (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers');
                self::fail("Case {$index} must be refused.");
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
        $limit = static fn (WorkflowContext $context): mixed => $context->signalExternalWorkflow(str_repeat('é', 191), str_repeat('s', 255));
        self::assertSame('signal_external_workflow', (new Replayer($this->codec))->replay($limit, [], [], 'php-workers')->commands[0]['type']);
    }

    public function testASignalCannotJoinAGroup(): void
    {
        $workflow = static fn (WorkflowContext $context): array => $context->all([
            static fn (): ExternalSignalResult => $context->signalExternalWorkflow('order-42', 'approve'),
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('signalExternalWorkflow() cannot run inside');

        (new Replayer($this->codec))->replay($workflow, [], [], 'php-workers');
    }

    public function testAStartedChildHandleSignalsTheChildInstance(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            $child = $context->startChildWorkflow('billing.reconcile', ['acct-1']);

            return $child->signal('approve', ['Ada'])->outcome->value;
        };
        $identity = ['sequence' => 1, 'child_call_id' => 'call-1', 'workflow_link_id' => 'call-1',
            'child_workflow_instance_id' => 'child-instance', 'child_workflow_run_id' => 'child-run-1', 'child_workflow_type' => 'billing.reconcile'];
        $started = [
            ['event_type' => 'ChildWorkflowScheduled', 'payload' => $identity],
            ['event_type' => 'ChildRunStarted', 'payload' => $identity],
        ];
        $commands = (new Replayer($this->codec))->replay($workflow, $started, [], 'php-workers')->commands;

        self::assertSame('signal_external_workflow', $commands[0]['type']);
        self::assertSame('child-instance', $commands[0]['workflow_id']);
        self::assertArrayNotHasKey('run_id', $commands[0], 'the signal follows the child through continue-as-new');

        $signal = ['sequence' => 2, 'target_workflow_instance_id' => 'child-instance', 'target_workflow_run_id' => null,
            'signal_name' => 'approve', 'arguments' => $this->codec->envelope(['Ada']), 'payload_codec' => 'avro'];
        $commands = (new Replayer($this->codec))->replay($workflow, [...$started,
            ['event_type' => 'ExternalSignalRequested', 'payload' => $signal],
            ['event_type' => 'ExternalSignalDelivered', 'payload' => [...$signal, 'target_workflow_run_id' => 'child-run-1', 'outcome' => 'delivered']],
        ], [], 'php-workers')->commands;

        self::assertSame('delivered', $this->codec->decodeEnvelope($commands[0]['result']));
    }

    /**
     * A runtime without signal_external_workflow refuses the command and fails
     * the task, so the worker refuses before submitting anything.
     */
    public function testTheWorkerSubmitsTheSignalOnlyWhenTheRuntimeAdvertisesIt(): void
    {
        foreach ([false, true] as $supported) {
            [$commands, $failure] = $this->tick(['signal_external_workflow' => $supported], []);
            if ($supported) {
                self::assertNull($failure);
                self::assertSame('signal_external_workflow', $commands[0]['type'] ?? null);
            } else {
                self::assertNull($commands, 'nothing is submitted without the advertised capability');
                self::assertStringContainsString('signal_external_workflow_unavailable', (string) $failure);
            }
        }
    }

    public function testAReplayedSignalNeedsNoCapability(): void
    {
        [$commands, $failure] = $this->tick([], $this->recorded($this->delivered()));

        self::assertNull($failure);
        self::assertSame(['complete_workflow'], array_column($commands, 'type'));
    }

    /**
     * @param array<string, mixed> $capabilities
     * @param list<array<string, mixed>> $history
     * @return array{?list<array<string, mixed>>, ?string}
     */
    private function tick(array $capabilities, array $history): array
    {
        $commands = null;
        $failure = null;
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($capabilities, $history, &$commands, &$failure): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['server_capabilities' => $capabilities,
                    'poll_status' => 'leased', 'task' => ['task_id' => 'sender-task', 'workflow_task_attempt' => 1,
                        'lease_owner' => 'worker-1', 'workflow_id' => 'sender', 'run_id' => 'sender-run',
                        'workflow_type' => 'orders.sender', 'payload_codec' => 'avro', 'history_events' => $history]];
            }
            if (str_ends_with($uri, '/sender-task/heartbeat')) {
                return ['task_id' => 'sender-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true];
            }
            if (str_ends_with($uri, '/sender-task/complete')) {
                $commands = $body['commands'];

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/sender-task/fail')) {
                $failure = $body['failure']['message'] ?? null;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            self::fail("Unexpected request {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'senders', workerId: 'worker-1');
        $worker->registerWorkflow('orders.sender', static fn (WorkflowContext $context): string
            => $context->signalExternalWorkflow('order-42', 'approve', ['Ada', 3])->outcome->value);

        self::assertTrue($worker->tick(0));

        return [$commands, $failure];
    }

    /** @param list<array<string, mixed>> $history
     *  @return list<array<string, mixed>>
     */
    private function replay(array $history): array
    {
        $workflow = static function (WorkflowContext $context): array {
            $result = $context->signalExternalWorkflow('order-42', 'approve', ['Ada', 3]);

            return [
                'outcome' => $result->outcome->value, 'delivered' => $result->delivered(), 'reason' => $result->reason,
                'target_workflow_id' => $result->targetWorkflowId, 'target_run_id' => $result->targetRunId,
                'target_command_id' => $result->targetCommandId, 'target_signal_id' => $result->targetSignalId,
                'retained' => $result->retained,
            ];
        };

        return (new Replayer($this->codec))->replay($workflow, $history, [], 'php-workers')->commands;
    }

    /** @return array<string, mixed> */
    private function identity(): array
    {
        return ['sequence' => 1, 'target_workflow_instance_id' => 'order-42', 'target_workflow_run_id' => null,
            'signal_name' => 'approve', 'request_id' => 'workflow-signal:sender-run:1'];
    }

    /** @return array<string, mixed> */
    private function delivered(): array
    {
        return ['event_type' => 'ExternalSignalDelivered', 'payload' => [
            ...$this->identity(), 'target_workflow_run_id' => 'order-42-run-1', 'outcome' => 'delivered',
            'target_command_id' => 'cmd-9', 'target_signal_id' => 'sig-9', 'retained' => false,
        ]];
    }

    /**
     * @param array<string, mixed> $resolution
     * @return list<array<string, mixed>>
     */
    private function recorded(array $resolution): array
    {
        return [
            ['event_type' => 'ExternalSignalRequested', 'payload' => [
                ...$this->identity(), 'arguments' => $this->codec->envelope(['Ada', 3]), 'payload_codec' => 'avro',
                'task' => ['id' => 'sender-task'],
            ]],
            $resolution,
        ];
    }
}
