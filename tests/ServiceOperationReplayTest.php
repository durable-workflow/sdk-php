<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ServiceOperationFailed;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ServiceOperationReplayTest extends TestCase
{
    public function testFreshCallEmitsOneDurableCommandWithOriginalIdentityAndTypedRequest(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context) => $context->serviceOperation('billing', 'invoices', 'create',
            ['invoice_id' => 42], new ServiceOperationOptions(waitTimeoutSeconds: 30));
        $first = (new Replayer($codec))->replay($workflow, [], [], 'callers', self::task());
        $cold = (new Replayer($codec))->replay($workflow, [], [], 'callers', self::task());
        self::assertSame($first->commands, $cold->commands);
        self::assertCount(1, $first->commands);
        $command = $first->commands[0];
        self::assertSame('start_service_operation', $command['type']);
        self::assertSame(['invoice_id' => 42], $codec->decodeEnvelope($command['request_payload']));
        self::assertSame(['caller_sdk_language' => 'sdk-php'], $command['metadata']);
        self::assertSame('completed', $command['wait_for']);
        self::assertSame(30, $command['wait_timeout_seconds']);
        $otherRun = (new Replayer($codec))->replay($workflow, [], [], 'callers', ['workflow_id' => 'caller', 'run_id' => 'other']);
        self::assertNotSame($command['idempotency_key'], $otherRun->commands[0]['idempotency_key']);
    }

    public function testStartedCompletionWaitSuspendsAcrossColdReplaysWithoutResubmitting(): void
    {
        foreach (range(1, 3) as $_) {
            self::assertSame([], $this->replay([self::event('ServiceCallStarted')])->commands);
        }
    }

    /** @param mixed $value */
    #[DataProvider('results')]
    public function testTerminalTypedResultResumesTheOriginalCall(mixed $value): void
    {
        $codec = new AvroPayloadCodec();
        $terminal = self::event('ServiceCallCompleted', ['response_payload' => $codec->envelope($value)]);
        foreach ([$terminal, self::event('ServiceCallStarted')] as $first) {
            $history = $first === $terminal ? [$terminal] : [$first, $terminal];
            foreach (range(1, 2) as $_) {
                $result = $this->replay($history);
                self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
                self::assertSame($codec->encode($value), $result->commands[0]['result']['blob']);
            }
        }
    }

    /** @return iterable<string, array{mixed}> */
    public static function results(): iterable
    {
        foreach (['null' => null, 'false' => false, 'zero' => 0, 'empty text' => '', 'empty list' => [],
            'map' => ['invoice_id' => 42], 'empty map' => AvroMapValue::fromPairs([]),
            'bytes' => AvroBinaryValue::fromBytes("\x00\xff")] as $name => $value) {
            yield $name => [$value];
        }
    }

    public function testCanonicalResponseOverridesAnEmptyServiceSnapshotAndRawAvroDecodes(): void
    {
        $codec = new AvroPayloadCodec();
        $result = $this->replay([self::event('ServiceCallCompleted', [
            'response_payload' => $codec->encode('recorded'),
            'service_call' => ['id' => 'call-1', 'status' => 'completed', 'result' => null],
        ])]);
        self::assertSame('recorded', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    #[DataProvider('failures')]
    public function testTypedFailureAndCancellationAreDeliveredAtTheCall(string $event, bool $cancelled): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            try {
                $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42], ['idempotency_key' => 'call-key']);
            } catch (ServiceOperationFailed $failure) {
                return [$failure->serviceCallId, $failure->failureType, $failure->getMessage(), $failure->cancelled];
            }
            throw new \RuntimeException('Recorded failure was not delivered.');
        };
        $result = (new Replayer($codec))->replay($workflow, [self::event('ServiceCallStarted'), self::event($event, [
            'exception_type' => 'invoice_declined', 'message' => 'Declined',
        ])], [], 'callers', self::task());
        self::assertSame(['call-1', 'invoice_declined', 'Declined', $cancelled], $codec->decodeEnvelope($result->commands[0]['result']));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function failures(): iterable
    {
        yield 'failed' => ['ServiceCallFailed', false];
        yield 'cancelled' => ['ServiceCallCancelled', true];
    }

    public function testAcceptedCallAlwaysReplaysItsOriginalAdmissionEvenAfterTerminalFailure(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            $call = $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42],
                ['wait_for' => 'accepted', 'idempotency_key' => 'call-key']);
            return [$call->serviceCallId, $call->status, $call->responsePayload];
        };
        $start = self::event('ServiceCallStarted', ['wait_for' => 'accepted']);
        foreach ([[$start], [$start, self::event('ServiceCallFailed', ['wait_for' => 'accepted'])]] as $history) {
            $result = (new Replayer($codec))->replay($workflow, $history, [], 'callers', self::task());
            self::assertSame(['call-1', 'started', null], $codec->decodeEnvelope($result->commands[0]['result']));
        }
    }

    /** @param array<string, mixed> $changes */
    #[DataProvider('intentChanges')]
    public function testChangedRecordedIntentIsRejected(array $changes): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay([self::event('ServiceCallStarted', $changes)]);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function intentChanges(): iterable
    {
        foreach (['endpoint_name' => 'other', 'service_name' => 'other', 'operation_name' => 'other',
            'wait_for' => 'accepted', 'operation_mode' => 'async', 'idempotency_key' => 'other',
            'request_payload' => (new AvroPayloadCodec())->envelope(['invoice_id' => 43])] as $key => $value) {
            yield $key => [[$key => $value]];
        }
    }

    public function testAnotherCallOrContradictoryTerminalCannotResolveTheOriginalSequence(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay([self::event('ServiceCallStarted'), self::event('ServiceCallCompleted', ['service_call_id' => 'another'])]);
    }

    public function testDuplicateTerminalReplaysOnce(): void
    {
        $terminal = self::event('ServiceCallCompleted', ['response_payload' => (new AvroPayloadCodec())->envelope(42)]);
        self::assertCount(1, $this->replay([self::event('ServiceCallStarted'), $terminal, $terminal])->commands);
    }

    public function testTerminalCannotCollideWithAnActivitySequence(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay([self::event('ServiceCallStarted'), ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1]]]);
    }

    /** @param list<array<string, mixed>> $history */
    private function replay(array $history): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context): mixed {
            $call = $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42], ['idempotency_key' => 'call-key']);
            return $call->responsePayload;
        }, $history, [], 'callers', self::task());
    }

    /** @return array<string, mixed> */
    private static function task(): array
    {
        return ['workflow_id' => 'caller', 'run_id' => 'caller-run'];
    }

    /** @param array<string, mixed> $changes @return array<string, mixed> */
    private static function event(string $type, array $changes = []): array
    {
        return ['event_type' => $type, 'payload' => array_replace([
            'sequence' => 1, 'service_call_id' => 'call-1', 'endpoint_name' => 'billing',
            'service_name' => 'invoices', 'operation_name' => 'create', 'operation_mode' => 'sync',
            'wait_for' => 'completed', 'idempotency_key' => 'call-key',
            'request_payload' => (new AvroPayloadCodec())->envelope(['invoice_id' => 42]),
            'service_call' => ['id' => 'call-1', 'result' => null],
        ], $changes)];
    }
}
