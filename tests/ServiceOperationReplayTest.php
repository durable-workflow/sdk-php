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

    public function testCanonicalOriginalServiceReferencesSurviveTerminalReplay(): void
    {
        $result = $this->replay(self::canonicalHistory());
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(['accepted' => true], (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    #[DataProvider('changedOriginalReferences')]
    public function testOriginalServiceReferencesCannotChangeOrDisappear(string $field, mixed $value, bool $omit): void
    {
        $history = self::canonicalHistory();
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            if ($omit) {
                unset($history[1]['payload'][$surface][$field]);
            } else {
                $history[1]['payload'][$surface][$field] = $value;
            }
        }
        if (array_key_exists($field, $history[1]['payload'])) {
            if ($omit) {
                unset($history[1]['payload'][$field]);
            } else {
                $history[1]['payload'][$field] = $value;
            }
        }
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage("Recorded service {$field} changed or was erased.");
        $this->replay($history);
    }

    /** @return iterable<string, array{string, mixed, bool}> */
    public static function changedOriginalReferences(): iterable
    {
        $changed = [
            'linked_workflow_instance_id' => 'other-target',
            'linked_workflow_run_id' => 'other-target-run',
            'linked_workflow_update_id' => 'other-update',
            'accepted_at' => '2026-10-13T02:01:00.000000Z',
            'call_deadline_at' => '2026-10-13T02:04:00.000000Z',
        ];
        foreach ($changed as $field => $value) {
            yield "{$field} changed" => [$field, $value, false];
            yield "{$field} erased" => [$field, null, false];
            yield "{$field} omitted" => [$field, null, true];
        }
    }

    public function testUnresolvedOriginalReferencesCanBeEnriched(): void
    {
        $history = self::canonicalHistory();
        foreach (['linked_workflow_instance_id', 'linked_workflow_run_id', 'linked_workflow_update_id',
            'accepted_at', 'call_deadline_at'] as $field) {
            $history[0]['payload']['service_call'][$field] = null;
            $history[0]['payload']['response_or_failure_surface'][$field] = null;
            if (array_key_exists($field, $history[0]['payload'])) {
                $history[0]['payload'][$field] = null;
            }
        }
        self::assertSame(['complete_workflow'], array_column($this->replay($history)->commands, 'type'));
    }

    public function testEquivalentOriginalTimestampRepresentationsAndDuplicateSnapshotsReplay(): void
    {
        $history = self::canonicalHistory();
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            $history[1]['payload'][$surface]['accepted_at'] = '2026-10-13T04:00:00+02:00';
            $history[1]['payload'][$surface]['call_deadline_at'] = '2026-10-13T02:03:00.000000000+00:00';
        }
        $duplicateAdmission = $history[0];
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            $duplicateAdmission['payload'][$surface]['accepted_at'] = '2026-10-13T02:00:00Z';
            $duplicateAdmission['payload'][$surface]['call_deadline_at'] = '2026-10-13T03:03:00.000+01:00';
        }
        $duplicateTerminal = $history[1];
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            $duplicateTerminal['payload'][$surface]['accepted_at'] = '2026-10-13T02:00:00.000Z';
            $duplicateTerminal['payload'][$surface]['call_deadline_at'] = '2026-10-13T02:03:00Z';
        }
        self::assertSame(['complete_workflow'], array_column($this->replay([
            $history[0], $duplicateAdmission, $history[1], $duplicateTerminal,
        ])->commands, 'type'));
    }

    public function testOriginalTimestampNormalizationPreservesSubsecondPrecision(): void
    {
        $history = self::canonicalHistory();
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            $history[0]['payload'][$surface]['call_deadline_at'] = '2026-10-13T02:03:00.123456Z';
            $history[1]['payload'][$surface]['call_deadline_at'] = '2026-10-13T03:03:00.123456000+01:00';
        }
        self::assertSame(['complete_workflow'], array_column($this->replay($history)->commands, 'type'));
        foreach (['service_call', 'response_or_failure_surface'] as $surface) {
            $history[1]['payload'][$surface]['call_deadline_at'] = '2026-10-13T02:03:00.123456001Z';
        }
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('Recorded service call_deadline_at changed or was erased.');
        $this->replay($history);
    }

    public function testCancellationOfACompletionWaitingCallProducesOnlyItsOriginalDeliveryIntent(): void
    {
        $requested = self::cancellationRequest();
        foreach ([[$requested], [self::event('ServiceCallStarted'), $requested]] as $history) {
            foreach (range(1, 3) as $_) {
                $result = $this->replay($history);
                self::assertSame([], $result->commands);
                self::assertSame('service_operation', $result->cancellationDelivery?->callKind);
                self::assertSame('cancel-1', $result->cancellationDelivery?->requestId);
                self::assertSame(1, $result->cancellationDelivery?->sequence);
            }
        }
    }

    public function testCanonicalDeliveryReplaysCallerCleanupWithoutRestartingTheService(): void
    {
        $workflow = static function (WorkflowContext $context): void {
            try {
                $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42], ['idempotency_key' => 'call-key']);
            } catch (\DurableWorkflow\Exception\WorkflowCancelled $cancel) {
                self::assertSame('cancel-1', $cancel->requestId);
                $context->sleep(1);
                throw $cancel;
            }
        };
        $history = [self::event('ServiceCallStarted'), self::cancellationRequest(),
            self::event('ServiceCallCancelled'), ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
                'workflow_run_id' => 'caller-run', 'workflow_command_id' => 'cancel-1', 'sequence' => 1,
                'call_kind' => 'service_operation',
            ]]];
        foreach (range(1, 3) as $_) {
            $result = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'callers', self::task());
            self::assertSame(['start_timer'], array_column($result->commands, 'type'));
            self::assertSame(1, $result->commands[0]['delay_seconds']);
            self::assertNull($result->cancellationDelivery);
        }
    }

    #[DataProvider('intentChanges')]
    public function testCancellationCannotHideAChangedOriginalCall(array $changes): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay([self::event('ServiceCallStarted', $changes), self::cancellationRequest()]);
    }

    private static function cancellationRequest(): array
    {
        return ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-10-12T08:00:02Z',
            'payload' => ['workflow_run_id' => 'caller-run', 'workflow_command_id' => 'cancel-1',
                'cleanup_deadline_at' => '2026-10-12T08:00:32Z']];
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

    /** @return list<array<string, mixed>> */
    private static function canonicalHistory(): array
    {
        $codec = new AvroPayloadCodec();
        $events = [];
        foreach (['ServiceCallStarted', 'ServiceCallCompleted'] as $type) {
            $surface = [
                'service_call_id' => 'call-1', 'namespace' => 'default',
                'caller_namespace' => 'default', 'target_namespace' => 'default',
                'endpoint_name' => 'billing', 'service_name' => 'invoices', 'operation_name' => 'create',
                'operation_mode' => 'sync',
                'status' => $type === 'ServiceCallStarted' ? 'accepted' : 'completed',
                'linked_workflow_instance_id' => 'target', 'linked_workflow_run_id' => 'target-run',
                'linked_workflow_update_id' => 'target-update',
                'accepted_at' => '2026-10-13T02:00:00.000000Z',
                'call_deadline_at' => '2026-10-13T02:03:00.000000Z',
            ];
            $events[] = ['event_type' => $type, 'payload' => [
                'sequence' => 1, 'service_call_id' => 'call-1',
                'workflow_instance_id' => 'caller', 'workflow_run_id' => 'caller-run',
                'caller_workflow_instance_id' => 'caller', 'caller_workflow_run_id' => 'caller-run',
                'endpoint_name' => 'billing', 'service_name' => 'invoices', 'operation_name' => 'create',
                'operation_mode' => 'sync', 'wait_for' => 'completed',
                'linked_workflow_instance_id' => 'target', 'linked_workflow_run_id' => 'target-run',
                'linked_workflow_update_id' => 'target-update',
                'request_payload' => $codec->envelope(['invoice_id' => 42]),
                'response_payload' => $type === 'ServiceCallCompleted' ? $codec->envelope(['accepted' => true]) : null,
                'service_call' => $surface, 'response_or_failure_surface' => $surface,
            ]];
        }

        return $events;
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
