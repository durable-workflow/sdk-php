<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationReplayTest extends TestCase
{
    public function testObservedRequestProducesIntentWithoutThrowingOrStartingTheCall(): void
    {
        $seen = null;
        $caught = false;
        $result = $this->replay(static function (WorkflowContext $context) use (&$seen, &$caught): void {
            $seen = $context->isCancellationRequested();
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled) {
                $caught = true;
            }
        }, [], ['cancellation_request' => self::observation()]);

        self::assertFalse($seen);
        self::assertFalse($caught);
        self::assertSame([], $result->commands);
        self::assertSame('timer', $result->cancellationDelivery?->callKind);
        self::assertSame('request-1', $result->cancellationDelivery?->requestId);
        self::assertSame(1, $result->cancellationDelivery?->sequence);
    }

    public function testDeliveryRunsFinallyCleanupAndColdReplayUsesItsCommittedResult(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            try {
                $context->sleep(10);
                return 'not cancelled';
            } catch (WorkflowCancelled $cancelled) {
                return (string) $cancelled->requestId;
            } finally {
                $context->cancellationShield(static function () use ($context): void {
                    $context->throwIfCancellationRequested();
                    $context->activity('cleanup');
                });
            }
        };
        $history = [self::request(), self::delivery()];
        $first = $this->replay($workflow, $history);
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));
        self::assertSame('cleanup', $first->commands[0]['activity_type']);
        self::assertNull($first->cancellationDelivery);
        self::assertSame($first->commands, $this->replay($workflow, $history)->commands);

        $history[] = $this->completedActivity(2, 'cleanup', 'cleaned');
        $finished = $this->replay($workflow, $history);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame('request-1', $this->codec()->decodeEnvelope($finished->commands[0]['result']));
    }

    public function testExplicitCheckAfterDeliveredCleanupKeepsOriginalRequestIdentity(): void
    {
        $workflow = static function (WorkflowContext $context): void {
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled) {
                $context->cancellationShield(static fn () => $context->throwIfCancellationRequested());
                $context->throwIfCancellationRequested();
            }
        };
        try {
            $this->replay($workflow, [self::request(), self::delivery()]);
            self::fail('The explicit check must preserve delivered cancellation.');
        } catch (WorkflowCancelled $error) {
            self::assertSame('request-1', $error->requestId);
        }
    }

    public function testCompletionBeforeRequestWinsAndNextCallReceivesIntent(): void
    {
        $seen = null;
        $result = $this->replay(static function (WorkflowContext $context) use (&$seen): void {
            $seen = $context->activity('first');
            $context->sleep(10);
        }, [$this->completedActivity(1, 'first', 'acknowledged'), self::request()]);

        self::assertSame('acknowledged', $seen);
        self::assertSame(2, $result->cancellationDelivery?->sequence);
        self::assertSame([], $result->commands);
    }

    public function testEarlierActivityFailureIsDeliveredBeforeCancellationIntent(): void
    {
        $seen = null;
        $result = $this->replay(static function (WorkflowContext $context) use (&$seen): void {
            try {
                $context->activity('first');
            } catch (ActivityFailed $failed) {
                $seen = $failed->getMessage();
            }
            $context->sleep(10);
        }, [self::event('ActivityFailed', ['sequence' => 1, 'activity_type' => 'first', 'message' => 'original failure']), self::request()]);

        self::assertSame('original failure', $seen);
        self::assertSame(2, $result->cancellationDelivery?->sequence);
    }

    public function testPendingMetadataIsRetainedBeforeDeliveryCanBeCommitted(): void
    {
        $result = $this->replay(static function (WorkflowContext $context): void {
            $context->sideEffect(static fn () => 'once');
            $context->sleep(10);
        }, [self::request()]);

        self::assertSame(['record_side_effect'], array_column($result->commands, 'type'));
        self::assertSame(2, $result->cancellationDelivery?->sequence);
    }

    public function testNestedShieldsDeferPendingCancellationAndRestoreDepthAfterFailure(): void
    {
        $workflow = static function (WorkflowContext $context): void {
            try {
                $context->cancellationShield(static function () use ($context): void {
                    $context->cancellationShield(static fn () => $context->activity('cleanup'));
                    throw new \RuntimeException('cleanup finished');
                });
            } catch (\RuntimeException) {
            }
            $context->sleep(10);
        };
        $history = [self::request()];
        $first = $this->replay($workflow, $history);
        self::assertNull($first->cancellationDelivery);
        self::assertSame('cleanup', $first->commands[0]['activity_type']);
        $history[] = $this->completedActivity(1, 'cleanup', 'cleaned');
        $next = $this->replay($workflow, $history);
        self::assertSame('timer', $next->cancellationDelivery?->callKind);
        self::assertSame(2, $next->cancellationDelivery?->sequence);
    }

    #[DataProvider('changedBoundaryProvider')]
    public function testChangedCommittedBoundaryIsRejected(callable $workflow, array $history): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($workflow, $history);
    }

    public static function changedBoundaryProvider(): array
    {
        return [
            'changed kind' => [static fn (WorkflowContext $context) => $context->activity('different'), [self::request(), self::delivery()]],
            'removed call' => [static fn () => 'done', [self::request(), self::delivery()]],
            'shielded committed call' => [static fn (WorkflowContext $context) => $context->cancellationShield(static fn () => $context->sleep(10)), [self::request(), self::delivery()]],
            'changed timer duration' => [static fn (WorkflowContext $context) => $context->sleep(20), [self::event('TimerScheduled', ['sequence' => 1, 'delay_seconds' => 10]), self::request(), self::delivery()]],
            'changed activity name' => [static fn (WorkflowContext $context) => $context->activity('different'), [self::event('ActivityScheduled', ['sequence' => 1, 'activity_type' => 'original']), self::request(), self::delivery(1, 'activity')]],
            'changed parallel span' => [static fn (WorkflowContext $context) => $context->all([$context->deferTimer(10)]), [self::request(), self::delivery(1, 'parallel', 2)]],
            'removed earlier call' => [static fn (WorkflowContext $context) => $context->sleep(10), [self::event('ActivityCompleted', ['sequence' => 1, 'activity_type' => 'original', 'result' => (new AvroPayloadCodec())->envelope('done')]), self::request(), self::delivery(2)]],
        ];
    }

    public function testReopenedConditionUsesPhysicalDeliverySequenceAndCleanupSequence(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            try {
                $context->waitCondition(static fn () => false, 'ready');
            } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->activity('cleanup'));
                return (string) $cancelled->requestId;
            }
            return 'not cancelled';
        };
        $history = [
            self::event('ConditionWaitOpened', ['sequence' => 1, 'condition_key' => 'ready', 'condition_wait_id' => 'wait-1']),
            self::event('ConditionWaitSatisfied', ['sequence' => 1, 'condition_wait_id' => 'wait-1']),
            self::event('ConditionWaitOpened', ['sequence' => 2, 'condition_key' => 'ready', 'condition_wait_id' => 'wait-2']),
            self::request(), self::delivery(2, 'condition'),
            $this->completedActivity(3, 'cleanup', 'cleaned'),
        ];
        $result = $this->replay($workflow, $history);
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('request-1', $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testSatisfiedNewConditionDoesNotConsumeACancellationBoundary(): void
    {
        $result = $this->replay(static function (WorkflowContext $context): void {
            $context->waitCondition(static fn () => true, 'already-ready');
            $context->sleep(10);
        }, [self::request()]);
        self::assertSame('timer', $result->cancellationDelivery?->callKind);
        self::assertSame(1, $result->cancellationDelivery?->sequence);
    }

    public function testParallelDeliveryConsumesItsFullSpanBeforeCleanup(): void
    {
        $workflow = static function (WorkflowContext $context): string {
            try {
                $context->all([$context->deferTimer(10), $context->deferActivity('work')]);
            } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->activity('cleanup'));
                return (string) $cancelled->requestId;
            }
            return 'not cancelled';
        };
        $result = $this->replay($workflow, [self::request(), self::delivery(1, 'parallel', 2), $this->completedActivity(3, 'cleanup', 'cleaned')]);
        self::assertSame('request-1', $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testPendingCancellationDoesNotExecuteLocalActivity(): void
    {
        $calls = 0;
        $result = (new Replayer($this->codec()))->replay(
            static fn (WorkflowContext $context) => $context->localActivity('side-effect'),
            [self::request()], [], 'php-workers', ['run_id' => 'run-1'],
            static function () use (&$calls): array { ++$calls; return ['outcome' => 'completed', 'result' => 'wrong']; },
        );
        self::assertSame(0, $calls);
        self::assertSame('local_activity', $result->cancellationDelivery?->callKind);
        self::assertSame([], $result->commands);
    }

    public function testSelectionWinnerSurvivesAndPendingLoserHandleUsesItsOwnRange(): void
    {
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/durable-selection-runtime-history.json'), true, flags: JSON_THROW_ON_ERROR);
        $history = array_values(array_filter($fixture['history'], static fn (array $event): bool => !(
            $event['event_type'] === 'ActivityCompleted' && $event['payload']['sequence'] === 1
        )));
        $winner = null;
        $workflow = static function (WorkflowContext $context) use (&$winner): string {
            $selected = $context->select([
                'slow' => static fn () => $context->activity('slow-activity'),
                'fast' => static fn () => $context->activity('fast-activity'),
            ]);
            $winner = $selected->result();
            try {
                $selected->handles['slow']->await();
            } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->activity('cleanup'));
                return (string) $cancelled->requestId;
            }
            return 'not cancelled';
        };
        $history[] = self::request();
        $intent = $this->replay($workflow, $history)->cancellationDelivery;
        self::assertSame('winner-value', $winner);
        self::assertSame('selection_handle', $intent?->callKind);
        self::assertSame(3, $intent?->sequence);
        self::assertSame(1, $intent?->operationSequence);
        self::assertSame(1, $intent?->operationSequenceSpan);
        $history[] = self::event('CooperativeCancellationDelivered', [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'sequence' => 3,
            'call_kind' => 'selection_handle', 'operation_sequence' => 1, 'operation_sequence_span' => 1,
        ]);
        $history[] = $this->completedActivity(4, 'cleanup', 'cleaned');
        $finished = $this->replay($workflow, $history);
        self::assertSame('request-1', $this->codec()->decodeEnvelope($finished->commands[0]['result']));
    }

    public function testSagaCompensationShieldsAnEarlierFailureFromPendingCancellation(): void
    {
        $workflow = static function (WorkflowContext $context): void {
            try {
                $context->saga()->run(static function (Saga $saga) use ($context): void {
                    $context->activity('reserve');
                    $saga->addCompensation('undo');
                    throw new \RuntimeException('original failure');
                });
            } catch (\RuntimeException) {
            }
            $context->sleep(10);
        };
        $history = [$this->completedActivity(1, 'reserve', 'reserved'), self::request()];
        $first = $this->replay($workflow, $history);
        self::assertNull($first->cancellationDelivery);
        self::assertSame('undo', $first->commands[0]['activity_type']);
        $history[] = $this->completedActivity(2, 'undo', 'undone');
        $next = $this->replay($workflow, $history);
        self::assertSame(3, $next->cancellationDelivery?->sequence);
    }

    private function replay(callable $workflow, array $history, array $task = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer($this->codec()))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'run-1', ...$task]);
    }

    private function codec(): AvroPayloadCodec
    {
        return new AvroPayloadCodec();
    }

    private function completedActivity(int $sequence, string $name, mixed $value): array
    {
        return self::event('ActivityCompleted', ['sequence' => $sequence, 'activity_type' => $name, 'result' => $this->codec()->envelope($value)]);
    }

    private static function event(string $type, array $payload): array
    {
        return ['event_type' => $type, 'payload' => $payload];
    }

    private static function observation(): array
    {
        return ['request_id' => 'request-1', 'requested_at' => '2026-10-01T00:00:00Z', 'cleanup_deadline_at' => '2026-10-01T00:01:00Z', 'history_refresh_page_token' => 'opaque-first-page'];
    }

    private static function request(): array
    {
        return ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-10-01T00:00:00Z', 'payload' => ['workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'cleanup_deadline_at' => '2026-10-01T00:01:00Z']];
    }

    private static function delivery(int $sequence = 1, string $kind = 'timer', int $span = 1): array
    {
        return self::event('CooperativeCancellationDelivered', ['workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'sequence' => $sequence, 'call_kind' => $kind, 'sequence_span' => $span]);
    }
}
