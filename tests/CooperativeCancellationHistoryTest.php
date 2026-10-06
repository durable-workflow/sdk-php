<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\CancellationDelivery;
use DurableWorkflow\Worker\CancellationHistory;
use DurableWorkflow\Worker\CancellationRequest;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationHistoryTest extends TestCase
{
    public function testObservationNeverFabricatesADelivery(): void
    {
        $observation = self::observation();
        $state = CancellationHistory::fromEvents([], 'run-1', $observation);
        self::assertSame($observation, $state->request);
        self::assertNull($state->delivery);
        self::assertTrue($state->eligible(1));
        $state = CancellationHistory::fromEvents([self::request()], 'run-1', $observation);
        self::assertSame($observation, $state->request);
        self::assertNull($state->delivery);
    }

    public function testColdCanonicalHistoryPreservesOriginalIdentityAndCallBoundary(): void
    {
        $state = CancellationHistory::fromEvents([self::request(), self::delivery()], 'run-1');
        self::assertSame('request-1', $state->request?->requestId);
        self::assertSame('2026-10-01T00:01:00.123456Z', $state->request?->cleanupDeadlineAt);
        self::assertNull($state->request?->historyRefreshPageToken);
        self::assertSame(0, $state->requestIndex);
        self::assertSame(1, $state->deliveryIndex);
        self::assertSame('timer', $state->delivery?->callKind);
        self::assertTrue($state->delivery?->interrupts(1));
        self::assertFalse($state->delivery?->interrupts(2));
        self::assertFalse($state->eligible(2));
    }

    public function testDeadlineComparisonUsesTheOriginalInstantAcrossTimezoneRepresentations(): void
    {
        $request = self::request();
        $request['payload']['cleanup_deadline_at'] = '2026-09-30T20:01:00.123456-04:00';
        $state = CancellationHistory::fromEvents([$request], 'run-1', self::observation());
        self::assertSame('2026-10-01T00:01:00.123456Z', $state->request?->cleanupDeadlineAt);
        self::assertSame('opaque-first-page', $state->request?->historyRefreshPageToken);
    }

    #[DataProvider('invalidHistoryProvider')]
    public function testInvalidCanonicalHistoryIsRejected(array $history): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        CancellationHistory::fromEvents($history, 'run-1');
    }

    public static function invalidHistoryProvider(): array
    {
        $cases = [
            'delivery without request' => [[self::delivery()]],
            'duplicate request' => [[self::request(), self::request()]],
            'duplicate delivery' => [[self::request(), self::delivery(), self::delivery()]],
        ];
        foreach ([
            'missing payload' => ['payload' => null],
            'event identity' => ['workflow_command_id' => 'other'],
            'missing timestamp' => ['recorded_at' => null],
        ] as $label => $change) {
            $cases[$label] = [[[...self::request(), ...$change]]];
        }
        foreach ([
            'workflow_run_id' => 'other', 'workflow_command_id' => '',
            'cleanup_deadline_at' => '2026-10-01T00:00:00.123456Z',
        ] as $key => $value) {
            $request = self::request();
            $request['payload'][$key] = $value;
            $cases[$key] = [[$request]];
        }
        $wrongRequest = self::delivery();
        $wrongRequest['payload']['workflow_command_id'] = 'other';
        $wrongRequest['workflow_command_id'] = 'other';
        $cases['wrong delivery request'] = [[self::request(), $wrongRequest]];
        $wrongRun = self::delivery();
        $wrongRun['payload']['workflow_run_id'] = 'other';
        $cases['wrong delivery run'] = [[self::request(), $wrongRun]];

        return $cases;
    }

    public function testObservationCannotReplaceTheOriginalRequestOrDeadline(): void
    {
        foreach (['request_id' => 'other', 'cleanup_deadline_at' => '2026-10-01T00:02:00Z'] as $key => $value) {
            $data = self::observationData();
            $data[$key] = $value;
            try {
                CancellationHistory::fromEvents([self::request()], 'run-1', CancellationRequest::fromObservation($data));
                self::fail('Expected immutable observation rejection.');
            } catch (NonDeterministicWorkflow $error) {
                self::assertSame('invalid_cooperative_cancellation_history', $error->reason);
            }
        }
    }

    #[DataProvider('earlierResolutionProvider')]
    public function testEarlierResultsRemainIneligibleForDelivery(string $kind): void
    {
        $history = [['event_type' => $kind, 'payload' => ['sequence' => 1]], self::request()];
        $state = CancellationHistory::fromEvents($history, 'run-1');
        self::assertFalse($state->eligible(1));
        self::assertTrue($state->eligible(2));
        $this->expectException(NonDeterministicWorkflow::class);
        CancellationHistory::fromEvents([...$history, self::delivery()], 'run-1');
    }

    public static function earlierResolutionProvider(): array
    {
        return array_map(static fn (string $kind): array => [$kind], [
            'ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled',
            'TimerFired', 'TimerCancelled', 'ConditionWaitSatisfied', 'ConditionWaitTimedOut',
            'SignalApplied', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated',
        ]);
    }

    public function testParallelDeliveryPreservesCompletedMembersAndCannotReplaceAnEarlierFailure(): void
    {
        $history = [['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1]], self::request()];
        self::assertTrue(CancellationHistory::fromEvents($history, 'run-1')->eligible(1, 2));
        $history[0]['event_type'] = 'ActivityFailed';
        self::assertFalse(CancellationHistory::fromEvents($history, 'run-1')->eligible(1, 2));
        $history[0]['event_type'] = 'ActivityCompleted';
        array_unshift($history, ['event_type' => 'TimerFired', 'payload' => ['sequence' => 2]]);
        self::assertFalse(CancellationHistory::fromEvents($history, 'run-1')->eligible(1, 2));
    }

    public function testEarlierSelectionWinnerAndCancellationCannotBeReplaced(): void
    {
        $history = [
            ['event_type' => 'SelectionResolved', 'payload' => ['selection_group_base_sequence' => 1, 'selection_group_size' => 2]],
            ['event_type' => 'SelectionOperationCancelled', 'payload' => ['member_base_sequence' => 3, 'member_size' => 2]],
            self::request(),
        ];
        $state = CancellationHistory::fromEvents($history, 'run-1');
        self::assertFalse($state->eligible(1, 2));
        self::assertFalse($state->eligible(3, 2));
        self::assertTrue($state->eligible(5));
    }

    public function testResolutionsAfterRequestDoNotPretendToPrecedeIt(): void
    {
        $history = [self::request(), ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1]]];
        self::assertTrue(CancellationHistory::fromEvents($history, 'run-1')->eligible(1));
    }

    public function testSelectionHandleDeliveryInterruptsItsEarlierOperationRange(): void
    {
        $delivery = CancellationDelivery::fromPayload([
            'workflow_command_id' => 'request-1', 'sequence' => 6, 'call_kind' => 'selection_handle',
            'operation_sequence' => 1, 'operation_sequence_span' => 3,
        ]);
        self::assertTrue($delivery->interrupts(1));
        self::assertTrue($delivery->interrupts(3));
        self::assertFalse($delivery->interrupts(4));
        self::assertFalse($delivery->interrupts(6));
    }

    #[DataProvider('invalidDeliveryProvider')]
    public function testInvalidDeliveryRangesAreRejected(array $change): void
    {
        $this->expectException(InvalidArgumentException::class);
        CancellationDelivery::fromPayload([...self::delivery()['payload'], ...$change]);
    }

    public static function invalidDeliveryProvider(): array
    {
        return array_map(static fn (array $change): array => [$change], [
            ['workflow_command_id' => ''], ['sequence' => 0], ['sequence' => '1'],
            ['sequence' => true], ['sequence' => 1.0], ['sequence' => PHP_INT_MAX],
            ['call_kind' => 'unknown'], ['sequence_span' => 2], ['sequence_span' => null],
            ['call_kind' => 'parallel', 'sequence_span' => 1001],
            ['operation_sequence' => 1], ['operation_sequence_span' => 2], ['operation_sequence_span' => null],
            ['call_kind' => 'selection_handle'],
            ['call_kind' => 'selection_handle', 'operation_sequence' => 1],
            ['call_kind' => 'selection_handle', 'sequence' => 3, 'operation_sequence' => 1, 'operation_sequence_span' => 3],
        ]);
    }

    public function testOrdinaryHistoryDoesNotEnableCooperativeDelivery(): void
    {
        $state = CancellationHistory::fromEvents([['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1]]]);
        self::assertNull($state->request);
        self::assertNull($state->delivery);
        self::assertFalse($state->eligible(1));
    }

    private static function observation(): CancellationRequest
    {
        return CancellationRequest::fromObservation(self::observationData());
    }

    private static function observationData(): array
    {
        return [
            'request_id' => 'request-1', 'requested_at' => '2026-10-01T00:00:00.123456Z',
            'cleanup_deadline_at' => '2026-10-01T00:01:00.123456Z', 'history_refresh_page_token' => 'opaque-first-page',
        ];
    }

    private static function request(): array
    {
        return [
            'event_type' => CancellationHistory::REQUEST_EVENT, 'workflow_command_id' => 'request-1',
            'recorded_at' => '2026-10-01T00:00:00.123456Z',
            'payload' => [
                'workflow_command_id' => 'request-1', 'workflow_run_id' => 'run-1',
                'cleanup_deadline_at' => '2026-10-01T00:01:00.123456Z',
            ],
        ];
    }

    private static function delivery(): array
    {
        return [
            'event_type' => CancellationHistory::DELIVERY_EVENT, 'workflow_command_id' => 'request-1',
            'payload' => ['workflow_command_id' => 'request-1', 'workflow_run_id' => 'run-1', 'sequence' => 1, 'call_kind' => 'timer'],
        ];
    }
}
