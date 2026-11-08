<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\ConditionWaitNativeEvidence as Evidence;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/Support/ConditionWaitNativeEvidence.php';

/**
 * Native Server 3.2.0 history shapes for #269: each open is a new
 * condition_wait_id; a condition timeout is a TimerScheduled with
 * timer_kind condition_timeout carrying the wait id; a timeout ends with
 * TimerFired (no ConditionWaitTimedOut).
 */
final class ConditionWaitNativeEvidenceTest extends TestCase
{
    private const T0 = 1_794_000_000.0;

    public function testAFalseWakeThatKeepsTheFirstDeadlinePasses(): void
    {
        $summary = Evidence::falseWake(Evidence::events(self::falseWake(15)), 20);
        self::assertSame(['first_timeout' => 20, 'reopen_timeout' => 15, 'remaining_at_wake' => 15.0,
            'fired_after_first_open' => 20.0], $summary);
    }

    /** The original #269 defect: the reopen restarts the full timeout and the deadline moves. */
    public function testAReopenWithTheFullTimeoutIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_reopen_budget');
        Evidence::falseWake(Evidence::events(self::falseWake(20, firedAt: 25.0)), 20);
    }

    public function testATimeoutThatFiresAfterTheFirstDeadlineIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_deadline_moved');
        Evidence::falseWake(Evidence::events(self::falseWake(15, firedAt: 23.5)), 20);
    }

    public function testAFalseWakeWithoutASignalIsRefused(): void
    {
        $history = array_values(array_filter(self::falseWake(15), static fn (array $e): bool => $e['event_type'] !== 'SignalReceived'));
        $this->expectExceptionMessage('condition_evidence_false_wake_signal');
        Evidence::falseWake(Evidence::events($history), 20);
    }

    public function testASatisfiedWakeMustCancelItsTimer(): void
    {
        $history = array_values(array_filter(self::falseWake(15), static fn (array $e): bool => $e['event_type'] !== 'TimerCancelled'));
        $this->expectExceptionMessage('condition_evidence_false_wake_resolution');
        Evidence::falseWake(Evidence::events($history), 20);
    }

    public function testASettledRunPasses(): void
    {
        self::assertSame(['waits' => 1, 'satisfied' => 1], Evidence::settledRun(Evidence::events(self::duplicate(false))));
    }

    /** The second #269 defect: a replayed task opens a second physical wait while the first is unresolved. */
    public function testAnOverlappingDuplicateOpenIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_overlapping_open');
        Evidence::settledRun(Evidence::events(self::duplicate(true)));
    }

    public function testAnUnresolvedWaitInACompletedRunIsRefused(): void
    {
        $history = [self::opened(1.0, 'w1', 'go', 60), self::scheduled(1.0, 't1', 'w1', 60), self::event(3.0, 'WorkflowCompleted')];
        $this->expectExceptionMessage('condition_evidence_unresolved_wait');
        Evidence::settledRun(Evidence::events($history));
    }

    public function testTerminateAndCancelCancelTheOpenConditionTimer(): void
    {
        foreach (['terminate' => 'WorkflowTerminated', 'cancel' => 'WorkflowCancelled'] as $close => $type) {
            $history = [self::opened(1.0, 'w1', 'k', 300), self::scheduled(1.0, 't1', 'w1', 300),
                self::event(4.0, $type), self::event(4.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1])];
            self::assertSame(['waits' => 1, 'close' => $type], Evidence::closedWithOpenWait(Evidence::events($history), $close));
        }
    }

    public function testATerminateWithoutTimerCancellationIsRefused(): void
    {
        $history = [self::opened(1.0, 'w1', 'k', 300), self::scheduled(1.0, 't1', 'w1', 300), self::event(4.0, 'WorkflowTerminated')];
        $this->expectExceptionMessage('condition_evidence_close_timer_not_cancelled');
        Evidence::closedWithOpenWait(Evidence::events($history), 'terminate');
    }

    public function testContinueAsNewAfterAFiredTimeoutPasses(): void
    {
        $history = [self::opened(1.0, 'w1', 'k', 2), self::scheduled(1.0, 't1', 'w1', 2),
            self::event(3.0, 'TimerFired', ['timer_id' => 't1', 'sequence' => 1]), self::event(3.2, 'WorkflowContinuedAsNew')];
        self::assertSame(['waits' => 1, 'close' => 'WorkflowContinuedAsNew'], Evidence::closedWithOpenWait(Evidence::events($history), 'continue'));
    }

    public function testANewlyAuthoredWaitKeepsItsOwnBudget(): void
    {
        self::assertSame(['second_timeout' => 20, 'reopen_timeout' => 15, 'remaining_at_wake' => 15.0],
            Evidence::ownBudget(Evidence::events(self::ownBudget(15)), 'first', 'second', 20));
    }

    /** #308: a new authored wait must not inherit the earlier wait's budget. */
    public function testANewWaitThatInheritsTheEarlierBudgetIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_own_budget');
        Evidence::ownBudget(Evidence::events(self::ownBudget(27)), 'first', 'second', 20);
    }

    public function testMalformedHistoryIsRefused(): void
    {
        $this->expectException(RuntimeException::class);
        Evidence::events([['event_type' => 'ConditionWaitOpened']]);
    }

    /** @return list<array<string, mixed>> */
    private static function falseWake(int $reopenTimeout, float $firedAt = 20.0): array
    {
        return [
            self::opened(0.0, 'w1', 'go', 20), self::scheduled(0.0, 't1', 'w1', 20),
            self::event(5.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(5.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]),
            self::opened(5.0, 'w2', 'go', $reopenTimeout, 2), self::scheduled(5.0, 't2', 'w2', $reopenTimeout, 2),
            self::event($firedAt, 'TimerFired', ['timer_id' => 't2', 'sequence' => 2]),
            self::event($firedAt + 0.2, 'WorkflowCompleted'),
        ];
    }

    /** @return list<array<string, mixed>> */
    private static function duplicate(bool $overlap): array
    {
        $history = [self::event(0.0, 'ActivityScheduled'), self::event(0.5, 'SignalReceived', ['signal_name' => 'noise']),
            self::event(2.0, 'ActivityCompleted'), self::opened(2.1, 'w1', 'go', 60), self::scheduled(2.1, 't1', 'w1', 60)];
        if ($overlap) {
            $history[] = self::opened(2.12, 'w2', 'go', 60, 2);
        }
        return [...$history, self::event(3.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(3.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(3.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]), self::event(3.1, 'WorkflowCompleted')];
    }

    /** @return list<array<string, mixed>> */
    private static function ownBudget(int $reopenTimeout): array
    {
        return [
            self::opened(0.0, 'a1', 'first', 30), self::scheduled(0.0, 'ta', 'a1', 30),
            self::event(10.0, 'SignalReceived', ['signal_name' => 'first']),
            self::event(10.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'a1', 'sequence' => 1]),
            self::event(10.0, 'TimerCancelled', ['timer_id' => 'ta', 'sequence' => 1]),
            self::opened(12.0, 'b1', 'second', 20, 2), self::scheduled(12.0, 'tb', 'b1', 20, 2),
            self::event(17.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event(17.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b1', 'sequence' => 2]),
            self::event(17.0, 'TimerCancelled', ['timer_id' => 'tb', 'sequence' => 2]),
            self::opened(17.0, 'b2', 'second', $reopenTimeout, 3), self::scheduled(17.0, 'tc', 'b2', $reopenTimeout, 3),
            self::event(19.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event(19.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b2', 'sequence' => 3]),
            self::event(19.0, 'TimerCancelled', ['timer_id' => 'tc', 'sequence' => 3]),
            self::event(19.1, 'WorkflowCompleted'),
        ];
    }

    /** @return array<string, mixed> */
    private static function opened(float $at, string $id, string $key, int $timeout, int $sequence = 1): array
    {
        return self::event($at, 'ConditionWaitOpened', ['condition_wait_id' => $id, 'condition_key' => $key,
            'timeout_seconds' => $timeout, 'sequence' => $sequence]);
    }

    /** @return array<string, mixed> */
    private static function scheduled(float $at, string $timer, string $wait, int $seconds, int $sequence = 1): array
    {
        return self::event($at, 'TimerScheduled', ['timer_id' => $timer, 'sequence' => $sequence, 'delay_seconds' => $seconds,
            'timer_kind' => 'condition_timeout', 'condition_wait_id' => $wait]);
    }

    /** @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function event(float $at, string $type, array $payload = []): array
    {
        $stamp = \DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', self::T0 + $at))->format('Y-m-d\TH:i:s.uP');

        return ['event_type' => $type, 'recorded_at' => $stamp, 'payload' => $payload];
    }
}
