<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\ConditionWaitAnchoredEvidence as Anchored;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/Support/ConditionWaitAnchoredEvidence.php';

/**
 * #269 anchored mode: the first stored timer anchor A on every reopen and
 * timeout timer, the original budget distinct from the authored remaining
 * time, and the authored time derived from the worker's actual delivery
 * witness (task ID + attempt), never from history recorded before completion.
 */
final class ConditionWaitAnchoredEvidenceTest extends TestCase
{
    private const T0 = 1_794_000_000.0;

    public function testAFalseWakeThatKeepsTheStoredAnchorPasses(): void
    {
        $summary = Anchored::falseWake(self::falseWake(), self::witness(), 'go', 20, 1);

        self::assertSame(self::stamp(20.0), $summary['anchor']);
        self::assertSame(20, $summary['budget']);
        self::assertSame([['task_id' => 'task-2', 'attempt' => 1, 'wake_event' => 'ConditionWaitSatisfied',
            'opened' => sprintf('%.6F', self::T0), 'now' => sprintf('%.6F', self::T0 + 5.0), 'authored' => 15, 'delivered_events' => 4]],
            $summary['reopens']);
        self::assertSame(0.4, $summary['fired_after_anchor']);
    }

    public function testRepeatedFalseWakesKeepTheSameAnchor(): void
    {
        $history = self::falseWake(second: true);
        $witness = [...self::witness(), Anchored::delivery('poll', 'task-3', 1, self::upTo($history, 10.0)),
            Anchored::completion('task-3', 1, [self::command(10)])];

        $summary = Anchored::falseWake($history, $witness, 'go', 20, 2);

        self::assertSame([15, 10], array_column($summary['reopens'], 'authored'));
    }

    /**
     * Causal: a signal recorded after the poll snapshot but before the reopen
     * was not delivered to that task. The witness keeps the worker's actual
     * remaining time (15); deriving visibility from history recorded before
     * the reopen would expect 14 and refuse a correct worker.
     */
    public function testALateArrivingUnseenEventCannotChangeTheExpectedAuthoredTimeout(): void
    {
        $history = self::falseWake(late: true);
        $witness = self::witness();

        $summary = Anchored::falseWake($history, $witness, 'go', 20, 1);
        self::assertSame(15, $summary['reopens'][0]['authored']);

        $beforeReopen = self::T0 + 6.2;
        self::assertSame(14, Anchored::authoredRemaining(20, self::T0, $beforeReopen),
            'recorded-before-completion visibility would expect a different value');

        // Had the worker actually received the late event, 15 would be wrong.
        $seen = self::witness(deliveredUntil: 6.2, history: $history);
        $this->expectExceptionMessage('anchored_evidence_authored_remaining');
        Anchored::falseWake($history, $seen, 'go', 20, 1);
    }

    /** The Replayer prefers timestamp over recorded_at; the witness keeps both and applies the same precedence. */
    public function testTheWorkerTimestampPrecedenceDecidesTheAuthoredTimeout(): void
    {
        $history = self::falseWake(reopenTimeout: 16);
        $delivered = array_map(static fn (array $event): array => $event + ['timestamp' => self::stamp(
            min(self::offset($event['recorded_at']), 4.2))], self::upTo($history, 5.0));
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', 1, $delivered), Anchored::completion('task-2', 1, [self::command(16)])];

        self::assertSame(16, Anchored::falseWake($history, $witness, 'go', 20, 1)['reopens'][0]['authored']);

        $recordedOnly = [$witness[0], Anchored::delivery('poll', 'task-2', 1, self::upTo($history, 5.0)), $witness[2]];
        $this->expectExceptionMessage('anchored_evidence_authored_remaining');
        Anchored::falseWake($history, $recordedOnly, 'go', 20, 1);
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>>|null $witness
     */
    #[DataProvider('refusals')]
    public function testAnchoredContractViolationsAreRefused(array $history, ?array $witness, string $code): void
    {
        try {
            Anchored::falseWake($history, $witness ?? self::witness(), 'go', 20, 1);
            self::fail('The anchored evidence was accepted.');
        } catch (RuntimeException $error) {
            self::assertSame($code, $error->getMessage());
        }
    }

    /** @return iterable<string, array{list<array<string, mixed>>, ?list<array<string, mixed>>, string}> */
    public static function refusals(): iterable
    {
        yield 'reopen timer one microsecond after the stored anchor' => [self::falseWake(reopenFireAt: 20.000001), null, 'anchored_evidence_anchor_moved'];
        yield 'reopen timer without the anchor' => [self::falseWake(reopenAnchor: false), null, 'anchored_evidence_anchor_missing'];
        yield 'reopen with another budget' => [self::falseWake(reopenBudget: 21), null, 'anchored_evidence_budget'];
        yield 'legacy subject' => [self::falseWake(anchored: false), null, 'anchored_evidence_not_anchored'];
        yield 'timer fired before the anchor' => [self::falseWake(firedAt: 19.9), null, 'anchored_evidence_fired_early'];
        yield 'timer fired after the dispatch-latency bound' => [self::falseWake(firedAt: 22.5), null, 'anchored_evidence_dispatch_latency'];
        yield 'anchor not the first open plus its budget' => [self::falseWake(anchorAt: 21.5), null, 'anchored_evidence_first_open_relation'];
        yield 'no completion witness for the reopen' => [self::falseWake(), [Anchored::completion('task-1', 1, [self::command(20)])], 'anchored_evidence_witness_unproven'];
        yield 'overflowing delivery' => [self::falseWake(), self::witness(overflow: true), 'anchored_evidence_witness_unproven'];
        yield 'delivery for another attempt' => [self::falseWake(), self::witness(deliveryAttempt: 2), 'anchored_evidence_witness_unproven'];
        yield 'recorded remaining differs from the authored command' => [self::falseWake(reopenTimeout: 14), null, 'anchored_evidence_recorded_remaining'];
    }

    public function testAFreshCallHasItsOwnAnchorAndBudget(): void
    {
        [$history, $witness] = self::ownBudget(secondOpenedAt: 12.0);

        $summary = Anchored::ownBudget($history, $witness, 'first', 30, 'second', 20);

        self::assertSame(self::stamp(30.0), $summary['first_anchor']);
        self::assertSame(self::stamp(32.0), $summary['second_anchor']);
        self::assertSame(15, $summary['reopens'][0]['authored']);
    }

    public function testAFreshCallReusingTheEarlierAnchorIsRefused(): void
    {
        [$history, $witness] = self::ownBudget(secondOpenedAt: 10.0);

        $this->expectExceptionMessage('anchored_evidence_anchor_reused');
        Anchored::ownBudget($history, $witness, 'first', 30, 'second', 20);
    }

    /** The witness keeps types, both timestamps and identities only: no arguments, payload values or tokens. */
    public function testTheWitnessKeepsTimesAndIdentitiesOnly(): void
    {
        $entry = Anchored::delivery('poll', 'task-9', 3, [['event_type' => 'SignalReceived', 'recorded_at' => self::stamp(1.0),
            'timestamp' => self::stamp(1.0), 'payload' => ['signal_name' => 'go', 'arguments' => ['codec' => 'avro', 'blob' => 'c2VjcmV0'],
                'token' => 'claimed-task-worker', 'sequence' => 4, 'condition_wait_id' => 'w1']]]);

        self::assertSame(['source' => 'poll', 'task_id' => 'task-9', 'attempt' => 3, 'overflow' => false, 'events' => [[
            'type' => 'SignalReceived', 'timestamp' => self::stamp(1.0), 'recorded_at' => self::stamp(1.0), 'sequence' => 4,
            'condition_wait_id' => 'w1', 'condition_wait_occurrence_id' => null, 'condition_key' => null, 'timer_id' => null]]], $entry);
        self::assertStringNotContainsString('c2VjcmV0', (string) json_encode($entry));
        self::assertStringNotContainsString('claimed-task-worker', (string) json_encode($entry));
        self::assertTrue(Anchored::delivery('poll', 'task-9', 1, array_fill(0, Anchored::MAX_WITNESS_EVENTS + 1, []))['overflow']);
    }

    /** @return list<array<string, mixed>> */
    private static function falseWake(bool $anchored = true, int $reopenTimeout = 15, float $reopenFireAt = 20.0, bool $reopenAnchor = true,
        int $reopenBudget = 20, float $firedAt = 20.4, float $anchorAt = 20.0, bool $late = false, bool $second = false): array
    {
        $policy = $anchored ? ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 20] : [];
        $history = [
            self::event(0.0, 'WorkflowStarted'),
            self::opened(0.0, 'w1', 20, $policy),
            self::scheduled(0.0, 't1', 'w1', $anchorAt, $anchored),
            self::event(5.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(5.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]),
        ];
        if ($late) {
            $history[] = self::event(6.2, 'SignalReceived', ['signal_name' => 'noise']);
        }
        $reopenPolicy = $anchored ? ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => $reopenBudget,
            'occurrence_deadline_at' => self::stamp(20.0)] : [];
        $history[] = self::opened(6.5, 'w2', $reopenTimeout, $reopenPolicy, 2);
        $history[] = self::scheduled(6.5, 't2', 'w2', $reopenFireAt, $anchored && $reopenAnchor, 2, $anchorAt);
        if ($second) {
            $history[] = self::event(10.0, 'SignalReceived', ['signal_name' => 'go']);
            $history[] = self::event(10.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w2', 'sequence' => 2]);
            $history[] = self::event(10.0, 'TimerCancelled', ['timer_id' => 't2', 'sequence' => 2]);
            $history[] = self::opened(10.3, 'w3', 10, $reopenPolicy, 3);
            $history[] = self::scheduled(10.3, 't3', 'w3', 20.0, true, 3);
            $last = 't3';
        }
        $history[] = self::event($firedAt, 'TimerFired', ['timer_id' => $last ?? 't2', 'fired_at' => self::stamp($firedAt)]);
        $history[] = self::event($firedAt + 0.2, 'WorkflowCompleted');

        return $history;
    }

    /** @return array{list<array<string, mixed>>, list<array<string, mixed>>} */
    private static function ownBudget(float $secondOpenedAt): array
    {
        $first = ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 30];
        $second = ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 20];
        $anchor = $secondOpenedAt + 20.0;
        $history = [
            self::event(0.0, 'WorkflowStarted'),
            self::opened(0.0, 'a1', 30, $first, 1, 'first', 'occ-0'), self::scheduled(0.0, 'ta', 'a1', 30.0, true),
            self::event(9.0, 'SignalReceived', ['signal_name' => 'first']),
            self::event(9.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'a1', 'sequence' => 1]),
            self::event(9.0, 'TimerCancelled', ['timer_id' => 'ta', 'sequence' => 1]),
            self::opened($secondOpenedAt, 'b1', 20, $second, 2, 'second', 'occ-1'), self::scheduled($secondOpenedAt, 'tb', 'b1', $anchor, true, 2),
            self::event($secondOpenedAt + 5.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event($secondOpenedAt + 5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b1', 'sequence' => 2]),
            self::event($secondOpenedAt + 5.0, 'TimerCancelled', ['timer_id' => 'tb', 'sequence' => 2]),
            self::opened($secondOpenedAt + 5.2, 'b2', 15, $second + ['occurrence_deadline_at' => self::stamp($anchor)], 3, 'second', 'occ-1'),
            self::scheduled($secondOpenedAt + 5.2, 'tc', 'b2', $anchor, true, 3, $anchor),
            self::event($secondOpenedAt + 7.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event($secondOpenedAt + 7.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b2', 'sequence' => 3]),
            self::event($secondOpenedAt + 7.0, 'TimerCancelled', ['timer_id' => 'tc', 'sequence' => 3]),
            self::event($secondOpenedAt + 7.1, 'WorkflowCompleted'),
        ];
        $witness = [
            Anchored::completion('task-1', 1, [self::command(30, 'occ-0', 30)]),
            Anchored::delivery('poll', 'task-2', 1, self::upTo($history, 9.0)),
            Anchored::completion('task-2', 1, [self::command(20, 'occ-1', 20)]),
            Anchored::delivery('poll', 'task-3', 1, self::upTo($history, $secondOpenedAt + 5.0)),
            Anchored::completion('task-3', 1, [self::command(15, 'occ-1', 20)]),
        ];

        return [$history, $witness];
    }

    /**
     * @param list<array<string, mixed>>|null $history
     * @return list<array<string, mixed>>
     */
    private static function witness(float $deliveredUntil = 5.0, bool $overflow = false, int $deliveryAttempt = 1, ?array $history = null): array
    {
        $delivered = self::upTo($history ?? self::falseWake(), $deliveredUntil);
        if ($overflow) {
            $delivered = array_pad($delivered, Anchored::MAX_WITNESS_EVENTS + 1, self::event(0.0, 'WorkflowTaskStarted'));
        }

        return [
            Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', $deliveryAttempt, $delivered),
            Anchored::completion('task-2', 1, [self::command(15)]),
        ];
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private static function upTo(array $history, float $offset): array
    {
        return array_values(array_filter($history, static fn (array $event): bool => self::offset($event['recorded_at']) <= $offset + 1e-9));
    }

    /** @return array<string, mixed> */
    private static function command(int $timeout, string $occurrence = 'occ-1', int $budget = 20): array
    {
        return ['type' => 'open_condition_wait', 'condition_wait_occurrence_id' => $occurrence, 'timeout_seconds' => $timeout,
            'deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => $budget, 'arguments' => 'not kept'];
    }

    /**
     * @param array<string, mixed> $policy
     * @return array<string, mixed>
     */
    private static function opened(float $at, string $id, int $timeout, array $policy, int $sequence = 1, string $key = 'go',
        string $occurrence = 'occ-1'): array
    {
        return self::event($at, 'ConditionWaitOpened', ['condition_wait_id' => $id, 'condition_key' => $key,
            'condition_wait_occurrence_id' => $occurrence, 'timeout_seconds' => $timeout, 'sequence' => $sequence] + $policy);
    }

    /** @return array<string, mixed> */
    private static function scheduled(float $at, string $timer, string $wait, float $fireAt, bool $anchor, int $sequence = 1,
        ?float $anchorAt = null): array
    {
        $payload = ['timer_id' => $timer, 'sequence' => $sequence, 'timer_kind' => 'condition_timeout', 'condition_wait_id' => $wait,
            'fire_at' => self::stamp($fireAt)];
        if ($anchor && $sequence > 1) {
            $payload += ['deadline_policy' => 'occurrence', 'occurrence_deadline_at' => self::stamp($anchorAt ?? $fireAt)];
        }

        return self::event($at, 'TimerScheduled', $payload);
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function event(float $at, string $type, array $payload = []): array
    {
        return ['event_type' => $type, 'recorded_at' => self::stamp($at), 'payload' => $payload];
    }

    private static function stamp(float $offset): string
    {
        return \DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', self::T0 + $offset))->format('Y-m-d\TH:i:s.uP');
    }

    private static function offset(string $stamp): float
    {
        return round((float) \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.uP', $stamp)->format('U.u') - self::T0, 6);
    }
}
