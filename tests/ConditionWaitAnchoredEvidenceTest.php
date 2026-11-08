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
        self::assertSame([['task_id' => 'task-2', 'attempt' => 1, 'wake_event' => 'TimerCancelled', 'wake_sequence' => 6,
            'wake_id' => 'e6', 'time_field' => 'recorded_at', 'opened' => sprintf('%.6F', self::T0), 'now' => sprintf('%.6F', self::T0 + 5.0),
            'authored' => 15, 'delivered_events' => 6]], $summary['reopens']);
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
     * Native can record a lower sequence with a later instant (ActivityScheduled,
     * TimerScheduled and ActivityStarted take their time before the sequence is
     * allocated). The Replayer uses the latest instant regardless of sequence,
     * and so does the check: no highest-sequence rule.
     */
    public function testASequenceTimeInversionUsesTheLatestInstant(): void
    {
        $history = self::falseWake();
        $delivered = self::upTo($history, 5.0);
        $delivered[3]['recorded_at'] = self::stamp(5.5);
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', 1, $delivered), Anchored::completion('task-2', 1, [self::command(15)])];

        $reopen = Anchored::falseWake($history, $witness, 'go', 20, 1)['reopens'][0];

        self::assertSame([4, sprintf('%.6F', self::T0 + 5.5), 15], [$reopen['wake_sequence'], $reopen['now'], $reopen['authored']],
            'the latest instant (sequence 4) is now, though sequences 5 and 6 are higher');
    }

    public function testASequenceTimeInversionThatMovesNowChangesTheExpectedTimeout(): void
    {
        $history = self::falseWake();
        $delivered = self::upTo($history, 5.0);
        $delivered[3]['recorded_at'] = self::stamp(6.2);
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', 1, $delivered), Anchored::completion('task-2', 1, [self::command(15)])];

        $this->expectExceptionMessage('anchored_evidence_authored_remaining');
        Anchored::falseWake($history, $witness, 'go', 20, 1);
    }

    /**
     * Continuation pages read current storage: an event appended after the
     * poll and actually delivered on a later page of the same attempt is
     * visible, so the worker authors from it.
     */
    public function testALateEventDeliveredOnAContinuationPageIsVisible(): void
    {
        $history = self::falseWake(late: true, reopenTimeout: 14);
        $page = array_values(array_filter($history, static fn (array $e): bool => abs(self::offset($e['recorded_at']) - 6.2) < 1e-9));
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', 1, self::upTo($history, 5.0), 'page-0'),
            Anchored::delivery('continuation_page', 'task-2', 1, $page, 'page-1'),
            Anchored::completion('task-2', 1, [self::command(14)])];

        $summary = Anchored::falseWake($history, $witness, 'go', 20, 1);

        self::assertSame(['SignalReceived', 14], [$summary['reopens'][0]['wake_event'], $summary['reopens'][0]['authored']]);
    }

    /** A task carried inside a completion response is a delivery route for that next task. */
    public function testACarriedTaskIsADeliveryRoute(): void
    {
        $history = self::falseWake();
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('carried_task', 'task-2', 1, self::upTo($history, 5.0)), Anchored::completion('task-2', 1, [self::command(15)])];

        self::assertSame(15, Anchored::falseWake($history, $witness, 'go', 20, 1)['reopens'][0]['authored']);
    }

    /** The Worker's own replay input (the observer hook) authors the reopen, in replay-input-only mode. */
    public function testTheWorkerReplayInputAuthorsTheReopen(): void
    {
        $history = self::falseWake();

        $summary = Anchored::falseWake($history, self::replayWitness([Anchored::replayInput('task-2', 1, self::upTo($history, 5.0))]),
            'go', 20, 1, true);

        self::assertSame([['task_id' => 'task-2', 'attempt' => 1, 'wake_event' => 'TimerCancelled', 'wake_sequence' => 6,
            'wake_id' => 'e6', 'time_field' => 'recorded_at', 'opened' => sprintf('%.6F', self::T0), 'now' => sprintf('%.6F', self::T0 + 5.0),
            'authored' => 15, 'delivered_events' => 6]], $summary['reopens']);
    }

    /** The observer adapter records exactly what the Worker hook reports, as bounded entries. */
    public function testTheObserverAdapterRecordsTheReplayInputAndCompletion(): void
    {
        $history = self::falseWake();
        $witness = [];
        $observe = Anchored::observer($witness);
        $observe('completion', 'task-1', 1, [self::command(20)]);
        $observe('accepted', 'task-1', 1, [self::command(20)]);
        $observe('history', 'task-2', 1, self::upTo($history, 5.0));
        $observe('completion', 'task-2', 1, [self::command(15)]);
        $observe('accepted', 'task-2', 1, [self::command(15)]);

        self::assertSame([Anchored::completion('task-1', 1, [self::command(20)]), Anchored::accepted('task-1', 1, [self::command(20)]),
            Anchored::replayInput('task-2', 1, self::upTo($history, 5.0)), Anchored::completion('task-2', 1, [self::command(15)]),
            Anchored::accepted('task-2', 1, [self::command(15)])], $witness);
        self::assertSame(15, Anchored::falseWake($history, $witness, 'go', 20, 1, true)['reopens'][0]['authored']);
        self::assertStringNotContainsString('not kept', (string) json_encode($witness));
    }

    /** A replay input is a prefix of history: a late event is visible only when the input contains it. */
    public function testAReplayInputContainingTheLateEventAuthorsFromIt(): void
    {
        $history = self::falseWake(late: true, reopenTimeout: 14);

        $summary = Anchored::falseWake($history, self::replayWitness([Anchored::replayInput('task-2', 1, self::upTo($history, 6.2))], 14),
            'go', 20, 1, true);

        self::assertSame(['SignalReceived', 7, 14], [$summary['reopens'][0]['wake_event'], $summary['reopens'][0]['wake_sequence'],
            $summary['reopens'][0]['authored']]);
    }

    public function testAFreshCallReopenIsAuthoredFromTheReplayInput(): void
    {
        [$history] = self::ownBudget(secondOpenedAt: 12.0);
        $witness = [
            Anchored::completion('task-1', 1, [self::command(30, 'occ-0', 30)]),
            Anchored::replayInput('task-2', 1, self::upTo($history, 9.0)),
            Anchored::completion('task-2', 1, [self::command(20, 'occ-1', 20)]),
            Anchored::accepted('task-2', 1, [self::command(20, 'occ-1', 20)]),
            Anchored::replayInput('task-3', 1, self::upTo($history, 17.0)),
            Anchored::completion('task-3', 1, [self::command(15, 'occ-1', 20)]),
            Anchored::accepted('task-3', 1, [self::command(15, 'occ-1', 20)]),
        ];

        self::assertSame(15, Anchored::ownBudget($history, $witness, 'first', 30, 'second', 20, true)['reopens'][0]['authored']);
    }

    /**
     * @param list<array<string, mixed>> $witness
     */
    #[DataProvider('replayInputRefusals')]
    public function testIncompleteOrOverBoundReplayInputIsRefused(array $witness, string $code): void
    {
        try {
            Anchored::falseWake(self::falseWake(), $witness, 'go', 20, 1, true);
            self::fail('The replay-input evidence was accepted.');
        } catch (RuntimeException $error) {
            self::assertSame($code, $error->getMessage());
        }
    }

    /** @return iterable<string, array{list<array<string, mixed>>, string}> */
    public static function replayInputRefusals(): iterable
    {
        $history = self::falseWake();
        $input = self::upTo($history, 5.0);
        $replay = static fn (array $events, int $attempt = 1): array => Anchored::replayInput('task-2', $attempt, $events);
        $without = static function (array $events, int $position): array {
            unset($events[$position]);

            return array_values($events);
        };
        $changed = $input;
        $changed[2]['id'] = 'e3-other';
        $retyped = $input;
        $retyped[3]['event_type'] = 'UpdateApplied';
        $padded = array_pad($input, Anchored::MAX_WITNESS_EVENTS + 1, self::event(5.0, 'WorkflowTaskStarted') + ['sequence' => 7, 'id' => 'pad']);

        yield 'transport slices only' => [self::replayWitness([Anchored::delivery('poll', 'task-2', 1, $input)]), 'anchored_evidence_replay_input_missing'];
        yield 'replay input for another attempt' => [self::replayWitness([$replay($input, 2)]), 'anchored_evidence_replay_input_missing'];
        yield 'two replay inputs for the attempt' => [self::replayWitness([$replay($input), $replay($input)]), 'anchored_evidence_replay_input_ambiguous'];
        yield 'replay input mixed with a poll slice' => [self::replayWitness([$replay($input), Anchored::delivery('poll', 'task-2', 1, $input)]),
            'anchored_evidence_replay_input_ambiguous'];
        yield 'truncated replay input' => [self::replayWitness([$replay($padded)]), 'anchored_evidence_replay_input_incomplete'];
        yield 'empty replay input' => [self::replayWitness([$replay([])]), 'anchored_evidence_replay_input_incomplete'];
        yield 'missing first event' => [self::replayWitness([$replay($without($input, 0))]), 'anchored_evidence_replay_input_incomplete'];
        yield 'missing middle event' => [self::replayWitness([$replay($without($input, 2))]), 'anchored_evidence_replay_input_incomplete'];
        yield 'repeated event' => [self::replayWitness([$replay([...$input, $input[5]])]), 'anchored_evidence_replay_input_incomplete'];
        yield 'event identity differs from history' => [self::replayWitness([$replay($changed)]), 'anchored_evidence_replay_input_mismatch'];
        yield 'event type differs from history' => [self::replayWitness([$replay($retyped)]), 'anchored_evidence_replay_input_mismatch'];
        yield 'replay input already contains the authored reopen' => [self::replayWitness([$replay(self::upTo($history, 6.5))]),
            'anchored_evidence_replay_input_over_bound'];
        yield 'unknown observer kind' => [[...self::replayWitness([$replay($input)]), ['source' => 'unknown', 'kind' => 'query', 'task_id' => 'task-2',
            'attempt' => 1]], 'anchored_evidence_witness_shape'];
    }

    /**
     * A refused proposal of attempt 1 cannot carry evidence: only the attempt
     * whose completion returned (accepted) binds to the recorded reopen, with
     * that attempt's own replay input.
     */
    public function testOnlyTheAcceptedAttemptOfATaskBindsToItsRecordedOpen(): void
    {
        $history = self::falseWake();
        $witness = [Anchored::completion('task-1', 1, [self::command(20)]), Anchored::accepted('task-1', 1, [self::command(20)]),
            Anchored::replayInput('task-2', 1, self::upTo($history, 4.0)), Anchored::completion('task-2', 1, [self::command(16)]),
            Anchored::replayInput('task-2', 2, self::upTo($history, 5.0)), Anchored::completion('task-2', 2, [self::command(15)]),
            Anchored::accepted('task-2', 2, [self::command(15)])];

        $reopen = Anchored::falseWake($history, $witness, 'go', 20, 1, true)['reopens'][0];

        self::assertSame(['task-2', 2, 15], [$reopen['task_id'], $reopen['attempt'], $reopen['authored']]);
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>> $witness
     */
    #[DataProvider('bindingRefusals')]
    public function testCompletionBindingViolationsAreRefused(array $history, array $witness, bool $replayInputOnly, string $code): void
    {
        try {
            Anchored::falseWake($history, $witness, 'go', 20, 1, $replayInputOnly);
            self::fail('The completion binding was accepted.');
        } catch (RuntimeException $error) {
            self::assertSame($code, $error->getMessage());
        }
    }

    /** @return iterable<string, array{list<array<string, mixed>>, list<array<string, mixed>>, bool, string}> */
    public static function bindingRefusals(): iterable
    {
        $history = self::falseWake();
        $input = Anchored::replayInput('task-2', 1, self::upTo($history, 5.0));
        $poll = Anchored::delivery('poll', 'task-2', 1, self::upTo($history, 5.0));
        $first = [Anchored::completion('task-1', 1, [self::command(20)]), Anchored::accepted('task-1', 1, [self::command(20)])];
        $unbound = $history;
        unset($unbound[6]['workflow_task_id']);

        yield 'reopen authored by a task other than the recorded one' => [$history,
            [...$first, $poll, Anchored::delivery('poll', 'task-9', 1, self::upTo($history, 5.0)), Anchored::completion('task-9', 1, [self::command(15)])],
            false, 'anchored_evidence_completion_binding'];
        yield 'recorded open without its authoring task id' => [$unbound, self::witness(), false, 'anchored_evidence_open_task_unbound'];
        yield 'two proposals of one task without an accepted report' => [$history,
            [...self::witness(), Anchored::delivery('poll', 'task-2', 2, self::upTo($history, 5.0)), Anchored::completion('task-2', 2, [self::command(15)])],
            false, 'anchored_evidence_completion_binding'];
        yield 'replay-input mode: the reopen proposal was never accepted' => [$history,
            [...$first, $input, Anchored::completion('task-2', 1, [self::command(15)])], true, 'anchored_evidence_completion_binding'];
        yield 'replay-input mode: accepted commands differ from the proposal' => [$history,
            [...$first, $input, Anchored::completion('task-2', 1, [self::command(15)]), Anchored::accepted('task-2', 1, [self::command(14)])],
            true, 'anchored_evidence_completion_binding'];
        yield 'replay-input mode: two accepted reports for one task' => [$history,
            [...self::replayWitness([$input]), Anchored::accepted('task-2', 2, [self::command(15)])], true, 'anchored_evidence_completion_binding'];
        yield 'an extra accepted completion authors the occurrence' => [$history,
            [...self::replayWitness([$input]), Anchored::completion('task-7', 1, [self::command(15)]), Anchored::accepted('task-7', 1, [self::command(15)])],
            true, 'anchored_evidence_completion_binding'];
        yield 'completion 0 does not author the full budget' => [$history,
            [Anchored::completion('task-1', 1, [self::command(19)]), $poll, Anchored::completion('task-2', 1, [self::command(15)])],
            false, 'anchored_evidence_first_command'];
    }

    /**
     * The opening instant is that of the chain's first open, matched by its
     * recorded id and sequence; if the Replayer could not time it (here a
     * non-string timestamp, which the Replayer reads as null), the evidence is
     * refused instead of falling back to another open or to recorded_at.
     */
    public function testAnUntimedFirstOpenIsRefused(): void
    {
        $history = self::falseWake();
        $delivered = self::upTo($history, 5.0);
        $delivered[1]['timestamp'] = 0;

        $this->expectExceptionMessage('anchored_evidence_opened_untimed');
        Anchored::falseWake($history, self::replayWitness([Anchored::replayInput('task-2', 1, $delivered)]), 'go', 20, 1, true);
    }

    /**
     * The Replayer stops at a non-null timestamp even when it is not a string,
     * so that event is untimed; the witness keeps that choice and never falls
     * through to its recorded_at (6.2 s here, which would expect 14).
     */
    public function testANonStringTimestampIsUntimedLikeTheReplayer(): void
    {
        $history = self::falseWake(late: true);
        $delivered = self::upTo($history, 6.2);
        $delivered[6]['timestamp'] = ['not', 'a', 'string'];

        $reopen = Anchored::falseWake($history, self::replayWitness([Anchored::replayInput('task-2', 1, $delivered)]), 'go', 20, 1, true)['reopens'][0];

        self::assertSame([6, 15, 7], [$reopen['wake_sequence'], $reopen['authored'], $reopen['delivered_events']]);
    }

    /** The recorded history is the Native history export of a run the qualifier started. */
    public function testAFreshNativeExportIsTheRecordedHistory(): void
    {
        $history = self::falseWake();
        $recorded = Anchored::freshNativeHistory(self::export($history), 'run-1');

        self::assertSame(['id', 'sequence', 'type', 'workflow_task_id', 'workflow_command_id', 'recorded_at', 'payload'], array_keys($recorded[0]));
        self::assertSame(15, Anchored::falseWake($recorded, self::replayWitness([Anchored::replayInput('task-2', 1, self::upTo($history, 5.0))]),
            'go', 20, 1, true)['reopens'][0]['authored']);
    }

    public function testTheNativeStartCommandPrefixPreservesTheReplayInputBinding(): void
    {
        [$history, $export] = self::nativeExport();
        $recorded = Anchored::freshNativeHistory($export, 'run-1');

        self::assertSame(['StartAccepted', 'WorkflowStarted'], array_column(array_slice($recorded, 0, 2), 'type'));
        self::assertSame(15, Anchored::falseWake($recorded,
            self::replayWitness([Anchored::replayInput('task-2', 1, self::upTo($history, 5.0))]),
            'go', 20, 1, true)['reopens'][0]['authored']);
    }

    public function testTheNativeMicrosecondZuluExportTimesAreAccepted(): void
    {
        [, $export] = self::nativeExport();
        foreach ($export['history_events'] as &$event) {
            $event['recorded_at'] = str_replace('+00:00', 'Z', $event['recorded_at']);
        }
        unset($event);

        $recorded = Anchored::freshNativeHistory($export, 'run-1');

        self::assertStringEndsWith('.000000Z', $recorded[0]['recorded_at']);
        self::assertSame($export['history_events'], $recorded);
    }

    /** @param array<string, mixed> $export */
    #[DataProvider('invalidNativeStarts')]
    public function testAnUnboundNativeStartCommandPrefixIsRefused(array $export): void
    {
        $this->expectExceptionMessage('anchored_evidence_not_fresh_native');
        Anchored::freshNativeHistory($export, 'run-1');
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function invalidNativeStarts(): iterable
    {
        [, $export] = self::nativeExport();
        $missing = $export;
        unset($missing['history_events'][0]['workflow_command_id']);
        yield 'missing accepted start command' => [$missing];
        $empty = $export;
        $empty['history_events'][0]['workflow_command_id'] = '';
        $empty['history_events'][1]['workflow_command_id'] = '';
        yield 'empty start command identity' => [$empty];
        $different = $export;
        $different['history_events'][1]['workflow_command_id'] = 'another-command';
        yield 'different start command identities' => [$different];
        foreach ([0, 1] as $position) {
            $authored = $export;
            $authored['history_events'][$position]['workflow_task_id'] = 'task-foreign';
            yield 'start prefix authored by a task at '.$position => [$authored];
        }
        $without = $export;
        $without['history_events'][1]['type'] = 'ConditionWaitOpened';
        yield 'accepted command without WorkflowStarted' => [$without];
        $duplicate = $export;
        $duplicate['history_events'][1]['type'] = 'StartAccepted';
        yield 'two accepted starts' => [$duplicate];
        $other = $export;
        $other['history_events'][0]['type'] = 'SignalReceived';
        yield 'unrelated first event' => [$other];
    }

    /**
     * @param array<string, mixed> $export
     */
    #[DataProvider('unfreshExports')]
    public function testAnExportThatIsNotAFreshNativeRunIsRefused(array $export): void
    {
        $this->expectExceptionMessage('anchored_evidence_not_fresh_native');
        Anchored::freshNativeHistory($export, 'run-1');
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function unfreshExports(): iterable
    {
        $export = self::export(self::falseWake());
        $untimed = $export;
        $untimed['history_events'][3]['recorded_at'] = null;
        $gap = $export;
        unset($gap['history_events'][2]);
        $gap['history_events'] = array_values($gap['history_events']);
        $late = $export;
        $late['history_events'] = array_slice($late['history_events'], 1);

        yield 'a Waterline v1 projection' => [$export + ['migration_projection' => ['read_only' => true]]];
        yield 'another run' => [self::export(self::falseWake(), 'run-2')];
        yield 'an untimed event' => [$untimed];
        yield 'a sequence gap' => [$gap];
        yield 'history not starting at WorkflowStarted' => [$late];
        yield 'no history' => [['workflow' => ['run_id' => 'run-1'], 'history_events' => []]];
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
        yield 'reopen timer without the policy' => [self::falseWake(reopenTimerPolicy: false), null, 'anchored_evidence_timer_policy'];
        yield 'first open without its anchor string' => [self::falseWake(firstDeadline: false), null, 'anchored_evidence_anchor_missing'];
        yield 'reopen with another budget' => [self::falseWake(reopenBudget: 21), null, 'anchored_evidence_budget'];
        yield 'legacy subject' => [self::falseWake(anchored: false), null, 'anchored_evidence_not_anchored'];
        yield 'timer fired before the anchor' => [self::falseWake(firedAt: 19.9), null, 'anchored_evidence_fired_early'];
        yield 'timer fired after the dispatch-latency bound' => [self::falseWake(firedAt: 22.5), null, 'anchored_evidence_dispatch_latency'];
        yield 'anchor not the first open plus its budget' => [self::falseWake(anchorAt: 21.5), null, 'anchored_evidence_first_open_relation'];
        yield 'no completion witness for the reopen' => [self::falseWake(), [Anchored::completion('task-1', 1, [self::command(20)])], 'anchored_evidence_completion_binding'];
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
        $entry = Anchored::delivery('poll', 'task-9', 3, [['id' => 'e7', 'sequence' => 7, 'event_type' => 'SignalReceived', 'recorded_at' => self::stamp(1.0),
            'timestamp' => self::stamp(1.0), 'payload' => ['signal_name' => 'go', 'arguments' => ['codec' => 'avro', 'blob' => 'c2VjcmV0'],
                'token' => 'claimed-task-worker', 'sequence' => 4, 'condition_wait_id' => 'w1']]]);

        self::assertSame(['source' => 'poll', 'task_id' => 'task-9', 'attempt' => 3, 'slice' => '0', 'overflow' => false, 'events' => [[
            'id' => 'e7', 'sequence' => 7, 'type' => 'SignalReceived', 'timestamp' => self::stamp(1.0), 'recorded_at' => self::stamp(1.0),
            'time_field' => 'timestamp', 'time' => self::stamp(1.0), 'workflow_task_id' => null, 'condition_wait_id' => 'w1',
            'condition_wait_occurrence_id' => null, 'condition_key' => null, 'timer_id' => null]]], $entry);
        self::assertStringNotContainsString('c2VjcmV0', (string) json_encode($entry));
        self::assertStringNotContainsString('claimed-task-worker', (string) json_encode($entry));
        self::assertTrue(Anchored::delivery('poll', 'task-9', 1, array_fill(0, Anchored::MAX_WITNESS_EVENTS + 1, []))['overflow']);
    }

    /** @return list<array<string, mixed>> */
    private static function falseWake(bool $anchored = true, int $reopenTimeout = 15, float $reopenFireAt = 20.0, bool $reopenAnchor = true,
        int $reopenBudget = 20, float $firedAt = 20.4, float $anchorAt = 20.0, bool $late = false, bool $second = false,
        bool $reopenTimerPolicy = true, bool $firstDeadline = true): array
    {
        $policy = $anchored ? ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 20]
            + ($firstDeadline ? ['occurrence_deadline_at' => self::stamp($anchorAt)] : []) : [];
        $history = [
            self::event(0.0, 'WorkflowStarted'),
            self::opened(0.0, 'w1', 20, $policy),
            self::scheduled(0.0, 't1', 'w1', $anchorAt, $anchored, $anchored),
            self::event(5.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(5.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]),
        ];
        if ($late) {
            $history[] = self::event(6.2, 'SignalReceived', ['signal_name' => 'noise']);
        }
        $reopenPolicy = $anchored ? ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => $reopenBudget,
            'occurrence_deadline_at' => self::stamp($anchorAt)] : [];
        $history[] = self::opened(6.5, 'w2', $reopenTimeout, $reopenPolicy, 2);
        $history[] = self::scheduled(6.5, 't2', 'w2', $reopenFireAt, $anchored && $reopenTimerPolicy, $anchored && $reopenAnchor, 2, $anchorAt);
        if ($second) {
            $history[] = self::event(10.0, 'SignalReceived', ['signal_name' => 'go']);
            $history[] = self::event(10.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w2', 'sequence' => 2]);
            $history[] = self::event(10.0, 'TimerCancelled', ['timer_id' => 't2', 'sequence' => 2]);
            $history[] = self::opened(10.3, 'w3', 10, $reopenPolicy, 3);
            $history[] = self::scheduled(10.3, 't3', 'w3', 20.0, true, true, 3);
            $last = 't3';
        }
        $history[] = self::event($firedAt, 'TimerFired', ['timer_id' => $last ?? 't2', 'fired_at' => self::stamp($firedAt)]);
        $history[] = self::event($firedAt + 0.2, 'WorkflowCompleted');

        return self::numbered($history);
    }

    /** @return array{list<array<string, mixed>>, list<array<string, mixed>>} */
    private static function ownBudget(float $secondOpenedAt): array
    {
        $anchor = $secondOpenedAt + 20.0;
        $first = ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 30, 'occurrence_deadline_at' => self::stamp(30.0)];
        $second = ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => 20, 'occurrence_deadline_at' => self::stamp($anchor)];
        $history = self::numbered([
            self::event(0.0, 'WorkflowStarted'),
            self::opened(0.0, 'a1', 30, $first, 1, 'first', 'occ-0'), self::scheduled(0.0, 'ta', 'a1', 30.0, true, true),
            self::event(9.0, 'SignalReceived', ['signal_name' => 'first']),
            self::event(9.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'a1', 'sequence' => 1]),
            self::event(9.0, 'TimerCancelled', ['timer_id' => 'ta', 'sequence' => 1]),
            self::opened($secondOpenedAt, 'b1', 20, $second, 2, 'second', 'occ-1'), self::scheduled($secondOpenedAt, 'tb', 'b1', $anchor, true, true, 2),
            self::event($secondOpenedAt + 5.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event($secondOpenedAt + 5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b1', 'sequence' => 2]),
            self::event($secondOpenedAt + 5.0, 'TimerCancelled', ['timer_id' => 'tb', 'sequence' => 2]),
            self::opened($secondOpenedAt + 5.2, 'b2', 15, $second, 3, 'second', 'occ-1'),
            self::scheduled($secondOpenedAt + 5.2, 'tc', 'b2', $anchor, true, true, 3, $anchor),
            self::event($secondOpenedAt + 7.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event($secondOpenedAt + 7.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b2', 'sequence' => 3]),
            self::event($secondOpenedAt + 7.0, 'TimerCancelled', ['timer_id' => 'tc', 'sequence' => 3]),
            self::event($secondOpenedAt + 7.1, 'WorkflowCompleted'),
        ]);
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
            $delivered = array_pad($delivered, Anchored::MAX_WITNESS_EVENTS + 1, self::event(0.0, 'WorkflowTaskStarted') + ['sequence' => 0, 'id' => 'pad']);
        }

        return [
            Anchored::completion('task-1', 1, [self::command(20)]),
            Anchored::delivery('poll', 'task-2', $deliveryAttempt, $delivered),
            Anchored::completion('task-2', 1, [self::command(15)]),
        ];
    }

    /**
     * The first call's proposed and accepted completion, the given task-2
     * entries, and task-2's proposed and accepted reopen completion.
     *
     * @param list<array<string, mixed>> $entries
     * @return list<array<string, mixed>>
     */
    private static function replayWitness(array $entries, int $reopenTimeout = 15): array
    {
        return [Anchored::completion('task-1', 1, [self::command(20)]), Anchored::accepted('task-1', 1, [self::command(20)]), ...$entries,
            Anchored::completion('task-2', 1, [self::command($reopenTimeout)]), Anchored::accepted('task-2', 1, [self::command($reopenTimeout)])];
    }

    /**
     * A Native history export bundle for the given numbered history: the
     * export's event shape (id, sequence, type, workflow_task_id,
     * workflow_command_id, recorded_at, payload) and the selected run.
     *
     * @param list<array<string, mixed>> $history
     * @return array<string, mixed>
     */
    private static function export(array $history, string $runId = 'run-1'): array
    {
        return ['schema' => 'durable-workflow.v2.history-export', 'schema_version' => 2, 'workflow' => ['run_id' => $runId],
            'history_events' => array_map(static fn (array $event): array => ['id' => $event['id'], 'sequence' => $event['sequence'],
                'type' => $event['event_type'], 'workflow_task_id' => $event['workflow_task_id'] ?? null, 'workflow_command_id' => null,
                'recorded_at' => $event['recorded_at'], 'payload' => $event['payload']], $history)];
    }

    /** @return array{list<array<string, mixed>>, array<string, mixed>} */
    private static function nativeExport(): array
    {
        // Native 3.3.0 records StartAccepted and WorkflowStarted for the same
        // control-plane start command before the first worker-authored open.
        $history = self::numbered([self::event(0.0, 'StartAccepted'), ...self::falseWake()]);
        $export = self::export($history);
        $export['history_events'][0]['workflow_command_id'] = 'command-start';
        $export['history_events'][1]['workflow_command_id'] = 'command-start';

        return [$history, $export];
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
        $task = ['w1' => 'task-1', 'w2' => 'task-2', 'w3' => 'task-3', 'a1' => 'task-1', 'b1' => 'task-2', 'b2' => 'task-3'][$id];

        return ['workflow_task_id' => $task] + self::event($at, 'ConditionWaitOpened', ['condition_wait_id' => $id, 'condition_key' => $key,
            'condition_wait_occurrence_id' => $occurrence, 'timeout_seconds' => $timeout, 'sequence' => $sequence] + $policy);
    }

    /** @return array<string, mixed> */
    private static function scheduled(float $at, string $timer, string $wait, float $fireAt, bool $policy, bool $anchor, int $sequence = 1,
        ?float $anchorAt = null): array
    {
        $payload = ['timer_id' => $timer, 'sequence' => $sequence, 'timer_kind' => 'condition_timeout', 'condition_wait_id' => $wait,
            'fire_at' => self::stamp($fireAt)];
        if ($policy) {
            $payload['deadline_policy'] = 'occurrence';
        }
        if ($anchor) {
            $payload['occurrence_deadline_at'] = self::stamp($anchorAt ?? $fireAt);
        }

        return self::event($at, 'TimerScheduled', $payload);
    }

    /**
     * Native event objects carry an id and a history sequence in delivery order.
     *
     * @param list<array<string, mixed>> $history
     * @return list<array<string, mixed>>
     */
    private static function numbered(array $history): array
    {
        foreach ($history as $index => $event) {
            $history[$index] = ['id' => 'e'.($index + 1), 'sequence' => $index + 1] + $event;
        }

        return $history;
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
