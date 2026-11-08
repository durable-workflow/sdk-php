<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DateTimeImmutable;
use RuntimeException;

/**
 * Anchored-mode history assertions for the native occurrence deadline (#269).
 *
 * A distinct mode: it requires the producer contract (deadline_policy
 * "occurrence" with condition_wait_timeout_budget_seconds on every physical
 * open, and the stored anchor A on every condition-timeout timer) and never
 * falls back to ConditionWaitNativeEvidence's legacy predicates, which stay
 * unchanged for their existing subjects. A legacy subject is refused here.
 *
 * A is the first physical wait's stored condition-timeout timer fire_at. It is
 * compared as the recorded value, never recomputed from rounded timestamps.
 *
 * The worker-authored remaining timeout is checked against what the worker
 * actually received: a bounded times/identities-only witness of its poll and
 * history-page responses and of its completion commands, bound to task ID and
 * attempt. The latest visible event is taken from that exact slice with the
 * Replayer's timestamp precedence and integer arithmetic; history recorded
 * before the completion is not treated as visible. No argument, payload or
 * token is kept in the witness.
 */
final class ConditionWaitAnchoredEvidence
{
    public const POLICY = 'occurrence';

    /** Deliveries larger than this are recorded as overflow and cannot prove visibility. */
    public const MAX_WITNESS_EVENTS = 512;

    /** Existing never-early firing and dispatch-latency bound (seconds after A). */
    public const DISPATCH_LATENCY = 2.0;

    /** The first-open relation |A - (O + T)| keeps the original 1 s tolerance. */
    public const FIRST_OPEN_TOLERANCE = 1.0;

    /** Native's four routes by which a workflow task attempt receives history. */
    public const ROUTES = ['poll', 'compressed_page', 'continuation_page', 'carried_task'];

    /**
     * The Worker's own replay input for an attempt, reported by the @internal
     * Worker::observeDeliveredWorkflowHistory hook: the exact history the
     * Replayer received after every page and refetch. One entry per attempt.
     */
    public const REPLAY_INPUT = 'replay_input';

    private const EVENT_KEYS = ['condition_wait_id', 'condition_wait_occurrence_id', 'condition_key', 'timer_id'];
    private const COMMAND_KEYS = ['condition_wait_occurrence_id', 'condition_key', 'timeout_seconds', 'deadline_policy',
        'condition_wait_timeout_budget_seconds'];

    /**
     * A bounded witness entry for one delivered slice of a workflow task
     * attempt (a claimed poll, a decoded compressed page, a continuation page
     * or a task carried in a completion response): event id, history
     * sequence, type, both time fields, the field the Replayer reads and
     * condition-wait identities only. The slice label identifies the page; it
     * is never the continuation token.
     *
     * @param list<mixed> $events the delivered history events
     * @return array<string, mixed>
     */
    public static function delivery(string $source, string $taskId, int $attempt, array $events, string $slice = '0'): array
    {
        self::check(in_array($source, self::ROUTES, true), 'anchored_witness_source');

        return self::slice($source, $taskId, $attempt, $events, $slice);
    }

    /**
     * A bounded witness entry for an attempt's complete replay input, with the
     * same kept fields as delivery().
     *
     * @param list<mixed> $history the Replayer input reported by the Worker
     * @return array<string, mixed>
     */
    public static function replayInput(string $taskId, int $attempt, array $history): array
    {
        return self::slice(self::REPLAY_INPUT, $taskId, $attempt, $history, 'replay');
    }

    /**
     * An observer for Worker::observeDeliveredWorkflowHistory that appends
     * bounded witness entries: 'history' as the replay input and 'completion'
     * as the completion commands. Any other kind is kept as an unknown entry,
     * which the anchored checks refuse.
     *
     * @param list<array<string, mixed>> $witness
     * @return \Closure(string, string, int, list<mixed>): void
     */
    public static function observer(array &$witness): \Closure
    {
        return static function (string $kind, string $taskId, int $attempt, array $items) use (&$witness): void {
            $witness[] = match ($kind) {
                'history' => self::replayInput($taskId, $attempt, $items),
                'completion' => self::completion($taskId, $attempt, $items),
                default => ['source' => 'unknown', 'kind' => $kind, 'task_id' => $taskId, 'attempt' => $attempt],
            };
        };
    }

    /**
     * @param list<mixed> $events
     * @return array<string, mixed>
     */
    private static function slice(string $source, string $taskId, int $attempt, array $events, string $slice): array
    {
        $entry = ['source' => $source, 'task_id' => $taskId, 'attempt' => $attempt, 'slice' => $slice,
            'overflow' => count($events) > self::MAX_WITNESS_EVENTS, 'events' => []];
        foreach (array_slice($events, 0, self::MAX_WITNESS_EVENTS) as $event) {
            if (!is_array($event)) {
                $entry['overflow'] = true;
                continue;
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $timestamp = self::scalar($event['timestamp'] ?? null);
            $recorded = self::scalar($event['recorded_at'] ?? null);
            $kept = ['id' => self::scalar($event['id'] ?? null), 'sequence' => self::scalar($event['sequence'] ?? null),
                'type' => self::scalar($event['event_type'] ?? $event['type'] ?? null), 'timestamp' => $timestamp, 'recorded_at' => $recorded,
                'time_field' => $timestamp !== null ? 'timestamp' : ($recorded !== null ? 'recorded_at' : null)];
            foreach (self::EVENT_KEYS as $key) {
                $kept[$key] = self::scalar($payload[$key] ?? null);
            }
            $entry['events'][] = $kept;
        }

        return $entry;
    }

    /**
     * A bounded witness entry for the commands a task attempt completed with:
     * command types and condition-wait identities and integers only.
     *
     * @param list<mixed> $commands
     * @return array<string, mixed>
     */
    public static function completion(string $taskId, int $attempt, array $commands): array
    {
        $entry = ['source' => 'completion', 'task_id' => $taskId, 'attempt' => $attempt, 'commands' => []];
        foreach (array_slice($commands, 0, self::MAX_WITNESS_EVENTS) as $command) {
            if (!is_array($command)) {
                continue;
            }
            $kept = ['type' => self::scalar($command['type'] ?? null)];
            foreach (self::COMMAND_KEYS as $key) {
                $kept[$key] = self::scalar($command[$key] ?? null);
            }
            $entry['commands'][] = $kept;
        }

        return $entry;
    }

    /** The Replayer's remaining-timeout arithmetic (src/Worker/Replayer.php remainingConditionReopen). */
    public static function authoredRemaining(int $timeout, float $openedAt, float $now): int
    {
        return max(0, min($timeout, (int) ceil($timeout - ($now - $openedAt))));
    }

    /**
     * An event instant with the Replayer's precedence (timestamp, then
     * recorded_at) and format: RFC 3339 with an explicit offset only.
     *
     * @param array<string, mixed> $event
     */
    public static function workerInstant(array $event): ?float
    {
        $value = $event['timestamp'] ?? $event['recorded_at'] ?? null;
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            return null;
        }
        try {
            return (float) (new DateTimeImmutable($value))->format('U.u');
        } catch (\Exception) {
            return null;
        }
    }

    /**
     * C1 anchored: every same-call false-wake reopen keeps the first stored
     * anchor exactly, carries the original budget, and authors the remaining
     * time the worker computed from what it actually received.
     *
     * @param list<array<string, mixed>> $history the run's recorded history
     * @param list<array<string, mixed>> $witness delivery and completion entries
     * @param bool $replayInputOnly require the Worker's replay input for every authoring attempt
     * @return array<string, mixed>
     */
    public static function falseWake(array $history, array $witness, string $key, int $timeout, int $reopens,
        bool $replayInputOnly = false): array
    {
        $events = self::events($history);
        $chains = self::chains($events);
        self::check(isset($chains[$key]) && count($chains) === 1, 'anchored_evidence_false_wake_shape');
        $chain = self::chain($events, $chains[$key], $timeout);
        self::check(count($chain['reopens']) === $reopens, 'anchored_evidence_reopen_count');
        $last = end($chain['waits']);
        self::check($last['fired_at'] !== null && $last['resolution'] === 'fired', 'anchored_evidence_reopen_not_fired');
        foreach (array_slice($chain['waits'], 0, -1) as $wait) {
            self::check($wait['resolution'] === 'satisfied' && $wait['timer_cancelled'], 'anchored_evidence_false_wake_resolution');
        }
        $anchor = self::instant($chain['anchor']);
        self::check($last['fired_at'] >= $anchor, 'anchored_evidence_fired_early');
        self::check($last['fired_at'] - $anchor <= self::DISPATCH_LATENCY, 'anchored_evidence_dispatch_latency');
        $authored = self::authored($events, $chain, $witness, $timeout, $replayInputOnly);
        self::check(self::count($events, 'WorkflowCompleted') === 1, 'anchored_evidence_not_completed');

        return ['anchor' => $chain['anchor'], 'budget' => $timeout, 'reopens' => $authored,
            'fired_after_anchor' => round($last['fired_at'] - $anchor, 6)];
    }

    /**
     * C4 anchored: after the first call's chain is satisfied, a newly authored
     * call is a fresh chain with its own anchor and budget, and its reopen keeps
     * that anchor and authors its own remaining time.
     *
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>> $witness
     * @param bool $replayInputOnly require the Worker's replay input for every authoring attempt
     * @return array<string, mixed>
     */
    public static function ownBudget(array $history, array $witness, string $firstKey, int $firstTimeout,
        string $secondKey, int $secondTimeout, bool $replayInputOnly = false): array
    {
        $events = self::events($history);
        $chains = self::chains($events);
        self::check(isset($chains[$firstKey], $chains[$secondKey]) && count($chains) === 2, 'anchored_evidence_own_budget_shape');
        $first = self::chain($events, $chains[$firstKey], $firstTimeout);
        $second = self::chain($events, $chains[$secondKey], $secondTimeout);
        self::check($first['reopens'] === [] && $first['waits'][0]['resolution'] === 'satisfied', 'anchored_evidence_first_wait');
        self::check($first['occurrence'] !== $second['occurrence'], 'anchored_evidence_occurrence_reused');
        self::check($second['anchor'] !== $first['anchor'], 'anchored_evidence_anchor_reused');
        self::check(count($second['reopens']) === 1, 'anchored_evidence_reopen_count');
        $authored = self::authored($events, $second, $witness, $secondTimeout, $replayInputOnly);
        self::check(self::count($events, 'WorkflowCompleted') === 1, 'anchored_evidence_not_completed');

        return ['first_anchor' => $first['anchor'], 'second_anchor' => $second['anchor'], 'second_budget' => $secondTimeout,
            'reopens' => $authored];
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array{type: string, recorded: ?float, payload: array<string, mixed>, id: string|int|null, sequence: string|int|null}>
     */
    private static function events(array $history): array
    {
        $events = [];
        foreach ($history as $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            self::check(is_string($type), 'anchored_evidence_event_shape');
            $stamp = $event['recorded_at'] ?? $event['timestamp'] ?? null;
            $events[] = ['type' => $type, 'recorded' => is_string($stamp) ? self::instant($stamp) : null,
                'payload' => is_array($event['payload'] ?? null) ? $event['payload'] : [],
                'id' => self::scalar($event['id'] ?? null), 'sequence' => self::scalar($event['sequence'] ?? null)];
        }

        return $events;
    }

    /**
     * Opens grouped by condition key, in order.
     *
     * @param list<array{type: string, recorded: ?float, payload: array<string, mixed>}> $events
     * @return array<string, list<int>>
     */
    private static function chains(array $events): array
    {
        $chains = [];
        foreach ($events as $index => $event) {
            if ($event['type'] === 'ConditionWaitOpened') {
                $key = $event['payload']['condition_key'] ?? null;
                self::check(is_string($key), 'anchored_evidence_wait_identity');
                $chains[$key][] = $index;
            }
        }

        return $chains;
    }

    /**
     * One anchored chain: the producer contract on every open, the first
     * stored anchor on every condition-timeout timer, and each wait's ending.
     *
     * @param list<array{type: string, recorded: ?float, payload: array<string, mixed>}> $events
     * @param list<int> $opens
     * @return array<string, mixed>
     */
    private static function chain(array $events, array $opens, int $timeout): array
    {
        $waits = [];
        $occurrence = null;
        foreach ($opens as $position => $index) {
            $payload = $events[$index]['payload'];
            self::check(($payload['deadline_policy'] ?? null) === self::POLICY, 'anchored_evidence_not_anchored');
            self::check(($payload['condition_wait_timeout_budget_seconds'] ?? null) === $timeout, 'anchored_evidence_budget');
            $id = $payload['condition_wait_id'] ?? null;
            $occurrence ??= $payload['condition_wait_occurrence_id'] ?? null;
            self::check(is_string($id) && is_string($occurrence)
                && ($payload['condition_wait_occurrence_id'] ?? null) === $occurrence, 'anchored_evidence_occurrence');
            self::check(is_int($payload['timeout_seconds'] ?? null)
                && ($position > 0 || $payload['timeout_seconds'] === $timeout), 'anchored_evidence_first_timeout');
            $waits[$id] = ['id' => $id, 'index' => $index, 'opened' => $events[$index]['recorded'], 'timeout' => $payload['timeout_seconds'],
                'deadline' => self::scalar($payload['occurrence_deadline_at'] ?? null), 'timer' => null, 'fire_at' => null,
                'timer_anchor' => null, 'timer_policy' => null, 'resolution' => null, 'fired_at' => null, 'timer_cancelled' => false];
        }
        $byTimer = [];
        foreach ($events as $event) {
            $payload = $event['payload'];
            $wait = $payload['condition_wait_id'] ?? null;
            if ($event['type'] === 'TimerScheduled' && ($payload['timer_kind'] ?? null) === 'condition_timeout'
                && is_string($wait) && isset($waits[$wait])) {
                self::check($waits[$wait]['timer'] === null && is_string($payload['timer_id'] ?? null), 'anchored_evidence_timer_identity');
                $waits[$wait]['timer'] = $payload['timer_id'];
                $waits[$wait]['fire_at'] = self::scalar($payload['fire_at'] ?? null);
                $waits[$wait]['timer_anchor'] = self::scalar($payload['occurrence_deadline_at'] ?? null);
                $waits[$wait]['timer_policy'] = self::scalar($payload['deadline_policy'] ?? null);
                $byTimer[$payload['timer_id']] = $wait;
            } elseif ($event['type'] === 'ConditionWaitSatisfied' && is_string($wait) && isset($waits[$wait])) {
                $waits[$wait]['resolution'] ??= 'satisfied';
            } elseif (in_array($event['type'], ['TimerFired', 'TimerCancelled'], true)
                && isset($byTimer[$payload['timer_id'] ?? ''])) {
                $owner = $byTimer[$payload['timer_id']];
                if ($event['type'] === 'TimerCancelled') {
                    $waits[$owner]['timer_cancelled'] = true;
                } else {
                    self::check($waits[$owner]['resolution'] === null, 'anchored_evidence_fired_after_resolution');
                    $waits[$owner]['resolution'] = 'fired';
                    $fired = self::scalar($payload['fired_at'] ?? null);
                    $waits[$owner]['fired_at'] = is_string($fired) ? self::instant($fired) : $event['recorded'];
                }
            }
        }
        $ordered = array_values($waits);
        $anchor = $ordered[0]['fire_at'];
        self::check(is_string($anchor) && self::instant($anchor) !== null && $ordered[0]['opened'] !== null, 'anchored_evidence_anchor_missing');
        self::check(abs(self::instant($anchor) - ($ordered[0]['opened'] + $timeout)) <= self::FIRST_OPEN_TOLERANCE, 'anchored_evidence_first_open_relation');
        foreach ($ordered as $wait) {
            self::check($wait['timer'] !== null, 'anchored_evidence_timer_missing');
            self::check($wait['timer_policy'] === self::POLICY, 'anchored_evidence_timer_policy');
            self::check($wait['timer_anchor'] !== null && $wait['deadline'] !== null, 'anchored_evidence_anchor_missing');
            self::check($wait['fire_at'] === $anchor && $wait['timer_anchor'] === $anchor && $wait['deadline'] === $anchor,
                'anchored_evidence_anchor_moved');
        }

        return ['occurrence' => $occurrence, 'anchor' => $anchor, 'waits' => $ordered, 'reopens' => array_slice($ordered, 1)];
    }

    /**
     * Bind each reopen to the one task attempt whose completion authored it,
     * and require its timeout to be the remaining time computed from the
     * events that attempt actually received.
     *
     * @param list<array{type: string, recorded: ?float, payload: array<string, mixed>, id: string|int|null, sequence: string|int|null}> $events
     * @param array<string, mixed> $chain
     * @param list<array<string, mixed>> $witness
     * @return list<array<string, mixed>>
     */
    private static function authored(array $events, array $chain, array $witness, int $timeout, bool $replayInputOnly): array
    {
        $completions = [];
        foreach ($witness as $entry) {
            self::check(in_array($entry['source'] ?? null, [...self::ROUTES, self::REPLAY_INPUT, 'completion'], true),
                'anchored_evidence_witness_shape');
            if (($entry['source'] ?? null) !== 'completion') {
                continue;
            }
            foreach ($entry['commands'] ?? [] as $command) {
                if (($command['type'] ?? null) === 'open_condition_wait'
                    && ($command['condition_wait_occurrence_id'] ?? null) === $chain['occurrence']) {
                    $completions[] = ['task_id' => $entry['task_id'] ?? null, 'attempt' => $entry['attempt'] ?? null, 'command' => $command];
                }
            }
        }
        self::check(count($completions) === count($chain['waits']), 'anchored_evidence_witness_unproven');
        $results = [];
        foreach ($chain['reopens'] as $index => $reopen) {
            $completion = $completions[$index + 1];
            $command = $completion['command'];
            self::check(is_string($completion['task_id']) && is_int($completion['attempt']), 'anchored_evidence_witness_unproven');
            self::check(($command['deadline_policy'] ?? null) === self::POLICY
                && ($command['condition_wait_timeout_budget_seconds'] ?? null) === $timeout, 'anchored_evidence_command_contract');
            $delivered = self::received($events, $witness, $completion['task_id'], $completion['attempt'], $reopen, $replayInputOnly);
            $now = null;
            $wake = null;
            $opened = null;
            $highest = null;
            foreach ($delivered as $event) {
                $at = self::workerInstant($event);
                if ($at === null) {
                    continue;
                }
                self::check(is_int($event['sequence'] ?? null), 'anchored_evidence_witness_unproven');
                if ($highest === null || $event['sequence'] > $highest['sequence']) {
                    $highest = $event;
                }
                // The Replayer takes the latest instant; equal instants resolve to the higher sequence.
                if ($now === null || $at > $now || ($at === $now && $event['sequence'] > $wake['sequence'])) {
                    $now = $at;
                    $wake = $event;
                }
                if ($opened === null && ($event['type'] ?? null) === 'ConditionWaitOpened'
                    && ($event['condition_wait_occurrence_id'] ?? null) === $chain['occurrence']) {
                    $opened = $at;
                }
            }
            self::check($now !== null && $opened !== null, 'anchored_evidence_witness_unproven');
            // The latest visible event is the highest history sequence actually delivered.
            self::check($wake['sequence'] === $highest['sequence'], 'anchored_evidence_witness_order');
            $expected = self::authoredRemaining($timeout, $opened, $now);
            self::check(($command['timeout_seconds'] ?? null) === $expected, 'anchored_evidence_authored_remaining');
            self::check($reopen['timeout'] === $expected, 'anchored_evidence_recorded_remaining');
            $results[] = ['task_id' => $completion['task_id'], 'attempt' => $completion['attempt'], 'wake_event' => $wake['type'] ?? null,
                'wake_sequence' => $wake['sequence'], 'wake_id' => $wake['id'] ?? null, 'time_field' => $wake['time_field'] ?? null,
                'opened' => sprintf('%.6F', $opened), 'now' => sprintf('%.6F', $now), 'authored' => $expected,
                'delivered_events' => count($delivered)];
        }

        return $results;
    }

    /**
     * The events one authoring attempt received. Transport slices (the four
     * routes) are concatenated as before. The Worker's replay input is used
     * alone and must be bounded exactly:
     * - exactly one replay-input entry for the attempt, and no transport slice
     *   for the same attempt (otherwise ambiguous);
     * - not truncated (overflow is incomplete evidence);
     * - sequences exactly 1..n, so no page or event is missing or repeated;
     * - each event the recorded event at that position (same id, sequence and
     *   type): the input is a prefix of the run's history;
     * - it ends before the reopen this attempt authored (an input that
     *   already contains that open or anything after it is over-bound).
     * In replay-input-only mode the attempt must have a replay input.
     *
     * @param list<array{type: string, recorded: ?float, payload: array<string, mixed>, id: string|int|null, sequence: string|int|null}> $events
     * @param list<array<string, mixed>> $witness
     * @param array<string, mixed> $reopen
     * @return list<array<string, mixed>>
     */
    private static function received(array $events, array $witness, string $taskId, int $attempt, array $reopen, bool $replayInputOnly): array
    {
        $slices = [];
        $inputs = [];
        foreach ($witness as $entry) {
            if (($entry['task_id'] ?? null) !== $taskId || ($entry['attempt'] ?? null) !== $attempt) {
                continue;
            }
            if (($entry['source'] ?? null) === self::REPLAY_INPUT) {
                $inputs[] = $entry;
            } elseif (in_array($entry['source'] ?? null, self::ROUTES, true)) {
                $slices[] = $entry;
            }
        }
        if ($inputs === []) {
            self::check(!$replayInputOnly, 'anchored_evidence_replay_input_missing');
            $delivered = [];
            foreach ($slices as $entry) {
                self::check(($entry['overflow'] ?? true) === false, 'anchored_evidence_witness_unproven');
                array_push($delivered, ...($entry['events'] ?? []));
            }
            self::check($delivered !== [], 'anchored_evidence_witness_unproven');

            return $delivered;
        }
        self::check(count($inputs) === 1 && $slices === [], 'anchored_evidence_replay_input_ambiguous');
        $input = $inputs[0];
        self::check(($input['overflow'] ?? true) === false, 'anchored_evidence_replay_input_incomplete');
        $delivered = $input['events'] ?? [];
        self::check(is_array($delivered) && $delivered !== [], 'anchored_evidence_replay_input_incomplete');
        foreach (array_values($delivered) as $position => $event) {
            self::check(($event['sequence'] ?? null) === $position + 1, 'anchored_evidence_replay_input_incomplete');
            $recorded = $events[$position] ?? null;
            self::check($recorded !== null && $recorded['sequence'] === $position + 1 && $recorded['id'] === ($event['id'] ?? null)
                && $recorded['type'] === ($event['type'] ?? null), 'anchored_evidence_replay_input_mismatch');
        }
        self::check(count($delivered) <= $reopen['index'], 'anchored_evidence_replay_input_over_bound');

        return array_values($delivered);
    }

    private static function instant(string $stamp): ?float
    {
        $at = DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.uP', $stamp)
            ?: DateTimeImmutable::createFromFormat(DATE_ATOM, $stamp);

        return $at === false ? null : (float) $at->format('U.u');
    }

    private static function scalar(mixed $value): string|int|null
    {
        return is_string($value) || is_int($value) ? $value : null;
    }

    /** @param list<array{type: string, recorded: ?float, payload: array<string, mixed>}> $events */
    private static function count(array $events, string $type): int
    {
        return count(array_filter($events, static fn (array $e): bool => $e['type'] === $type));
    }

    private static function check(bool $condition, string $code): void
    {
        if (!$condition) {
            throw new RuntimeException($code);
        }
    }
}
