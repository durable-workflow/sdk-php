<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DateTimeImmutable;
use DurableWorkflow\Worker;
use RuntimeException;

/**
 * History assertions for the installed native condition-wait qualifier (#269).
 *
 * Pure functions over one run's history page, using the native event shapes
 * of Server 3.2.0: each open is a new condition_wait_id, a condition timeout is
 * a TimerScheduled with timer_kind condition_timeout that carries the wait's
 * condition_wait_id, and a timeout ends with TimerFired (native never records
 * ConditionWaitTimedOut). Physical database state is Server-owned and checked
 * separately.
 */
final class ConditionWaitNativeEvidence
{
    /**
     * Signals each qualifier workflow type receives. Native enforces the
     * durable command contract, so a signal the worker does not declare for a
     * workflow type is refused ("declared signals []").
     */
    public const DECLARED_SIGNALS = [
        'cwn.false_wake' => ['go'],
        'cwn.duplicate' => ['noise', 'go'],
        'cwn.close' => ['never'],
        'cwn.own_budget' => ['first', 'second'],
        'cwn.turns' => ['decision'],
    ];

    /** Declare the qualifier's signals through the public Worker API. */
    public static function declareSignals(Worker $worker): void
    {
        foreach (self::DECLARED_SIGNALS as $workflowType => $signals) {
            foreach ($signals as $signal) {
                $worker->declareSignal($workflowType, $signal);
            }
        }
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array{type: string, at: float, payload: array<string, mixed>}>
     */
    public static function events(array $history): array
    {
        $events = [];
        foreach ($history as $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            $stamp = $event['recorded_at'] ?? $event['timestamp'] ?? null;
            if (!is_string($type) || !is_string($stamp)) {
                throw new RuntimeException('condition_evidence_event_shape');
            }
            $at = DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.uP', $stamp)
                ?: DateTimeImmutable::createFromFormat(DATE_ATOM, $stamp);
            if ($at === false) {
                throw new RuntimeException('condition_evidence_event_time');
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $events[] = ['type' => $type, 'at' => (float) $at->format('U.u'), 'payload' => $payload];
        }

        return $events;
    }

    /**
     * Condition waits in open order. A second open for a key while an earlier
     * open of that key is unresolved in history is an overlapping physical wait.
     *
     * @param list<array{type: string, at: float, payload: array<string, mixed>}> $events
     * @return list<array{id: string, key: ?string, opened_at: float, timeout: ?int, resolution: ?string,
     *     resolved_at: ?float, timer_id: ?string, timer_cancelled: bool}>
     */
    public static function waits(array $events): array
    {
        $waits = [];
        $byTimer = [];
        $bySequence = [];
        foreach ($events as $event) {
            $payload = $event['payload'];
            switch ($event['type']) {
                case 'ConditionWaitOpened':
                    $id = $payload['condition_wait_id'] ?? null;
                    if (!is_string($id) || $id === '' || isset($waits[$id])) {
                        throw new RuntimeException('condition_evidence_wait_identity');
                    }
                    $key = is_string($payload['condition_key'] ?? null) ? $payload['condition_key'] : null;
                    foreach ($waits as $wait) {
                        if ($wait['key'] === $key && $wait['resolution'] === null) {
                            throw new RuntimeException('condition_evidence_overlapping_open');
                        }
                    }
                    $timeout = $payload['timeout_seconds'] ?? null;
                    $waits[$id] = ['id' => $id, 'key' => $key, 'opened_at' => $event['at'],
                        'timeout' => is_int($timeout) ? $timeout : null, 'resolution' => null,
                        'resolved_at' => null, 'timer_id' => null, 'timer_cancelled' => false];
                    break;
                case 'ConditionWaitSatisfied':
                    $id = $payload['condition_wait_id'] ?? null;
                    if (!is_string($id) || !isset($waits[$id]) || $waits[$id]['resolution'] !== null) {
                        throw new RuntimeException('condition_evidence_satisfied_unknown');
                    }
                    $waits[$id]['resolution'] = 'satisfied';
                    $waits[$id]['resolved_at'] = $event['at'];
                    break;
                case 'TimerScheduled':
                    if (($payload['timer_kind'] ?? null) !== 'condition_timeout') {
                        break;
                    }
                    $id = $payload['condition_wait_id'] ?? null;
                    $timer = $payload['timer_id'] ?? null;
                    if (!is_string($id) || !isset($waits[$id]) || !is_string($timer)) {
                        throw new RuntimeException('condition_evidence_timer_identity');
                    }
                    $waits[$id]['timer_id'] = $timer;
                    $byTimer[$timer] = $id;
                    if (is_int($payload['sequence'] ?? null)) {
                        $bySequence[$payload['sequence']] = $id;
                    }
                    break;
                case 'TimerFired':
                case 'TimerCancelled':
                    $timer = $payload['timer_id'] ?? null;
                    $id = is_string($timer) && isset($byTimer[$timer]) ? $byTimer[$timer]
                        : (is_int($payload['sequence'] ?? null) ? ($bySequence[$payload['sequence']] ?? null) : null);
                    if ($id === null) {
                        break;
                    }
                    if ($event['type'] === 'TimerCancelled') {
                        $waits[$id]['timer_cancelled'] = true;
                        break;
                    }
                    if ($waits[$id]['resolution'] !== null) {
                        throw new RuntimeException('condition_evidence_fired_after_resolution');
                    }
                    $waits[$id]['resolution'] = 'fired';
                    $waits[$id]['resolved_at'] = $event['at'];
                    break;
            }
        }

        return array_values($waits);
    }

    /**
     * C1: a false wake keeps the first open's deadline.
     *
     * @param list<array{type: string, at: float, payload: array<string, mixed>}> $events
     * @return array<string, mixed>
     */
    public static function falseWake(array $events, int $timeout, float $tolerance = 1.0, float $fireTolerance = 2.0): array
    {
        $waits = self::waits($events);
        self::check(count($waits) === 2 && $waits[0]['key'] === $waits[1]['key'], 'condition_evidence_false_wake_shape');
        [$first, $reopen] = $waits;
        self::check($first['timeout'] === $timeout, 'condition_evidence_first_timeout');
        self::check($first['resolution'] === 'satisfied' && $first['timer_cancelled'], 'condition_evidence_false_wake_resolution');
        self::check(self::count($events, 'SignalReceived', $first['opened_at'], (float) $first['resolved_at']) >= 1, 'condition_evidence_false_wake_signal');
        $remaining = $timeout - ((float) $first['resolved_at'] - $first['opened_at']);
        self::check(is_int($reopen['timeout']) && abs($reopen['timeout'] - $remaining) <= $tolerance, 'condition_evidence_reopen_budget');
        self::check($reopen['resolution'] === 'fired', 'condition_evidence_reopen_not_fired');
        $deadline = $first['opened_at'] + $timeout;
        self::check(abs((float) $reopen['resolved_at'] - $deadline) <= $fireTolerance, 'condition_evidence_deadline_moved');
        self::check(self::count($events, 'WorkflowCompleted') === 1, 'condition_evidence_not_completed');

        return ['first_timeout' => $first['timeout'], 'reopen_timeout' => $reopen['timeout'],
            'remaining_at_wake' => round($remaining, 3), 'fired_after_first_open' => round((float) $reopen['resolved_at'] - $first['opened_at'], 3)];
    }

    /**
     * C2/C5 run: completed, no overlapping open, and every wait ended by a
     * recorded satisfaction or its condition timeout.
     *
     * @param list<array{type: string, at: float, payload: array<string, mixed>}> $events
     * @return array<string, mixed>
     */
    public static function settledRun(array $events, string $close = 'WorkflowCompleted'): array
    {
        $waits = self::waits($events);
        self::check(self::count($events, $close) === 1, 'condition_evidence_close_missing');
        foreach ($waits as $wait) {
            self::check(in_array($wait['resolution'], ['satisfied', 'fired'], true), 'condition_evidence_unresolved_wait');
        }

        return ['waits' => count($waits), 'satisfied' => count(array_filter($waits, static fn (array $w): bool => $w['resolution'] === 'satisfied'))];
    }

    /**
     * C3: a run closed by terminate or cancel with an open wait records a
     * TimerCancelled for every pending condition timer. Continue-as-new after a
     * fired timeout needs no timer event; physical settlement is Server-owned.
     *
     * @param list<array{type: string, at: float, payload: array<string, mixed>}> $events
     * @return array<string, mixed>
     */
    public static function closedWithOpenWait(array $events, string $close): array
    {
        $waits = self::waits($events);
        $type = ['terminate' => 'WorkflowTerminated', 'cancel' => 'WorkflowCancelled', 'continue' => 'WorkflowContinuedAsNew'][$close]
            ?? throw new RuntimeException('condition_evidence_close_kind');
        self::check(self::count($events, $type) === 1 && $waits !== [], 'condition_evidence_close_missing');
        foreach ($waits as $wait) {
            if ($close === 'continue') {
                self::check($wait['resolution'] === 'fired', 'condition_evidence_continue_without_timeout');
            } else {
                self::check($wait['resolution'] === null && $wait['timer_id'] !== null && $wait['timer_cancelled'], 'condition_evidence_close_timer_not_cancelled');
            }
        }

        return ['waits' => count($waits), 'close' => $type];
    }

    /**
     * C4: after an earlier wait's consumed terminal, a newly authored wait keeps
     * its own budget when it reopens after a false wake.
     *
     * @param list<array{type: string, at: float, payload: array<string, mixed>}> $events
     * @return array<string, mixed>
     */
    public static function ownBudget(array $events, string $firstKey, string $secondKey, int $secondTimeout, float $tolerance = 1.0): array
    {
        $waits = self::waits($events);
        $first = array_values(array_filter($waits, static fn (array $w): bool => $w['key'] === $firstKey));
        $second = array_values(array_filter($waits, static fn (array $w): bool => $w['key'] === $secondKey));
        self::check(count($first) === 1 && $first[0]['resolution'] === 'satisfied', 'condition_evidence_first_wait');
        self::check(count($second) === 2 && $second[0]['opened_at'] >= (float) $first[0]['resolved_at'], 'condition_evidence_second_wait_shape');
        self::check($second[0]['timeout'] === $secondTimeout && $second[0]['resolution'] === 'satisfied', 'condition_evidence_second_first_open');
        $remaining = $secondTimeout - ((float) $second[0]['resolved_at'] - $second[0]['opened_at']);
        self::check(is_int($second[1]['timeout']) && abs($second[1]['timeout'] - $remaining) <= $tolerance, 'condition_evidence_own_budget');
        self::check(self::count($events, 'WorkflowCompleted') === 1, 'condition_evidence_not_completed');

        return ['second_timeout' => $secondTimeout, 'reopen_timeout' => $second[1]['timeout'], 'remaining_at_wake' => round($remaining, 3)];
    }

    /** @param list<array{type: string, at: float, payload: array<string, mixed>}> $events */
    private static function count(array $events, string $type, ?float $from = null, ?float $to = null): int
    {
        return count(array_filter($events, static fn (array $e): bool => $e['type'] === $type
            && ($from === null || $e['at'] >= $from) && ($to === null || $e['at'] <= $to)));
    }

    private static function check(bool $condition, string $code): void
    {
        if (!$condition) {
            throw new RuntimeException($code);
        }
    }
}
