<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;

/** @internal Proves one consumed physical condition wait, including its timeout pair. */
final class ConditionReplayClockEvidence
{
    private const TERMINALS = ['ConditionWaitSatisfied', 'ConditionWaitTimedOut'];

    /** @param list<array<string, mixed>> $history
     * @param (Closure(array<string, mixed>, string): bool)|null $rowMatches
     */
    public static function matches(array $history, int $index, int $sequence, ?Closure $rowMatches = null): bool
    {
        $event = $history[$index];
        $payload = $event['payload'] ?? null;
        $waitId = is_array($payload) ? ($payload['condition_wait_id'] ?? null) : null;
        if (!is_array($payload) || !ReplayClockHistory::identity($waitId) || ReplayClockHistory::sequence($payload) !== $sequence) {
            return false;
        }
        $rows = self::linkedRows($history, $sequence, $waitId, $rowMatches ?? ReplayClockHistory::scalarRow(...));

        return $rows !== null && self::lifecycle($history, $rows, $index);
    }

    /** @param list<array<string, mixed>> $history
     * @param array<string, list<int>> $rows
     */
    private static function lifecycle(array $history, array $rows, int $index): bool
    {
        $terminals = array_merge(...array_values(array_intersect_key($rows, array_flip(self::TERMINALS))));
        if (count($rows['ConditionWaitOpened'] ?? []) !== 1 || count($terminals) !== 1) {
            return false;
        }
        $opened = $rows['ConditionWaitOpened'][0];
        $terminal = $terminals[0];
        return $opened < $terminal && self::sameDefinition($history[$opened]['payload'], $history[$terminal]['payload'])
            && (ReplayClockHistory::kind($history[$terminal]) !== 'ConditionWaitTimedOut'
                || is_int($history[$opened]['payload']['timeout_seconds'] ?? null))
            && self::timerPair($history, $rows, $opened, $terminal, $index);
    }

    /** @param list<array<string, mixed>> $history
     * @param Closure(array<string, mixed>, string): bool $rowMatches
     * @return array<string, list<int>>|null
     */
    private static function linkedRows(array $history, int $sequence, string $waitId, Closure $rowMatches): ?array
    {
        $positions = [];
        $allowed = ['ConditionWaitOpened', ...self::TERMINALS, 'TimerScheduled', 'TimerFired', 'TimerCancelled'];
        foreach ($history as $position => $row) {
            $body = $row['payload'] ?? null;
            $kind = ReplayClockHistory::kind($row);
            // Signal receipt may wake a predicate, but never supplies its clock boundary.
            if (!is_array($body) || $kind === 'SignalReceived'
                || (ReplayClockHistory::legacySequenceClaim($body) !== $sequence && ($body['condition_wait_id'] ?? null) !== $waitId)) {
                continue;
            }
            // Core's condition-timeout timers carry the wait identity, without
            // the owning condition's parallel path. The complete timer pair
            // must remain scalar and correlate with that validated wait.
            $rowQualified = str_starts_with((string) $kind, 'Timer')
                ? ReplayClockHistory::scalarRow($body, 'condition_wait') : $rowMatches($body, 'condition_wait');
            if (!in_array($kind, $allowed, true) || ReplayClockHistory::sequence($body) !== $sequence
                || ($body['condition_wait_id'] ?? null) !== $waitId || !$rowQualified
                || ($body['cancellation_scope'] ?? null) !== null) {
                return null;
            }
            if (str_starts_with($kind, 'Timer') && (($body['timer_kind'] ?? null) !== 'condition_timeout'
                || !ReplayClockHistory::identity($body['timer_id'] ?? null))) {
                return null;
            }
            $positions[$kind][] = $position;
        }

        return $positions;
    }

    /** @param array<string, mixed> $opened
     * @param array<string, mixed> $terminal
     */
    private static function sameDefinition(array $opened, array $terminal): bool
    {
        foreach (['condition_key', 'condition_definition_fingerprint', 'timeout_seconds'] as $field) {
            if (($opened[$field] ?? null) !== ($terminal[$field] ?? null)) {
                return false;
            }
        }
        if (array_key_exists('condition_wait_occurrence_id', $terminal)
            && $terminal['condition_wait_occurrence_id'] !== ($opened['condition_wait_occurrence_id'] ?? null)) {
            return false;
        }
        $timeout = $opened['timeout_seconds'] ?? null;

        return $timeout === null || (is_int($timeout) && $timeout >= 0);
    }

    /** @param list<array<string, mixed>> $history
     * @param array<string, list<int>> $rows
     */
    private static function timerPair(array $history, array $rows, int $opened, int $terminal, int $selected): bool
    {
        $scheduled = $rows['TimerScheduled'] ?? [];
        $fired = $rows['TimerFired'] ?? [];
        $cancelled = $rows['TimerCancelled'] ?? [];
        $terminalBody = $history[$terminal]['payload'];
        if ($scheduled === [] && $fired === [] && $cancelled === []) {
            return $selected === $terminal && ($terminalBody['timer_id'] ?? null) === null;
        }
        if (count($scheduled) !== 1 || count($fired) + count($cancelled) !== 1) {
            return false;
        }
        $timerEnd = ($fired ?: $cancelled)[0];

        return self::timerLink($history, $scheduled[0], $timerEnd, $opened, $terminal)
            && self::selectedInstant($history, $terminal, $selected, $timerEnd, $fired !== []);
    }

    /** @param list<array<string, mixed>> $history */
    private static function timerLink(array $history, int $scheduled, int $timerEnd, int $opened, int $terminal): bool
    {
        $terminalBody = $history[$terminal]['payload'];
        $timer = $history[$scheduled]['payload'];
        $opening = $history[$opened]['payload'];
        $timeout = $opening['timeout_seconds'] ?? null;

        return $scheduled > $opened && $timerEnd > $scheduled && $timerEnd < $terminal
            && is_int($timeout) && ($timer['delay_seconds'] ?? null) === $timeout
            && ($terminalBody['timer_id'] ?? null) === $timer['timer_id']
            && $history[$timerEnd]['payload']['timer_id'] === $timer['timer_id']
            && self::timerDefinition($opening, $timer) && self::timerDefinition($opening, $history[$timerEnd]['payload']);
    }

    /** @param list<array<string, mixed>> $history */
    private static function selectedInstant(array $history, int $terminal, int $selected, int $timerEnd, bool $fired): bool
    {
        $timedOut = ReplayClockHistory::kind($history[$terminal]) === 'ConditionWaitTimedOut';
        if ($timedOut !== $fired) {
            return false;
        }
        if ($selected === $terminal) {
            return true;
        }
        // Core's first timeout send and later terminal replay must prove the same instant.
        $time = ReplayClockHistory::recordedTime($history[$terminal]);

        return $timedOut && $selected === $timerEnd && $time !== null
            && $time == ReplayClockHistory::recordedTime($history[$timerEnd]);
    }

    /** @param array<string, mixed> $opened
     * @param array<string, mixed> $timer
     */
    private static function timerDefinition(array $opened, array $timer): bool
    {
        foreach (['condition_key', 'condition_definition_fingerprint', 'condition_wait_occurrence_id'] as $field) {
            if (array_key_exists($field, $timer) && $timer[$field] !== ($opened[$field] ?? null)) {
                return false;
            }
        }

        return true;
    }
}
