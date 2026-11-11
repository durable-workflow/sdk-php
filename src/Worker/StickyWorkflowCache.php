<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * Bounded process-local cache keyed by exact workflow/run/build identity.
 *
 * Values are complete durable histories, never correctness state. A miss,
 * expiry, eviction, worker replacement, or build mismatch therefore falls
 * back to the history supplied by the runtime and a full replay.
 */
final class StickyWorkflowCache
{
    public const MAX_TTL_SECONDS = 3600;

    /** @var array<string, array{history: list<array<string, mixed>>, expires_at: float, touched_at: float}> */
    private array $entries = [];

    /** @var array{hit: int, miss: int, eviction: int, forced_cold_replay: int} */
    private array $metrics = ['hit' => 0, 'miss' => 0, 'eviction' => 0, 'forced_cold_replay' => 0];

    /** @var \Closure(): float */
    private readonly \Closure $clock;

    public function __construct(
        private readonly int $capacity = 100,
        private readonly int $ttlSeconds = 300,
        ?\Closure $clock = null,
    ) {
        if ($capacity < 1 || $ttlSeconds < 1) {
            throw new \InvalidArgumentException('Sticky cache capacity and TTL must be positive integers.');
        }
        if ($ttlSeconds > self::MAX_TTL_SECONDS) {
            throw new \InvalidArgumentException('Sticky cache TTL must not exceed 3600 seconds.');
        }

        $this->clock = $clock ?? static fn (): float => microtime(true);
    }

    /** @param list<array<string, mixed>> $durableHistory */
    public function remember(string $workflowId, string $runId, string $buildId, array $durableHistory): void
    {
        $now = ($this->clock)();
        $key = self::key($workflowId, $runId, $buildId);
        $this->entries[$key] = [
            'history' => $durableHistory,
            'expires_at' => $now + $this->ttlSeconds,
            'touched_at' => $now,
        ];
        $this->evictExpired($now);
        while (count($this->entries) > $this->capacity) {
            uasort($this->entries, static fn (array $left, array $right): int => $left['touched_at'] <=> $right['touched_at']);
            array_shift($this->entries);
            ++$this->metrics['eviction'];
        }
    }

    /**
     * $afterSequence and $lastSequence are the poll answer's sticky_history_suffix v1 fields:
     * 0 marks full history, N > 0 marks events after sequence N only.
     *
     * @param list<array<string, mixed>> $runtimeHistory
     * @return list<array<string, mixed>>|null Null when authoritative cold history is required.
     */
    public function history(
        string $workflowId,
        string $runId,
        string $buildId,
        array $runtimeHistory,
        ?string $replayMode,
        ?int $afterSequence = null,
        ?int $lastSequence = null,
    ): ?array {
        $now = ($this->clock)();
        $this->evictExpired($now);
        $key = self::key($workflowId, $runId, $buildId);
        $entry = $this->entries[$key] ?? null;
        $stickyHitExpected = $replayMode === 'sticky_hit_expected';
        $forcedColdReplay = $replayMode === 'forced_cold_replay';
        $runtimeHistoryIsComplete = self::startsWithWorkflowStart($runtimeHistory) && ($afterSequence ?? 0) === 0;
        $cachedHistoryIsComplete = $entry !== null && self::startsWithWorkflowStart($entry['history']);
        if ($forcedColdReplay || ! $stickyHitExpected || ! $cachedHistoryIsComplete || $runtimeHistoryIsComplete) {
            ++$this->metrics['miss'];
            if ($forcedColdReplay || $stickyHitExpected) {
                ++$this->metrics['forced_cold_replay'];
            }

            // A runtime suffix is only replayable with a validated cached
            // prefix. Tell the worker to fetch authoritative history instead
            // of treating the suffix as a complete durable history.
            if (! $runtimeHistoryIsComplete && ($stickyHitExpected || $forcedColdReplay || ($afterSequence ?? 0) > 0)) {
                return null;
            }

            return $runtimeHistory;
        }

        // A suffix must continue the cached prefix exactly: it starts right after the
        // cached last sequence, has no gaps, and ends where the answer says it does.
        // Anything else would replay a history the Server never recorded.
        $cachedLast = self::sequenceOf($entry['history'][array_key_last($entry['history'])]);
        $rejected = $afterSequence !== null
            // sticky_history_suffix v1: the Server named the boundary, so it must match exactly.
            ? $cachedLast !== $afterSequence || !self::continues($runtimeHistory, $afterSequence, $lastSequence)
            // Older answers carry no boundary. Refuse only a suffix whose own sequences contradict the cache.
            : $runtimeHistory !== [] && $cachedLast !== null && !self::continues($runtimeHistory, $cachedLast, null);
        if ($rejected) {
            ++$this->metrics['miss'];
            ++$this->metrics['forced_cold_replay'];
            unset($this->entries[$key]);

            return null;
        }

        ++$this->metrics['hit'];
        $this->entries[$key]['touched_at'] = $now;

        return array_merge($entry['history'], $runtimeHistory);
    }

    /** @param list<array<string, mixed>> $suffix */
    private static function continues(array $suffix, int $after, ?int $last): bool
    {
        if ($suffix === []) {
            return false;
        }
        $expected = $after + 1;
        foreach ($suffix as $event) {
            if (self::sequenceOf($event) !== $expected) {
                return false;
            }
            ++$expected;
        }

        return $last === null || $last === $expected - 1;
    }

    /** @param array<string, mixed> $event */
    private static function sequenceOf(array $event): ?int
    {
        $sequence = $event['sequence'] ?? null;

        return is_int($sequence) && $sequence > 0 ? $sequence : null;
    }

    /** The last sequence of the cached history for this run, or null when none is cached or it has no sequences. */
    public function lastSequence(string $workflowId, string $runId, string $buildId): ?int
    {
        $history = $this->entries[self::key($workflowId, $runId, $buildId)]['history'] ?? [];

        return $history === [] ? null : self::sequenceOf($history[array_key_last($history)]);
    }

    /** @return array{hit: int, miss: int, eviction: int, forced_cold_replay: int} */
    public function metrics(): array
    {
        return $this->metrics;
    }

    public function ttlSeconds(): int
    {
        return $this->ttlSeconds;
    }

    public function clear(): void
    {
        $this->entries = [];
    }

    private function evictExpired(float $now): void
    {
        foreach ($this->entries as $key => $entry) {
            if ($entry['expires_at'] <= $now) {
                unset($this->entries[$key]);
                ++$this->metrics['eviction'];
            }
        }
    }

    /** @internal
     *  @param list<array<string, mixed>> $history
     */
    public static function startsWithWorkflowStart(array $history): bool
    {
        $first = $history[0] ?? null;
        if (!is_array($first)) {
            return false;
        }
        $kind = $first['event_type'] ?? $first['type'] ?? null;
        if ($kind === 'WorkflowStarted') {
            return true;
        }
        // New runs retain the accepted start command before the start event.
        $second = $history[1] ?? null;
        return $kind === 'StartAccepted' && is_array($second)
            && ($second['event_type'] ?? $second['type'] ?? null) === 'WorkflowStarted';
    }

    private static function key(string $workflowId, string $runId, string $buildId): string
    {
        if ($workflowId === '' || $runId === '' || $buildId === '') {
            throw new \InvalidArgumentException('Sticky cache keys require workflow, run, and build identities.');
        }

        return hash('sha256', $workflowId."\0".$runId."\0".$buildId);
    }
}
