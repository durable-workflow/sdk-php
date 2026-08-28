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
     * @param list<array<string, mixed>> $runtimeHistory
     * @return list<array<string, mixed>>|null Null when authoritative cold history is required.
     */
    public function history(
        string $workflowId,
        string $runId,
        string $buildId,
        array $runtimeHistory,
        ?string $replayMode,
    ): ?array {
        $now = ($this->clock)();
        $this->evictExpired($now);
        $key = self::key($workflowId, $runId, $buildId);
        $entry = $this->entries[$key] ?? null;
        $stickyHitExpected = $replayMode === 'sticky_hit_expected';
        $forcedColdReplay = $replayMode === 'forced_cold_replay';
        $runtimeHistoryIsComplete = self::startsWithWorkflowStart($runtimeHistory);
        $cachedHistoryIsComplete = $entry !== null && self::startsWithWorkflowStart($entry['history']);
        if ($forcedColdReplay || ! $stickyHitExpected || ! $cachedHistoryIsComplete || $runtimeHistoryIsComplete) {
            ++$this->metrics['miss'];
            if ($forcedColdReplay || $stickyHitExpected) {
                ++$this->metrics['forced_cold_replay'];
            }

            // A runtime suffix is only replayable with a validated cached
            // prefix. Tell the worker to fetch authoritative history instead
            // of treating the suffix as a complete durable history.
            if (! $runtimeHistoryIsComplete && ($stickyHitExpected || $forcedColdReplay)) {
                return null;
            }

            return $runtimeHistory;
        }

        ++$this->metrics['hit'];
        $this->entries[$key]['touched_at'] = $now;

        // Only a strict suffix consumes the cached durable prefix. Complete
        // authoritative history took the cold-replay branch above.
        return array_merge($entry['history'], $runtimeHistory);
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

    /** @param list<array<string, mixed>> $history */
    private static function startsWithWorkflowStart(array $history): bool
    {
        $first = $history[0] ?? null;

        return is_array($first) && ($first['event_type'] ?? $first['type'] ?? null) === 'WorkflowStarted';
    }

    private static function key(string $workflowId, string $runId, string $buildId): string
    {
        if ($workflowId === '' || $runId === '' || $buildId === '') {
            throw new \InvalidArgumentException('Sticky cache keys require workflow, run, and build identities.');
        }

        return hash('sha256', $workflowId."\0".$runId."\0".$buildId);
    }
}
