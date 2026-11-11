<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\StickyWorkflowCache;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** sdk-php#233 / server#1017: sticky_history_suffix v1 acceptance vectors. */
final class StickyHistorySuffixTest extends TestCase
{
    public function testAContiguousSuffixReplaysAfterTheCachedPrefix(): void
    {
        $cache = self::cacheWith(12);

        $history = $cache->history('wf', 'run', 'build', self::events(13, 15), 'sticky_hit_expected', 12, 15);

        self::assertSame(range(1, 15), array_column($history ?? [], 'sequence'));
        self::assertSame(1, $cache->metrics()['hit']);
    }

    /** @return iterable<string, array{int, list<array<string, mixed>>, int, ?int}> */
    public static function rejectedSuffixes(): iterable
    {
        yield 'gap: cache ends before the suffix' => [11, self::events(13, 15), 12, 15];
        yield 'overlap: cache already has the first suffix event' => [13, self::events(13, 15), 12, 15];
        yield 'non-contiguous suffix' => [12, [...self::events(13, 13), ...self::events(15, 15)], 12, 15];
        yield 'suffix ends short of the advertised last sequence' => [12, self::events(13, 14), 12, 15];
        yield 'empty suffix' => [12, [], 12, 12];
        yield 'cache without sequences' => [0, self::events(13, 15), 12, 15];
    }

    /** @param list<array<string, mixed>> $suffix */
    #[DataProvider('rejectedSuffixes')]
    public function testAnyOtherSuffixForcesAnAuthoritativeRefetch(int $cachedLast, array $suffix, int $after, ?int $last): void
    {
        $cache = self::cacheWith($cachedLast);

        self::assertNull($cache->history('wf', 'run', 'build', $suffix, 'sticky_hit_expected', $after, $last));
        self::assertSame(1, $cache->metrics()['forced_cold_replay']);
        // The rejected entry is dropped, so the next suffix can't build on it either.
        self::assertNull($cachedLast === 0 ? null : $cache->history('wf', 'run', 'build', self::events($cachedLast + 1, $cachedLast + 1), 'sticky_hit_expected', $cachedLast, $cachedLast + 1));
    }

    public function testAColdFullAnswerReplacesTheCache(): void
    {
        $cache = self::cacheWith(12);

        $history = $cache->history('wf', 'run', 'build', self::events(1, 15), 'forced_cold_replay', 0, 15);

        self::assertSame(range(1, 15), array_column($history ?? [], 'sequence'));
        self::assertSame(1, $cache->metrics()['forced_cold_replay']);
    }

    public function testASuffixOutsideAStickyHitIsNeverTreatedAsFullHistory(): void
    {
        foreach (['cold_replay', null] as $mode) {
            self::assertNull(self::cacheWith(12)->history('wf', 'run', 'build', self::events(13, 15), $mode, 12, 15));
        }
    }

    public function testALegacySuffixWithoutTheNewFieldsMustStillContinueTheCache(): void
    {
        self::assertSame(range(1, 14), array_column(
            self::cacheWith(12)->history('wf', 'run', 'build', self::events(13, 14), 'sticky_hit_expected') ?? [], 'sequence'));
        self::assertNull(self::cacheWith(12)->history('wf', 'run', 'build', self::events(14, 15), 'sticky_hit_expected'));
    }

    private static function cacheWith(int $last): StickyWorkflowCache
    {
        $cache = new StickyWorkflowCache(clock: static fn (): float => 0.0);
        $cache->remember('wf', 'run', 'build', $last === 0
            ? [['event_type' => 'WorkflowStarted', 'payload' => []]]
            : self::events(1, $last));

        return $cache;
    }

    /** @return list<array<string, mixed>> */
    private static function events(int $from, int $to): array
    {
        $events = [];
        for ($sequence = $from; $sequence <= $to; ++$sequence) {
            $events[] = ['sequence' => $sequence, 'event_type' => $sequence === 1 ? 'WorkflowStarted' : 'ActivityScheduled', 'payload' => []];
        }

        return $events;
    }
}
