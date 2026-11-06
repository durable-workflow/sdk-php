<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Carbon\CarbonImmutable;
use DateTimeImmutable;

/** @internal Core's consumed-group aggregate, separate from scalar redrive time. */
final class GroupReplayClockEvidence
{
    /** @param list<array{time: DateTimeImmutable, offset: int, order: int}> $candidates */
    public static function time(array $candidates, ?int $failedOrder): ?DateTimeImmutable
    {
        usort($candidates, static fn (array $left, array $right): int => $left['offset'] <=> $right['offset']);
        try {
            return $failedOrder === null ? self::success($candidates) : self::failure($candidates, $failedOrder);
        } catch (\Exception) {
            return null;
        }
    }

    /** @param list<array{time: DateTimeImmutable, offset: int, order: int}> $candidates */
    private static function success(array $candidates): ?DateTimeImmutable
    {
        $selected = null;
        $milliseconds = null;
        foreach ($candidates as $candidate) {
            $at = CarbonImmutable::instance($candidate['time'])->getTimestampMs();
            if ($milliseconds === null || $at > $milliseconds) {
                $selected = $candidate['time'];
                $milliseconds = $at;
            }
        }

        return $selected;
    }

    /** @param list<array{time: DateTimeImmutable, offset: int, order: int}> $candidates */
    private static function failure(array $candidates, int $failedOrder): ?DateTimeImmutable
    {
        $selected = null;
        $milliseconds = null;
        foreach ($candidates as $candidate) {
            $at = CarbonImmutable::instance($candidate['time'])->getTimestampMs();
            if ($selected === null || $at < $milliseconds
                || ($at === $milliseconds && $candidate['offset'] < $selected['offset'])) {
                $selected = $candidate;
                $milliseconds = $at;
            }
        }
        if ($selected === null || $selected['order'] !== $failedOrder) {
            return null;
        }

        return DateTimeImmutable::createFromInterface(CarbonImmutable::createFromTimestampMs($milliseconds, 'UTC'));
    }
}
