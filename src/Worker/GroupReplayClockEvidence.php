<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;

/** @internal Consumed groups with the additive clock's canonical-v1 numeric domain. */
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
            $at = self::canonicalMilliseconds($candidate['time']);
            if ($at === null) {
                return null;
            }
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
            $at = self::canonicalMilliseconds($candidate['time']);
            if ($at === null) {
                return null;
            }
            if ($selected === null || $at < $milliseconds
                || ($at === $milliseconds && $candidate['offset'] < $selected['offset'])) {
                $selected = $candidate;
                $milliseconds = $at;
            }
        }
        if ($selected === null || $selected['order'] !== $failedOrder) {
            return null;
        }

        return self::fromMilliseconds($milliseconds);
    }

    /** Nearest millisecond, exact halfway away from zero; no runtime float rounding. */
    private static function canonicalMilliseconds(DateTimeImmutable $time): ?int
    {
        $microseconds = self::utcMicroseconds($time);
        if ($microseconds === null || $microseconds === PHP_INT_MIN || abs($microseconds) > PHP_INT_MAX - 500) {
            return null;
        }
        $rounded = intdiv(abs($microseconds) + 500, 1_000);

        return $microseconds < 0 ? -$rounded : $rounded;
    }

    private static function utcMicroseconds(DateTimeImmutable $time): ?int
    {
        $secondsText = $time->format('U');
        $seconds = (int) $secondsText;
        if ((string) $seconds !== $secondsText || $seconds < intdiv(PHP_INT_MIN, 1_000_000)
            || $seconds > intdiv(PHP_INT_MAX - 999_999, 1_000_000)) {
            return null;
        }

        return $seconds * 1_000_000 + (int) $time->format('u');
    }

    private static function fromMilliseconds(int $milliseconds): ?DateTimeImmutable
    {
        $seconds = intdiv($milliseconds, 1_000);
        $remainder = $milliseconds % 1_000;
        if ($remainder < 0) {
            --$seconds;
            $remainder += 1_000;
        }
        $time = DateTimeImmutable::createFromFormat('U u', $seconds.' '.str_pad((string) ($remainder * 1_000), 6, '0', STR_PAD_LEFT));

        return $time === false ? null : $time->setTimezone(new DateTimeZone('UTC'));
    }
}
