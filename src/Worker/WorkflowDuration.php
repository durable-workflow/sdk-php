<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Carbon\CarbonInterval;
use DateInterval;
use InvalidArgumentException;
use Throwable;

/**
 * Converts friendly workflow durations to the whole seconds recorded in history.
 *
 * Duration strings convert exactly like the embedded durable-workflow/workflow package's `timer()`
 * and `await()`: `(int) ceil(CarbonInterval::fromString($duration)->totalSeconds)`. Intervals are
 * measured by their y/m/d/h/i/s/f fields with `CarbonInterval::instance()->totalSeconds`, which
 * matches the embedded package for every interval that does not carry diff() start and end dates.
 * Carbon applies its cascade factors rather than a calendar (by default a day is 24 hours, a week
 * 7 days, a month 4 weeks and a year 12 months), so calendar dates and time zones do not affect
 * measurement. Floating-point precision is retained from the installed Carbon major: very large
 * durations containing millisecond or microsecond parts can produce different whole seconds
 * across majors. Changing the factors globally with
 * `CarbonInterval::setCascadeFactors()` changes the result, as it does in the embedded package.
 *
 * Numeric strings such as "30" or "1.5" convert as they were coerced when these parameters were
 * typed `int|float` in files without strict_types: timers and condition timeouts round them up to
 * whole seconds, and option values pass through unchanged. With strict_types they used to throw a
 * TypeError and are now accepted the same way.
 *
 * The SDK is stricter than the embedded package, so every accepted string converts to the same
 * seconds there and here. A string must consist only of Carbon duration parts (optionally joined by
 * "and"). Parts must not be negative, have more than 15 integer digits, reach 2^53 seconds alone or
 * together (Carbon's integer arithmetic could wrap them), or use a fraction with a calendar unit
 * (months, quarters, years, decades, centuries, millennia), which Carbon truncates.
 * ISO 8601 strings must be passed as a DateInterval. Intervals must not be negative or relative
 * (weekday or "first/last day of" `DateInterval::createFromDateString()` expressions).
 *
 * @internal
 */
final class WorkflowDuration
{
    /** Carbon's own `CarbonInterval::fromString()` part grammar. */
    private const CARBON_PART = '/(-?\d+(?:\.\d+)?)\h*([^\d\h]*)/i';

    /** Carbon `fromString()` units whose fractions Carbon silently truncates. */
    private const CALENDAR_UNITS = [
        'millennia', 'millennium', 'century', 'centuries', 'decade', 'decades', 'year', 'years', 'y', 'yr', 'yrs',
        'quarter', 'quarters', 'month', 'months', 'mo', 'mos',
    ];

    /**
     * Longest integer part of one string duration part. This keeps Carbon's integer parsing in
     * range, but a shorter calendar value can still overflow Carbon's arithmetic, so each part's
     * seconds are also bounded by MAX_SECONDS before Carbon sees the string.
     */
    private const MAX_PART_DIGITS = 15;

    /** Seconds per Carbon `fromString()` unit with Carbon's default cascade factors. */
    private const UNIT_SECONDS = [
        'millennia' => 29_030_400_000, 'millennium' => 29_030_400_000,
        'century' => 2_903_040_000, 'centuries' => 2_903_040_000,
        'decade' => 290_304_000, 'decades' => 290_304_000,
        'year' => 29_030_400, 'years' => 29_030_400, 'y' => 29_030_400, 'yr' => 29_030_400, 'yrs' => 29_030_400,
        'quarter' => 7_257_600, 'quarters' => 7_257_600,
        'month' => 2_419_200, 'months' => 2_419_200, 'mo' => 2_419_200, 'mos' => 2_419_200,
        'week' => 604_800, 'weeks' => 604_800, 'w' => 604_800,
        'day' => 86_400, 'days' => 86_400, 'd' => 86_400,
        'hour' => 3_600, 'hours' => 3_600, 'h' => 3_600,
        'minute' => 60, 'minutes' => 60, 'm' => 60,
        'second' => 1, 'seconds' => 1, 's' => 1,
        'millisecond' => 0.001, 'milliseconds' => 0.001, 'milli' => 0.001, 'ms' => 0.001,
        'microsecond' => 0.000_001, 'microseconds' => 0.000_001, 'micro' => 0.000_001, 'µs' => 0.000_001,
    ];

    /** Seconds per DateInterval field (y, m, d, h, i, s) with Carbon's default cascade factors. */
    private const FIELD_SECONDS = [29_030_400, 2_419_200, 86_400, 3_600, 60, 1];

    private const ACCEPTED_FORMS = 'Accepted forms: int or float seconds; a duration string such as "30 minutes", '
        .'"1h 30m" or "1 hour and 30 minutes"; or a DateInterval or CarbonInterval such as '
        .'new DateInterval(\'PT30M\') or CarbonInterval::minutes(30).';

    /** Largest whole-second value that converts from a float without losing integer precision. */
    private const MAX_SECONDS = 9_007_199_254_740_992;

    private function __construct()
    {
    }

    /**
     * Convert a duration string or interval to whole seconds, rounding up like integer and float seconds.
     *
     * A numeric string converts like the int or float it coerced to before string durations were
     * accepted: `max(0, (int) ceil((float) $duration))`.
     *
     * @param string $parameter the parameter or option name reported in validation errors
     * @param int<0, 1> $minimum 0 where zero is a valid duration, 1 where at least one second is required
     */
    public static function seconds(string|DateInterval $duration, string $parameter, int $minimum = 0): int
    {
        if (is_string($duration) && is_numeric($duration)) {
            return max(0, (int) ceil((float) $duration));
        }
        if (is_string($duration)) {
            return self::fromString($duration, $parameter, $minimum);
        }

        return self::fromInterval($duration, $parameter, $minimum);
    }

    /**
     * Convert a duration string or interval option value. Numbers, numeric strings and every other
     * value pass through unchanged to the option's existing validation.
     *
     * @param int<0, 1> $minimum
     */
    public static function normalize(mixed $value, string $parameter, int $minimum = 0): mixed
    {
        if ((is_string($value) && !is_numeric($value)) || $value instanceof DateInterval) {
            return self::seconds($value, $parameter, $minimum);
        }

        return $value;
    }

    /** @param int<0, 1> $minimum */
    private static function fromString(string $duration, string $parameter, int $minimum): int
    {
        $definition = trim($duration);
        if ($definition === '') {
            throw self::invalid($parameter, $duration, 'it is empty');
        }
        if (preg_match('/^P[T\d]/i', $definition) === 1) {
            throw self::invalid(
                $parameter,
                $duration,
                'ISO 8601 duration strings are not accepted; pass new DateInterval('.var_export($definition, true).') instead',
            );
        }
        if (preg_match('/-\h*\d/', $definition) === 1) {
            throw self::invalid($parameter, $duration, 'durations and their parts must not be negative');
        }
        $residue = preg_replace(self::CARBON_PART, ' ', $definition);
        if (preg_match('/\d/', $definition) !== 1
            || !is_string($residue)
            || preg_match('/^(?:\s|\band\b)*$/iD', $residue) !== 1) {
            throw self::invalid($parameter, $duration, 'it is not a duration of numbers and units');
        }
        preg_match_all(self::CARBON_PART, $definition, $parts, PREG_SET_ORDER);
        $estimate = 0.0;
        foreach ($parts as [, $value, $unit]) {
            [$integer, $fraction] = explode('.', $value.'.');
            if (strlen(ltrim($integer, '0')) > self::MAX_PART_DIGITS) {
                throw self::invalid($parameter, $duration, 'it is too large');
            }
            if (trim($fraction, '0') !== '' && in_array(strtolower($unit), self::CALENDAR_UNITS, true)) {
                throw self::invalid(
                    $parameter,
                    $duration,
                    "the {$unit} part must be a whole number; Carbon would truncate its fraction",
                );
            }
            // Carbon's integer calendar arithmetic can wrap a huge part to a small positive total,
            // so bound every part, and their sum, as floats before Carbon sees the string.
            $estimate += (float) $value * (self::UNIT_SECONDS[$unit === 'µs' ? 'µs' : strtolower($unit)] ?? 0);
            if ($estimate >= self::MAX_SECONDS) {
                throw self::invalid($parameter, $duration, 'it is too large');
            }
        }

        try {
            $interval = CarbonInterval::fromString($definition);
        } catch (Throwable $error) {
            throw self::invalid($parameter, $duration, 'it has an unknown unit or separator', $error);
        }

        return self::wholeSeconds($interval->totalSeconds, $parameter, $duration, $minimum);
    }

    /** @param int<0, 1> $minimum */
    private static function fromInterval(DateInterval $interval, string $parameter, int $minimum): int
    {
        if (self::isRelative($interval)) {
            throw self::invalid(
                $parameter,
                $interval,
                'relative DateInterval::createFromDateString() expressions such as weekdays or "last day of" are not fixed durations',
            );
        }
        // Read the fields on every PHP version. PHP 8.2+ still exposes them for createFromDateString()
        // values, and measuring fields keeps diff() intervals identical on Carbon 2 and 3.
        $fields = [$interval->y, $interval->m, $interval->d, $interval->h, $interval->i, $interval->s, $interval->f];
        if ($interval->invert === 1 || min($fields) < 0) {
            throw self::invalid($parameter, $interval, 'it is negative or has a negative part');
        }
        $estimate = (float) $interval->f;
        foreach (self::FIELD_SECONDS as $index => $factor) {
            $estimate += (float) $fields[$index] * $factor;
        }
        if ($estimate >= self::MAX_SECONDS) {
            throw self::invalid($parameter, $interval, 'it is too large');
        }
        $plain = new DateInterval('PT0S');
        [$plain->y, $plain->m, $plain->d, $plain->h, $plain->i, $plain->s, $plain->f] = $fields;

        try {
            $total = CarbonInterval::instance($plain)->totalSeconds;
        } catch (Throwable $error) {
            throw self::invalid($parameter, $interval, 'Carbon could not measure it', $error);
        }

        return self::wholeSeconds($total, $parameter, $interval, $minimum);
    }

    private static function isRelative(DateInterval $interval): bool
    {
        $properties = (array) $interval;
        if (!empty($properties['have_special_relative'])
            || !empty($properties['have_weekday_relative'])
            || !empty($properties['first_last_day_of'])) {
            return true; // PHP 8.1 exposes the relative flags directly.
        }
        if (($properties['from_string'] ?? false) !== true) {
            return false;
        }
        // PHP 8.2+ hides those flags for createFromDateString() values; parse the kept text with the
        // same timelib parser to find them.
        $relative = date_parse((string) ($properties['date_string'] ?? ''))['relative'] ?? [];

        return isset($relative['weekday']) || isset($relative['weekdays'])
            || !empty($relative['first_day_of_month']) || !empty($relative['last_day_of_month']);
    }

    /** @param int<0, 1> $minimum */
    private static function wholeSeconds(float|int $total, string $parameter, string|DateInterval $duration, int $minimum): int
    {
        if (!is_finite((float) $total) || abs($total) >= self::MAX_SECONDS) {
            throw self::invalid($parameter, $duration, 'it is too large');
        }
        if ($total < 0) {
            throw self::invalid($parameter, $duration, 'it is negative');
        }
        $seconds = (int) ceil($total);
        if ($seconds < $minimum) {
            throw self::invalid($parameter, $duration, 'it must be at least 1 second');
        }

        return $seconds;
    }

    private static function invalid(
        string $parameter,
        string|DateInterval $duration,
        string $reason,
        ?Throwable $previous = null,
    ): InvalidArgumentException {
        $shown = is_string($duration)
            ? json_encode($duration, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE)
            : $duration::class.' '.self::describeInterval($duration);

        return new InvalidArgumentException(
            "Invalid {$parameter} duration {$shown}: {$reason}. ".self::ACCEPTED_FORMS,
            0,
            $previous,
        );
    }

    private static function describeInterval(DateInterval $interval): string
    {
        return sprintf(
            '%sP%dY%dM%dDT%dH%dM%dS',
            $interval->invert === 1 ? '-' : '',
            $interval->y,
            $interval->m,
            $interval->d,
            $interval->h,
            $interval->i,
            $interval->s,
        );
    }
}
