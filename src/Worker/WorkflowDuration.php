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
 * Strings and intervals convert exactly like the embedded durable-workflow/workflow package's
 * `timer()` and `await()`: `(int) ceil(CarbonInterval::fromString($duration)->totalSeconds)` for a
 * string, and `(int) ceil($interval->totalSeconds)` for a CarbonInterval, with a plain DateInterval
 * first wrapped by `CarbonInterval::instance()`. Carbon applies fixed factors rather than a
 * calendar (a day is 24 hours, a week 7 days, a month 4 weeks and a year 12 months), so the result
 * depends only on the input, never on the current date or time zone.
 *
 * The SDK is stricter than the embedded package about what it accepts, so every accepted value
 * converts to the same seconds there and here: a string must consist only of Carbon duration parts
 * (optionally joined by "and"), parts may not be negative, ISO 8601 strings must be passed as a
 * DateInterval, and weekday or "last day of" `DateInterval::createFromDateString()` expressions are
 * rejected.
 *
 * @internal
 */
final class WorkflowDuration
{
    /** Carbon's own `CarbonInterval::fromString()` part grammar. */
    private const CARBON_PART = '/(-?\d+(?:\.\d+)?)\h*([^\d\h]*)/i';

    private const ACCEPTED_FORMS = 'Accepted forms: int or float seconds; a duration string such as "30 minutes", '
        .'"1h 30m" or "1 hour and 30 minutes" (units: microseconds, milliseconds, seconds, minutes, hours, days, '
        .'weeks, months, years); or a DateInterval or CarbonInterval such as new DateInterval(\'PT30M\') or '
        .'CarbonInterval::minutes(30).';

    /** Largest whole-second value that converts from a float without losing integer precision. */
    private const MAX_SECONDS = 9_007_199_254_740_992;

    private function __construct()
    {
    }

    /**
     * Convert a duration string or interval to whole seconds, rounding up like integer and float seconds.
     *
     * @param string $parameter the parameter or option name reported in validation errors
     * @param int<0, 1> $minimum 0 where zero is a valid duration, 1 where at least one second is required
     */
    public static function seconds(string|DateInterval $duration, string $parameter, int $minimum = 0): int
    {
        if (is_string($duration)) {
            return self::fromString($duration, $parameter, $minimum);
        }

        return self::fromInterval($duration, $parameter, $minimum);
    }

    /**
     * Convert a string or interval option value, leaving every other value for existing validation.
     *
     * @param int<0, 1> $minimum
     */
    public static function normalize(mixed $value, string $parameter, int $minimum = 0): mixed
    {
        if (is_string($value) || $value instanceof DateInterval) {
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
        if (preg_match('/^P(?:T|\d)/i', $definition) === 1) {
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
        $properties = (array) $interval;
        if (($properties['from_string'] ?? false) === true) {
            // PHP 8.2+ keeps DateInterval::createFromDateString() input as text instead of fields.
            // Parse that text with the string rules, which match what PHP 8.1 records as fields.
            return self::fromString((string) ($properties['date_string'] ?? ''), $parameter, $minimum);
        }
        if (!empty($properties['have_special_relative'])
            || !empty($properties['have_weekday_relative'])
            || !empty($properties['first_last_day_of'])) {
            throw self::invalid(
                $parameter,
                $interval,
                'relative DateInterval::createFromDateString() expressions such as weekdays or "last day of" are not fixed durations',
            );
        }

        try {
            $total = ($interval instanceof CarbonInterval ? $interval : CarbonInterval::instance($interval))->totalSeconds;
        } catch (Throwable $error) {
            throw self::invalid($parameter, $interval, 'Carbon could not measure it ('.$error->getMessage().')', $error);
        }

        return self::wholeSeconds($total, $parameter, $interval, $minimum);
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
        try {
            return ($interval->invert === 1 ? '-' : '').$interval->format('P%yY%mM%dDT%hH%iM%sS');
        } catch (Throwable) {
            return '(unformattable)';
        }
    }
}
