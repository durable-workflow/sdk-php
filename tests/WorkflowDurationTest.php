<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Carbon\Carbon;
use Carbon\CarbonInterval;
use DateInterval;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\WorkflowDuration;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowDurationTest extends TestCase
{
    private string $timezone;

    protected function setUp(): void
    {
        $this->timezone = date_default_timezone_get();
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();
        date_default_timezone_set($this->timezone);
    }

    /** @return iterable<string, array{int|float|string|DateInterval}> */
    public static function thirtyMinutes(): iterable
    {
        yield 'int' => [1800];
        yield 'float' => [1800.0];
        yield 'fractional float rounds up' => [1799.25];
        yield 'string' => ['30 minutes'];
        yield 'short string' => ['30m'];
        yield 'fractional string' => ['0.5 hours'];
        yield 'compound string' => ['29 minutes and 60 seconds'];
        yield 'fractional seconds string rounds up' => ['1799.5 seconds'];
        yield 'DateInterval' => [new DateInterval('PT30M')];
        yield 'DateInterval with microseconds rounds up' => [self::withMicroseconds(new DateInterval('PT29M59S'), 0.25)];
        yield 'CarbonInterval' => [CarbonInterval::minutes(30)];
        yield 'CarbonInterval from string' => [CarbonInterval::fromString('30 minutes')];
        yield 'createFromDateString' => [DateInterval::createFromDateString('30 minutes')];
    }

    #[DataProvider('thirtyMinutes')]
    public function testSleepRecordsIdenticalTimerCommands(int|float|string|DateInterval $duration): void
    {
        $workflow = static function (WorkflowContext $context) use ($duration): void {
            $context->sleep($duration);
        };

        self::assertSame(self::commandsFor(static fn (WorkflowContext $context) => $context->sleep(1800)), self::commandsFor($workflow));
        self::assertSame(1800, self::commandsFor($workflow)[0]['delay_seconds']);
    }

    #[DataProvider('thirtyMinutes')]
    public function testDeferTimerInGroupRecordsIdenticalTimerCommands(int|float|string|DateInterval $duration): void
    {
        $baseline = self::commandsFor(static fn (WorkflowContext $context) => $context->all([$context->deferTimer(1800)]));
        $commands = self::commandsFor(static fn (WorkflowContext $context) => $context->all([$context->deferTimer($duration)]));

        self::assertSame($baseline, $commands);
        self::assertSame(1800, $commands[0]['delay_seconds']);
    }

    #[DataProvider('thirtyMinutes')]
    public function testWaitConditionRecordsIdenticalTimeoutCommands(int|float|string|DateInterval $duration): void
    {
        $workflow = static fn (int|float|string|DateInterval $timeout) => static fn (WorkflowContext $context): bool => $context->waitCondition(
            static fn (): bool => $context->signals('verify') !== [],
            key: 'email-verified',
            timeout: $timeout,
        );

        $commands = self::commandsFor($workflow($duration));
        self::assertSame(self::commandsFor($workflow(1800)), $commands);
        self::assertSame('open_condition_wait', $commands[0]['type']);
        self::assertSame(1800, $commands[0]['timeout_seconds']);
    }

    #[DataProvider('thirtyMinutes')]
    public function testDeferConditionUsesTheSameTimeout(int|float|string|DateInterval $duration): void
    {
        $observed = [];
        $workflow = static function (WorkflowContext $context) use ($duration, &$observed): void {
            $observed[] = $context->deferCondition(static fn (): bool => false, 'deferred', $duration)->command->attributes;
            $observed[] = $context->deferCondition(static fn (): bool => false, 'deferred', 1800)->command->attributes;
        };
        self::commandsFor($workflow);

        self::assertSame(1800, $observed[0]['timeout_seconds']);
        self::assertSame(
            array_diff_key($observed[1], ['condition_definition_fingerprint' => true]),
            array_diff_key($observed[0], ['condition_definition_fingerprint' => true]),
        );
    }

    #[DataProvider('thirtyMinutes')]
    public function testRecordedThirtyMinuteTimerReplaysForEveryForm(int|float|string|DateInterval $duration): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1800]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1800]],
        ];
        $result = (new Replayer($codec))->replay(static function (WorkflowContext $context) use ($duration): string {
            $context->sleep($duration);

            return 'fired';
        }, $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('fired', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testRecordedTimerRejectsADifferentFriendlyDuration(): void
    {
        $history = [
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1800]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1800]],
        ];

        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->sleep('31 minutes'),
            $history,
            [],
            'php-workers',
        );
    }

    /**
     * Activity and child timeout options keep passing int and float values through unchanged.
     *
     * @return iterable<string, array{int|string|DateInterval}>
     */
    public static function thirtyMinuteOptions(): iterable
    {
        foreach (self::thirtyMinutes() as $name => [$duration]) {
            if (!is_float($duration)) {
                yield $name => [$duration];
            }
        }
    }

    #[DataProvider('thirtyMinuteOptions')]
    public function testActivityTimeoutOptionsRecordIdenticalCommands(int|string|DateInterval $duration): void
    {
        $options = static fn (int|string|DateInterval $timeout): array => [
            'start_to_close_timeout' => $timeout,
            'schedule_to_start_timeout' => $timeout,
            'schedule_to_close_timeout' => $timeout,
            'heartbeat_timeout' => $timeout,
        ];

        $commands = self::commandsFor(static fn (WorkflowContext $context) => $context->activity('send-email', [], $options($duration)));
        self::assertSame(self::commandsFor(static fn (WorkflowContext $context) => $context->activity('send-email', [], $options(1800))), $commands);
        foreach (array_keys($options(1800)) as $field) {
            self::assertSame(1800, $commands[0][$field]);
        }
    }

    public function testAbandonAcceptsAFriendlyScheduleToCloseTimeout(): void
    {
        $options = WorkflowCommand::canonicalRemoteActivityOptions([
            'cancellation_policy' => 'abandon',
            'schedule_to_close_timeout' => '1 hour',
        ]);

        self::assertSame(3600, $options['schedule_to_close_timeout']);
    }

    public function testLocalActivityChildWorkflowAndServiceTimeoutsAcceptFriendlyDurations(): void
    {
        $local = WorkflowCommand::canonicalLocalActivityOptions([
            'start_to_close_timeout' => '30 seconds',
            'schedule_to_close_timeout' => new DateInterval('PT1M'),
            'heartbeat_timeout' => CarbonInterval::seconds(10),
        ]);
        self::assertSame(
            ['start_to_close_timeout' => 30, 'schedule_to_close_timeout' => 60, 'heartbeat_timeout' => 10],
            $local,
        );

        $commands = self::commandsFor(static fn (WorkflowContext $context) => $context->childWorkflow('child', [], [
            'execution_timeout_seconds' => '2 hours',
            'run_timeout_seconds' => new DateInterval('PT1H'),
        ]));
        self::assertSame(7200, $commands[0]['execution_timeout_seconds']);
        self::assertSame(3600, $commands[0]['run_timeout_seconds']);

        $service = WorkflowCommand::serviceOperation('billing', 'invoices', 'create', [], ['wait_timeout_seconds' => '5 minutes']);
        self::assertSame(300, $service->attributes['wait_timeout_seconds']);
        $zero = WorkflowCommand::serviceOperation('billing', 'invoices', 'create', [], ['wait_timeout_seconds' => '0 seconds']);
        self::assertSame(0, $zero->attributes['wait_timeout_seconds']);
    }

    /**
     * The embedded package converts with ceil(CarbonInterval::fromString($s)->totalSeconds).
     *
     * @return iterable<string, array{string, int}>
     */
    public static function embeddedStrings(): iterable
    {
        yield '30 minutes' => ['30 minutes', 1800];
        yield '1 hour 30 minutes' => ['1 hour 30 minutes', 5400];
        yield '1 hour and 30 minutes' => ['1 hour and 30 minutes', 5400];
        yield '1h 30m' => ['1h 30m', 5400];
        yield '2 days' => ['2 days', 172800];
        yield '1 week' => ['1 week', 604800];
        yield '1.5 weeks' => ['1.5 weeks', 907200];
        yield '90 seconds' => ['90 seconds', 90];
        yield '1.5 hours' => ['1.5 hours', 5400];
        yield '1 month uses the four-week factor' => ['1 month', 2419200];
        yield '1 year uses the twelve-month factor' => ['1 year', 29030400];
        yield 'uppercase' => ['30 MINUTES', 1800];
        yield 'no space' => ['30minutes', 1800];
        yield 'surrounding whitespace' => ['  30 minutes  ', 1800];
        yield 'milliseconds round up' => ['1500 ms', 2];
        yield 'one microsecond rounds up' => ['1 µs', 1];
        yield 'zero' => ['0 seconds', 0];
    }

    #[DataProvider('embeddedStrings')]
    public function testStringsConvertExactlyLikeTheEmbeddedPackage(string $duration, int $seconds): void
    {
        self::assertSame($seconds, (int) ceil(CarbonInterval::fromString($duration)->totalSeconds));
        self::assertSame($seconds, WorkflowDuration::seconds($duration, 'timer'));
    }

    public function testIntervalsConvertExactlyLikeTheEmbeddedPackage(): void
    {
        foreach ([
            CarbonInterval::minutes(90),
            CarbonInterval::days(2)->hours(3),
            CarbonInterval::months(1),
            new DateInterval('P1W'),
            new DateInterval('P1Y2M3DT4H5M6S'),
            (new \DateTimeImmutable('2024-01-01'))->diff(new \DateTimeImmutable('2024-01-03 12:00:00')),
        ] as $interval) {
            $carbon = $interval instanceof CarbonInterval ? $interval : CarbonInterval::instance($interval);
            self::assertSame((int) ceil($carbon->totalSeconds), WorkflowDuration::seconds($interval, 'timer'));
        }
    }

    /** @return iterable<string, array{string|DateInterval, string}> */
    public static function rejected(): iterable
    {
        yield 'empty string' => ['', 'Invalid timer duration "": it is empty.'];
        yield 'whitespace' => ['   ', 'Invalid timer duration "   ": it is empty.'];
        yield 'garbage' => ['abc', 'Invalid timer duration "abc": it is not a duration of numbers and units.'];
        yield 'number without a unit' => ['30 and 5', 'Invalid timer duration "30 and 5": it has an unknown unit'];
        yield 'unknown unit' => ['30 lightyears', 'Invalid timer duration "30 lightyears": it has an unknown unit'];
        yield 'unsupported abbreviation' => ['30 mins', 'Invalid timer duration "30 mins": it has an unknown unit'];
        yield 'trailing garbage' => ['30 minutes garbage', 'Invalid timer duration "30 minutes garbage": it is not a duration of numbers and units.'];
        yield 'leading garbage' => ['in 30 minutes', 'Invalid timer duration "in 30 minutes": it is not a duration of numbers and units.'];
        yield 'comma separator' => ['1 hour, 30 minutes', 'Invalid timer duration "1 hour, 30 minutes": it has an unknown unit'];
        yield 'negative' => ['-5 minutes', 'Invalid timer duration "-5 minutes": durations and their parts must not be negative.'];
        yield 'negative part' => ['1 hour -5 minutes', 'Invalid timer duration "1 hour -5 minutes": durations and their parts must not be negative.'];
        yield 'ISO 8601 string' => ['PT30M', 'Invalid timer duration "PT30M": ISO 8601 duration strings are not accepted; pass new DateInterval(\'PT30M\') instead.'];
        yield 'too large' => ['99999999999 years', 'Invalid timer duration "99999999999 years": it is too large.'];
        yield 'years wrapping inside Carbon' => ['635428518853 years', 'Invalid timer duration "635428518853 years": it is too large.'];
        yield 'days wrapping inside Carbon' => ['213503982334602 days', 'Invalid timer duration "213503982334602 days": it is too large.'];
        yield 'one day past 2^53 seconds' => ['104249991375 days', 'it is too large.'];
        yield 'weeks reaching 2^53 seconds' => ['14892857142 weeks', 'it is too large.'];
        yield 'parts summing past 2^53 seconds' => ['104249991374 days 104249991374 days', 'it is too large.'];
        yield 'DateInterval years wrapping inside Carbon' => [self::withField(new DateInterval('PT0S'), 'y', 635428518853), 'Invalid timer duration DateInterval P635428518853Y0M0DT0H0M0S: it is too large.'];
        yield 'DateInterval days wrapping inside Carbon' => [self::withField(new DateInterval('PT0S'), 'd', 213503982334602), 'Invalid timer duration DateInterval P0Y0M213503982334602DT0H0M0S: it is too large.'];
        yield 'too many digits' => ['99999999999999999999 years', 'Invalid timer duration "99999999999999999999 years": it is too large.'];
        yield 'too many second digits' => ['1234567890123456 seconds', 'it is too large.'];
        yield 'fractional month' => ['0.5 months', 'Invalid timer duration "0.5 months": the months part must be a whole number; Carbon would truncate its fraction.'];
        yield 'fractional year' => ['1.5 years', 'the years part must be a whole number'];
        yield 'fractional quarter' => ['1.5 quarters', 'the quarters part must be a whole number'];
        yield 'fractional mo' => ['2.25mo', 'the mo part must be a whole number'];
        yield 'inverted DateInterval' => [self::inverted(new DateInterval('PT5M')), 'Invalid timer duration DateInterval -P0Y0M0DT0H5M0S: it is negative or has a negative part.'];
        yield 'negative CarbonInterval' => [CarbonInterval::minutes(-5), 'it is negative'];
        $relative = 'relative DateInterval::createFromDateString() expressions such as weekdays or "last day of" are not fixed durations.';
        yield 'weekday relative' => [DateInterval::createFromDateString('next weekday'), $relative];
        yield 'named weekday' => [DateInterval::createFromDateString('monday'), $relative];
        yield 'last day of' => [DateInterval::createFromDateString('last day of next month'), $relative];
        yield 'first day of' => [DateInterval::createFromDateString('first day of next month'), $relative];
        yield 'negative createFromDateString' => [DateInterval::createFromDateString('-5 minutes'), 'it is negative or has a negative part.'];
        yield 'createFromDateString ago' => [DateInterval::createFromDateString('3 days ago'), 'it is negative or has a negative part.'];
        yield 'mixed-sign createFromDateString' => [DateInterval::createFromDateString('1 hour -5 minutes'), 'it is negative or has a negative part.'];
        yield 'negative DateInterval field' => [self::withField(new DateInterval('PT1H'), 'i', -5), 'it is negative or has a negative part.'];
    }

    #[DataProvider('rejected')]
    public function testUnparseableAndNegativeDurationsAreRejectedWithAClearMessage(string|DateInterval $duration, string $message): void
    {
        try {
            WorkflowDuration::seconds($duration, 'timer');
            self::fail('Expected the duration to be rejected.');
        } catch (InvalidArgumentException $error) {
            self::assertStringContainsString($message, $error->getMessage());
            self::assertStringContainsString('Accepted forms: int or float seconds; a duration string such as "30 minutes"', $error->getMessage());
        }
    }

    public function testContextMethodsNameTheRejectedParameter(): void
    {
        $cases = [
            'timer' => static fn (WorkflowContext $context) => $context->sleep('abc'),
            'condition wait timeout' => static fn (WorkflowContext $context) => $context->waitCondition(static fn (): bool => false, 'k', 'soon'),
            'remote activity start_to_close_timeout' => static fn (WorkflowContext $context) => $context->activity('a', [], ['start_to_close_timeout' => '30 lightyears']),
            'child workflow run_timeout_seconds' => static fn (WorkflowContext $context) => $context->childWorkflow('c', [], ['run_timeout_seconds' => '-1 hour']),
        ];
        foreach ($cases as $parameter => $workflow) {
            try {
                self::commandsFor($workflow);
                self::fail("Expected {$parameter} to be rejected.");
            } catch (InvalidArgumentException $error) {
                self::assertStringStartsWith("Invalid {$parameter} duration ", $error->getMessage());
            }
        }
    }

    public function testZeroIsRejectedWhereAtLeastOneSecondIsRequired(): void
    {
        foreach (['0 seconds', new DateInterval('PT0S'), '0.0000001 seconds'] as $zero) {
            try {
                WorkflowCommand::canonicalRemoteActivityOptions(['start_to_close_timeout' => $zero]);
                self::fail('Expected a zero activity timeout to be rejected.');
            } catch (InvalidArgumentException $error) {
                self::assertStringContainsString('Invalid remote activity start_to_close_timeout duration', $error->getMessage());
                self::assertStringContainsString('it must be at least 1 second.', $error->getMessage());
            }
        }

        $this->expectExceptionMessage('Invalid local activity heartbeat_timeout duration "0 seconds": it must be at least 1 second.');
        WorkflowCommand::canonicalLocalActivityOptions(['heartbeat_timeout' => '0 seconds']);
    }

    public function testZeroRemainsValidForTimersAndConditionTimeouts(): void
    {
        self::assertSame(0, self::commandsFor(static fn (WorkflowContext $context) => $context->sleep('0 seconds'))[0]['delay_seconds']);
        self::assertSame(0, self::commandsFor(static fn (WorkflowContext $context) => $context->sleep(0))[0]['delay_seconds']);
        $wait = static fn (int|DateInterval $timeout) => static fn (WorkflowContext $context) => $context->waitCondition(
            static fn (): bool => false,
            'k',
            $timeout,
        );
        self::assertSame(self::commandsFor($wait(0)), self::commandsFor($wait(new DateInterval('PT0S'))));
    }

    public function testConversionDoesNotDependOnTheCurrentDateOrTimezone(): void
    {
        $inputs = ['30 minutes', '1.5 hours', '2 days', '1 week', '1 month', '1 year', new DateInterval('P1M'),
            new DateInterval('P1Y2M3DT4H'), CarbonInterval::months(2), CarbonInterval::days(1)];
        $reference = array_map(static fn (string|DateInterval $input): int => WorkflowDuration::seconds($input, 'timer'), $inputs);

        // Month ends, leap days and DST transitions in several zones.
        foreach (['2024-02-29 12:00:00', '2025-01-31 23:59:59', '2026-03-08 01:30:00', '2026-11-01 01:30:00', '2100-12-31 00:00:00'] as $now) {
            foreach (['UTC', 'America/New_York', 'Australia/Lord_Howe', 'Pacific/Kiritimati'] as $timezone) {
                date_default_timezone_set($timezone);
                Carbon::setTestNow(Carbon::parse($now, $timezone));
                self::assertSame(
                    $reference,
                    array_map(static fn (string|DateInterval $input): int => WorkflowDuration::seconds($input, 'timer'), $inputs),
                    "{$now} {$timezone}",
                );
            }
        }
        self::assertSame([1800, 5400, 172800, 604800, 2419200, 29030400, 2419200, 34142400, 4838400, 86400], $reference);
    }

    /** @return iterable<string, array{DateInterval, int}> */
    public static function createdFromDateString(): iterable
    {
        yield '5 mins' => [DateInterval::createFromDateString('5 mins'), 300];
        yield '1 sec' => [DateInterval::createFromDateString('1 sec'), 1];
        yield '1 fortnight' => [DateInterval::createFromDateString('1 fortnight'), 1209600];
        yield '+1 day' => [DateInterval::createFromDateString('+1 day'), 86400];
        yield '2 weeks 3 days' => [DateInterval::createFromDateString('2 weeks 3 days'), 1468800];
        yield '1 month' => [DateInterval::createFromDateString('1 month'), 2419200];
        yield '500 msec rounds up' => [DateInterval::createFromDateString('500 msec'), 1];
    }

    /** The same createFromDateString() value converts identically on PHP 8.1 and 8.2+. */
    #[DataProvider('createdFromDateString')]
    public function testCreateFromDateStringIntervalsUseTheirFieldsOnEveryPhpVersion(DateInterval $interval, int $seconds): void
    {
        self::assertSame($seconds, WorkflowDuration::seconds($interval, 'timer'));
    }

    public function testDiffIntervalsAreMeasuredByTheirFieldsOnEveryCarbonVersion(): void
    {
        $start = Carbon::parse('2024-01-01 00:00:00', 'UTC');
        $end = Carbon::parse('2024-02-10 01:00:00', 'UTC');
        // 1 month, 9 days and 1 hour with Carbon's 4-week month; not the 40-day calendar span.
        $fields = 28 * 86400 + 9 * 86400 + 3600;

        self::assertSame($fields, WorkflowDuration::seconds($start->diffAsCarbonInterval($end), 'timer'));
        self::assertSame($fields, WorkflowDuration::seconds($start->toDateTimeImmutable()->diff($end->toDateTimeImmutable()), 'timer'));
    }

    public function testNumericStringsConvertLikeTheNumbersTheyCoercedToBefore(): void
    {
        foreach (['30' => 30, '1.5' => 2, ' 30' => 30, '30 ' => 30, '1e3' => 1000, '.5' => 1, '-5' => 0, '0' => 0] as $input => $seconds) {
            self::assertSame($seconds, WorkflowDuration::seconds((string) $input, 'timer'), (string) $input);
            self::assertSame((string) $input, WorkflowDuration::normalize((string) $input, 'option', 1), (string) $input);
        }
    }

    /**
     * Seconds that SDK 3.9.0 produced for these inputs on Carbon 2.73.0 and 3.15.1. The #257 bound
     * rejects only values whose true total reaches 2^53 seconds, so every accepted value is unchanged.
     *
     * @return iterable<string, array{string|DateInterval, int}>
     */
    public static function acceptedBy390(): iterable
    {
        yield '30 minutes' => ['30 minutes', 1800];
        yield '1 hour 30 minutes' => ['1 hour 30 minutes', 5400];
        yield '1h 30m' => ['1h 30m', 5400];
        yield '2 days' => ['2 days', 172800];
        yield '1 week' => ['1 week', 604800];
        yield '1.5 weeks' => ['1.5 weeks', 907200];
        yield '90 seconds' => ['90 seconds', 90];
        yield '1.5 hours' => ['1.5 hours', 5400];
        yield '1 month' => ['1 month', 2419200];
        yield '1 year' => ['1 year', 29030400];
        yield '1 quarter' => ['1 quarter', 7257600];
        yield '1 decade' => ['1 decade', 290304000];
        yield '1 century' => ['1 century', 2903040000];
        yield '1 millennium' => ['1 millennium', 29030400000];
        yield '300000 millennia' => ['300000 millennia', 8709120000000000];
        yield '310000000 years' => ['310000000 years', 8999424000000000];
        yield '3720000000 months' => ['3720000000 months', 8999424000000000];
        yield '104249991374 days' => ['104249991374 days', 9007199254713600];
        yield '2501999792 hours' => ['2501999792 hours', 9007199251200];
        yield '150119987579 minutes' => ['150119987579 minutes', 9007199254740];
        yield '900719925474099 seconds' => ['900719925474099 seconds', 900719925474099];
        yield '999999999999999 ms' => ['999999999999999 ms', 1000000000000];
        yield '999999999999999 µs' => ['999999999999999 µs', 1000000000];
        yield '0.001 ms' => ['0.001 ms', 1];
        yield '1500 ms' => ['1500 ms', 2];
        yield '1 µs' => ['1 µs', 1];
        yield '0 seconds' => ['0 seconds', 0];
        yield '30' => ['30', 30];
        yield '1.5' => ['1.5', 2];
        yield 'DateInterval PT30M' => [new DateInterval('PT30M'), 1800];
        yield 'DateInterval P1Y2M3DT4H5M6S' => [new DateInterval('P1Y2M3DT4H5M6S'), 34142706];
        yield 'DateInterval P300000000Y' => [new DateInterval('P300000000Y'), 8709120000000000];
        yield 'DateInterval P104249991374D' => [new DateInterval('P104249991374D'), 9007199254713600];
    }

    #[DataProvider('acceptedBy390')]
    public function testAcceptedValuesConvertExactlyAsIn390(string|DateInterval $duration, int $seconds): void
    {
        self::assertSame($seconds, WorkflowDuration::seconds($duration, 'timer'));
    }

    /**
     * @param callable(WorkflowContext): mixed $workflow
     * @return list<array<string, mixed>>
     */
    private static function commandsFor(callable $workflow): array
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers')->commands;
    }

    private static function withMicroseconds(DateInterval $interval, float $fraction): DateInterval
    {
        $interval->f = $fraction;

        return $interval;
    }

    private static function withField(DateInterval $interval, string $field, int $value): DateInterval
    {
        $interval->{$field} = $value;

        return $interval;
    }

    private static function inverted(DateInterval $interval): DateInterval
    {
        $interval->invert = 1;

        return $interval;
    }
}
