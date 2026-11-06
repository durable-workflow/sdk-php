<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class GroupBusinessReplayClockTest extends TestCase
{
    private const FORMAT = 'Y-m-d\TH:i:s.uP';

    #[DataProvider('successInstants')]
    public function testConsumedGroupFoldsAuthoredOrderWithCoreMilliseconds(string $first, string $second, string $expected): void
    {
        $history = self::history(['activity', 'timer'], [$first, $second]);
        $handler = static function (WorkflowContext $context): array {
            $values = $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$values, self::read($context)];
        };

        self::assertSame([['first', null], $expected], $this->replay($handler, $history));
        self::assertSame($this->replay($handler, $history), $this->replay($handler, $history));
    }

    public static function successInstants(): array
    {
        return [
            ['2026-11-06T00:00:00.123400Z', '2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123400+00:00'],
            ['2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123500Z', '2026-11-06T00:00:00.123500+00:00'],
            ['2026-11-06T00:00:00.123501Z', '2026-11-06T00:00:00.124499Z', '2026-11-06T00:00:00.123501+00:00'],
            ['2026-11-06T00:00:00.124499Z', '2026-11-06T00:00:00.124500Z', '2026-11-06T00:00:00.124500+00:00'],
            ['2026-11-06T00:00:01Z', '2026-11-06T00:00:00Z', '2026-11-06T00:00:01.000000+00:00'],
            ['2026-11-06T01:00:00.123400+01:00', '2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123400+00:00'],
        ];
    }

    public function testPhysicalTerminalOrderCannotChangeTheAuthoredTieWinner(): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:00.123400Z', '2026-11-06T00:00:00.123499Z']);
        [$history[2], $history[3]] = [$history[3], $history[2]];

        self::assertSame('2026-11-06T00:00:00.123400+00:00', $this->replay(static function (WorkflowContext $context): ?string {
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return self::read($context);
        }, $history));
    }

    #[DataProvider('failureInstants')]
    public function testCaughtFailureUsesCoreCarbonReconstructionAndPreservesException(string $input, string $expected): void
    {
        $history = self::history(['activity', 'activity'], [$input, '2099-01-01T00:00:00Z'], [0]);

        self::assertSame(['original-0', $expected], $this->replay(self::failureHandler(), $history));
    }

    /** Frozen from Carbon 3.15.1 ce0e5636 getTimestampMs/createFromTimestampMs, not a floor operation. */
    public static function failureInstants(): array
    {
        return [
            ['2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123000+00:00'],
            ['2026-11-06T00:00:00.123500Z', '2026-11-06T00:00:00.124000+00:00'],
            ['2026-11-06T00:00:00.123501Z', '2026-11-06T00:00:00.124000+00:00'],
            ['2026-11-06T00:00:00.124499Z', '2026-11-06T00:00:00.124000+00:00'],
            ['2026-11-06T00:00:00.124500Z', '2026-11-06T00:00:00.125000+00:00'],
            ['2026-11-06T00:00:00.124501Z', '2026-11-06T00:00:00.125000+00:00'],
            ['2026-11-06T00:00:00.000499Z', '2026-11-06T00:00:00.000000+00:00'],
            ['2026-11-06T00:00:00.000500Z', '2026-11-06T00:00:00.001000+00:00'],
            ['1969-12-31T23:59:59.999499Z', '1969-12-31T23:59:59.999000+00:00'],
            ['1969-12-31T23:59:59.999500Z', '1969-12-31T23:59:59.999000+00:00'],
            ['1969-12-31T23:59:59.999501Z', '1970-01-01T00:00:00.000000+00:00'],
            ['1969-12-31T23:59:59.123500Z', '1969-12-31T23:59:59.123000+00:00'],
        ];
    }

    public function testConflictingFailureOrderKeepsTheLegacyExceptionAndRefusesClock(): void
    {
        $history = self::history(['activity', 'activity'], ['2026-11-06T00:00:06Z', '2026-11-06T00:00:05Z'], [0, 1]);

        self::assertSame(['original-0', null], $this->replay(self::failureHandler(), $history));
    }

    public function testFailureSameMillisecondTieMustAgreeWithTheLegacyWinner(): void
    {
        $history = self::history(['activity', 'activity'], ['2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123400Z'], [0, 1]);
        [$history[2], $history[3]] = [$history[3], $history[2]];

        self::assertSame(['original-1', null], $this->replay(self::failureHandler(), $history));
    }

    public function testAgreeingMultipleFailureUsesOnlyTheSelectedMember(): void
    {
        $history = self::history(['activity', 'activity'], ['2026-11-06T00:00:05.123500Z', '2026-11-06T00:00:06Z'], [0, 1]);

        self::assertSame(['original-0', '2026-11-06T00:00:05.124000+00:00'], $this->replay(self::failureHandler(), $history));
    }

    public function testUnconsumedDifferentGroupCannotSupplyTime(): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        $future = self::history(['activity', 'timer'], ['2099-01-01T00:00:00Z', '2099-01-01T00:00:00Z'], base: 3);

        self::assertSame('2026-11-06T00:00:02.000000+00:00', $this->replay(static function (WorkflowContext $context): ?string {
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
            $before = self::read($context);
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return $before;
        }, [...$history, ...$future]));
    }

    public function testPendingLaterGroupAndDiscardFinallyCannotBorrowTime(): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        $pending = array_slice(self::history(['activity', 'timer'], ['2099-01-01T00:00:00Z', '2099-01-01T00:00:00Z'], base: 3), 0, 2);
        foreach ($pending as &$row) { $row['recorded_at'] = '2099-01-01T00:00:00Z'; }
        unset($row);
        $before = null;
        $finallyUnavailable = false;
        (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context) use (&$before, &$finallyUnavailable): void {
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
            $before = self::read($context);
            try { $context->all([$context->deferActivity('first'), $context->deferTimer(2)]); }
            finally {
                try { $context->replayNow(); } catch (ReplayClockUnavailable) { $finallyUnavailable = true; }
            }
        }, [...$history, ...$pending], [], 'php-workers', ['workflow_id' => 'parent', 'run_id' => 'run']);
        gc_collect_cycles();

        self::assertSame('2026-11-06T00:00:02.000000+00:00', $before);
        self::assertTrue($finallyUnavailable);
    }

    public function testDuplicateTerminalCannotBorrowAnotherMembersClock(): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        $history[] = $history[2];

        self::assertSame([['first', null], null], $this->replay(static function (WorkflowContext $context): array {
            $values = $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$values, self::read($context)];
        }, $history));
    }

    private static function failureHandler(): callable
    {
        return static function (WorkflowContext $context): array {
            try {
                $context->all([$context->deferActivity('first'), $context->deferActivity('second')]);
            } catch (ActivityFailed $failure) {
                return [$failure->getMessage(), self::read($context)];
            }
            self::fail('The original group exception must still be thrown.');
        };
    }

    /** Test-only unavailable observation; the product accessor always returns a time or throws. */
    private static function read(WorkflowContext $context): ?string
    {
        try { return $context->replayNow()->format(self::FORMAT); }
        catch (ReplayClockUnavailable) { return null; }
    }

    /** @param list<string> $families
     * @param list<string> $times
     * @param list<int> $failures
     * @return list<array<string, mixed>>
     */
    private static function history(array $families, array $times, array $failures = [], int $base = 1): array
    {
        $kind = count(array_unique($families)) === 1 ? 'activity' : 'mixed';
        $prefix = $kind === 'activity' ? 'parallel-activities' : 'parallel-calls';
        $history = [];
        $terminals = [];
        foreach ($families as $offset => $family) {
            $sequence = $base + $offset;
            $entry = ['parallel_group_id' => $prefix.':'.$base.':2', 'parallel_group_kind' => $kind,
                'parallel_group_base_sequence' => $base, 'parallel_group_size' => 2, 'parallel_group_index' => $offset];
            $body = ['sequence' => $sequence, ...$entry, 'parallel_group_path' => [$entry]];
            if ($family === 'activity') {
                $body += ['activity_execution_id' => 'activity-'.$sequence, 'activity_type' => $offset === 0 ? 'first' : 'second'];
                $opening = 'ActivityScheduled';
                $terminal = in_array($offset, $failures, true) ? 'ActivityFailed' : 'ActivityCompleted';
                $result = ['message' => 'original-'.$offset, 'result' => (new AvroPayloadCodec())->envelope($offset === 0 ? 'first' : 'second')];
            } else {
                $body += ['timer_id' => 'timer-'.$sequence, 'delay_seconds' => 2];
                $opening = 'TimerScheduled';
                $terminal = 'TimerFired';
                $result = [];
            }
            $history[] = ['event_type' => $opening, 'payload' => $body];
            $terminals[] = ['event_type' => $terminal, 'recorded_at' => $times[$offset], 'payload' => [...$body, ...$result]];
        }

        return [...$history, ...$terminals];
    }

    /** @param list<array<string, mixed>> $history */
    private function replay(callable $handler, array $history): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', ['workflow_id' => 'parent', 'run_id' => 'run']);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
