<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\GroupReplayClockEvidence;
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

    /** Canonical-v1 source table is distinct from the preserved legacy Carbon observations. */
    public static function failureInstants(): array
    {
        $table = json_decode((string) file_get_contents(__DIR__.'/fixtures/replay-clock/canonical-v1.json'), true, flags: JSON_THROW_ON_ERROR);

        return array_map(static fn (array $row): array => [$row['input'], $row['failure_time']], $table['vectors']);
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

    #[DataProvider('duplicateSequences')]
    public function testDuplicateTerminalCannotBorrowAnotherMembersClock(int|string $sequence): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        $history[] = $history[2];
        $history[4]['payload']['sequence'] = $sequence;

        self::assertSame([['first', null], null], $this->replay(static function (WorkflowContext $context): array {
            $values = $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$values, self::read($context)];
        }, $history));
    }

    public static function duplicateSequences(): array
    {
        return [[1], ['1']];
    }

    public function testEmptyGroupAddsNoBoundaryAndCannotInventEntryTime(): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        self::assertSame([[], '2026-11-06T00:00:02.000000+00:00'], $this->replay(static function (WorkflowContext $context): array {
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$context->all([]), self::read($context)];
        }, $history));
        self::assertSame([[], null], $this->replay(static fn (WorkflowContext $context): array => [$context->all([]), self::read($context)], []));
    }

    public function testGroupTimeDoesNotClampToThePrecedingScalar(): void
    {
        $body = ['sequence' => 1, 'activity_execution_id' => 'prefix', 'activity_type' => 'prefix'];
        $history = [
            ['event_type' => 'ActivityScheduled', 'payload' => $body],
            ['event_type' => 'ActivityCompleted', 'recorded_at' => '2099-01-01T00:00:00Z',
                'payload' => [...$body, 'result' => (new AvroPayloadCodec())->envelope('prefix')]],
            ...self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z'], base: 2),
        ];

        self::assertSame(['2099-01-01T00:00:00.000000+00:00', '2026-11-06T00:00:02.000000+00:00'], $this->replay(static function (WorkflowContext $context): array {
            $context->activity('prefix');
            $before = self::read($context);
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$before, self::read($context)];
        }, $history));
    }

    public function testCapturedCallbackCannotBorrowThePreviousGroupTime(): void
    {
        $history = [...self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']),
            ...self::history(['activity', 'timer'], ['2026-11-06T00:00:03Z', '2026-11-06T00:00:04Z'], base: 3)];

        self::assertSame([true, '2026-11-06T00:00:04.000000+00:00'], $this->replay(static function (WorkflowContext $context): array {
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
            $unavailable = false;
            $context->all([static function () use ($context, &$unavailable): mixed {
                try { $context->replayNow(); } catch (ReplayClockUnavailable) { $unavailable = true; }

                return $context->activity('first');
            }, $context->deferTimer(2)]);

            return [$unavailable, self::read($context)];
        }, $history));
    }

    public function testNestedAllKeepsItsResultsAndAuthoredLeafTieWinner(): void
    {
        $handler = static function (WorkflowContext $context): array {
            $values = $context->all([static fn (): array => $context->all([$context->deferActivity('first'), $context->deferTimer(2)]),
                $context->deferActivity('second')]);

            return [$values, self::read($context)];
        };
        $commands = (new Replayer(new AvroPayloadCodec()))->replay($handler, [], [], 'php-workers')->commands;
        $history = [];
        $terminals = [];
        foreach ($commands as $offset => $command) {
            $body = array_intersect_key($command, array_flip(['parallel_group_id', 'parallel_group_kind', 'parallel_group_mode',
                'parallel_group_base_sequence', 'parallel_group_size', 'parallel_group_index', 'parallel_group_path']));
            $body['sequence'] = $offset + 1;
            $timer = $command['type'] === 'start_timer';
            $body += $timer ? ['timer_id' => 'nested-timer', 'delay_seconds' => 2]
                : ['activity_execution_id' => 'nested-activity-'.$offset, 'activity_type' => $command['activity_type']];
            $history[] = ['event_type' => $timer ? 'TimerScheduled' : 'ActivityScheduled', 'payload' => $body];
            $value = $timer ? [] : ['result' => (new AvroPayloadCodec())->envelope($command['activity_type'])];
            $terminals[] = ['event_type' => $timer ? 'TimerFired' : 'ActivityCompleted',
                'recorded_at' => ['2026-11-06T00:00:00.123400Z', '2026-11-06T00:00:00.123499Z', '2026-11-06T00:00:00.123300Z'][$offset],
                'payload' => [...$body, ...$value]];
        }

        self::assertSame([[['first', null], 'second'], '2026-11-06T00:00:00.123400+00:00'], $this->replay($handler, [...$history, ...array_reverse($terminals)]));
    }

    #[DataProvider('missingTimes')]
    public function testMissingAuthorityCannotBorrowSiblingTimeAndALaterGroupRecovers(string $mutation): void
    {
        $history = self::history(['activity', 'timer'], ['2026-11-06T00:00:01Z', '2026-11-06T00:00:02Z']);
        if ($mutation === 'absent') {
            unset($history[2]['recorded_at']);
        } else {
            $history[2]['timestamp'] = $mutation === 'null-alias' ? null : '2026-02-30T00:00:00Z';
        }
        $history = [...$history, ...self::history(['activity', 'timer'], ['2026-11-06T00:00:03Z', '2026-11-06T00:00:04Z'], base: 3)];

        self::assertSame([['first', null], null, '2026-11-06T00:00:04.000000+00:00'], $this->replay(static function (WorkflowContext $context): array {
            $values = $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
            $before = self::read($context);
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$values, $before, self::read($context)];
        }, $history));
    }

    public static function missingTimes(): array
    {
        return [['absent'], ['null-alias'], ['invalid-alias']];
    }

    public function testUnrepresentableMicrosecondArithmeticRefusesInsteadOfWrapping(): void
    {
        foreach ([PHP_INT_MIN, PHP_INT_MAX] as $seconds) {
            $time = new \DateTimeImmutable('@'.$seconds);
            $candidate = [['time' => $time, 'offset' => 0, 'order' => 0]];
            self::assertNull(GroupReplayClockEvidence::time($candidate, null));
            self::assertNull(GroupReplayClockEvidence::time($candidate, 0));
        }
    }

    public function testFreshGroupSuffixUsesItsOwnTimeAfterAReusedScalarPrefix(): void
    {
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/replay-regressions/business-replay-clock-canonical-redrive.json'), true, flags: JSON_THROW_ON_ERROR);
        $prefix = array_slice($fixture['history'], 0, 2);
        $prefix[0]['payload']['resume_step_sequence'] = 2;
        $history = [...$prefix, ...self::history(['activity', 'timer'], ['2026-11-03T00:05:01Z', '2026-11-03T00:05:02Z'], base: 2)];
        foreach ($history as $index => &$row) { $row['sequence'] = $index + 1; }
        unset($row);
        $activity = $prefix[1]['payload']['activity_type'];

        self::assertSame(['2026-11-03T00:00:00.000000+00:00', '2026-11-03T00:05:02.000000+00:00'], $this->replay(static function (WorkflowContext $context) use ($activity): array {
            $context->activity($activity);
            $before = self::read($context);
            $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);

            return [$before, self::read($context)];
        }, $history, $fixture['workflow_tasks'][0]));
    }

    public function testHypotheticalReusedGroupPrefixRemainsUnqualified(): void
    {
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/replay-regressions/business-replay-clock-canonical-redrive.json'), true, flags: JSON_THROW_ON_ERROR);
        $start = $fixture['history'][0];
        $history = array_slice(self::history(['activity', 'activity'], [$start['recorded_at'], $start['recorded_at']]), 2);
        foreach ($history as $offset => &$row) {
            $row['payload']['reused_from_run_id'] = $start['payload']['continued_from_run_id'];
            $row['payload']['reused_recorded_at'] = $start['payload']['replayed_started_at'];
            $row['payload']['reused_activity_execution_id'] = 'source-activity-'.$offset;
        }
        unset($row);
        array_unshift($history, $start);
        foreach ($history as $index => &$row) { $row['sequence'] = $index + 1; }
        unset($row);

        self::assertSame([['first', 'second'], null], $this->replay(static function (WorkflowContext $context): array {
            $values = $context->all([$context->deferActivity('first'), $context->deferActivity('second')]);

            return [$values, self::read($context)];
        }, $history, $fixture['workflow_tasks'][0]));
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
    private function replay(callable $handler, array $history, array $task = ['workflow_id' => 'parent', 'run_id' => 'run']): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', $task);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
