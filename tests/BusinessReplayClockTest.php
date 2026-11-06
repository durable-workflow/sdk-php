<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use DateTimeImmutable;

final class BusinessReplayClockTest extends TestCase
{
    private const FIRST = '2026-11-06T00:00:00.123456+00:00';

    public function testDeadlineLoopAdvancesAtConsumedTimersAndReplaysCold(): void
    {
        $history = [...$this->activity(1, self::FIRST),
            ...self::timer(2, '2026-11-06T00:00:02.123456+00:00'),
            ...self::timer(3, '2026-11-06T00:00:04.123456+00:00')];
        $handler = static function (WorkflowContext $context): array {
            $context->activity('anchor');
            $start = $context->replayNow();
            self::assertInstanceOf(DateTimeImmutable::class, $start);
            self::assertSame($start, $context->replayNow());
            $deadline = $start->modify('+4 seconds');
            $times = [$start->format('Y-m-d\TH:i:s.uP')];
            while ($context->replayNow() < $deadline) {
                $context->sleep(2);
                $times[] = $context->replayNow()->format('Y-m-d\TH:i:s.uP');
            }

            return $times;
        };
        $first = $this->replayResult($handler, $history);
        $cold = $this->replayResult($handler, $history);

        self::assertSame([self::FIRST, '2026-11-06T00:00:02.123456+00:00', '2026-11-06T00:00:04.123456+00:00'], $first);
        self::assertSame($first, $cold);
    }

    #[DataProvider('nextInstants')]
    public function testEqualBackwardAndOffsetInstantsAreNotClamped(string $input, string $expected): void
    {
        $observed = $this->replayResult(static function (WorkflowContext $context): array {
            $context->activity('anchor');
            $first = $context->replayNow();
            $context->activity('anchor');

            return [$first->format('Y-m-d\TH:i:s.uP'), $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
        }, [...$this->activity(1, self::FIRST), ...$this->activity(2, $input)]);

        self::assertSame([self::FIRST, $expected], $observed);
    }

    public static function nextInstants(): array
    {
        return [
            'equal' => [self::FIRST, self::FIRST],
            'backward' => ['2026-11-05T23:59:59.000001Z', '2026-11-05T23:59:59.000001+00:00'],
            'offset' => ['2026-11-06T01:00:01.987654+01:00', '2026-11-06T00:00:01.987654+00:00'],
        ];
    }

    #[DataProvider('invalidTimes')]
    public function testInvalidPresentTimestampCannotFallBackToRecordedAt(mixed $timestamp): void
    {
        $history = $this->activity(1, self::FIRST);
        $history[1]['timestamp'] = $timestamp;

        $this->expectException(ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): string {
            $context->activity('anchor');

            return $context->replayNow()->format('c');
        }, $history);
    }

    public static function invalidTimes(): array
    {
        return array_map(static fn (mixed $value): array => [$value], [
            null, 123, '', '2026-02-30T00:00:00Z', '2026-11-06T24:00:00Z',
            '2026-11-06T00:00:00.1234567Z', '2026-11-06T00:00:00+24:00',
            '2026-11-06 00:00:00', 'now',
        ]);
    }

    public function testOnlyTheConsumedBoundarySuppliesTime(): void
    {
        $history = [...$this->activity(1, self::FIRST), ...$this->activity(2, 'invalid future time')];
        $observed = $this->replayResult(static function (WorkflowContext $context): array {
            $context->activity('anchor');
            $before = $context->replayNow()->format('Y-m-d\TH:i:s.uP');
            $context->activity('anchor');
            try { $context->replayNow(); } catch (ReplayClockUnavailable) { return [$before, 'unavailable']; }

            self::fail('The invalid consumed boundary must clear availability.');
        }, $history);

        self::assertSame([self::FIRST, 'unavailable'], $observed);
    }

    public function testARecordedSideEffectCannotRetainTimeAndANewScalarMayRecover(): void
    {
        $history = [...$this->activity(1, self::FIRST), [
            'event_type' => 'SideEffectRecorded', 'recorded_at' => '2026-11-06T00:00:02Z',
            'payload' => ['sequence' => 2, 'result' => (new AvroPayloadCodec())->envelope('fixed')],
        ], ...$this->activity(3, '2026-11-06T00:00:03Z')];
        $observed = $this->replayResult(static function (WorkflowContext $context): array {
            $context->activity('anchor');
            $context->sideEffect(static fn (): string => 'fixed');
            try { $context->replayNow(); } catch (ReplayClockUnavailable) {
                $context->activity('anchor');

                return ['unavailable', $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
            }
            self::fail('An inline result cannot retain the preceding clock.');
        }, $history);

        self::assertSame(['unavailable', '2026-11-06T00:00:03.000000+00:00'], $observed);
    }

    public function testANewInlineSideEffectCannotRetainThePrecedingTime(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): string {
            $context->activity('anchor');
            $context->sideEffect(static fn (): string => 'fixed');

            return $context->replayNow()->format('c');
        }, $this->activity(1, self::FIRST));
    }

    public function testFailureDeliveryKeepsItsOwnTimeAndOriginalError(): void
    {
        $history = $this->activity(1, self::FIRST);
        $history[1]['event_type'] = 'ActivityFailed';
        $history[1]['payload']['message'] = 'original activity failure';
        $history[1]['payload']['reused_recorded_at'] = '2026-01-01T00:00:00Z';
        $observed = $this->replayResult(static function (WorkflowContext $context): array {
            try { $context->activity('anchor'); } catch (ActivityFailed $error) {
                return [$error->getMessage(), $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
            }
            self::fail('The original activity failure must still be thrown.');
        }, $history);

        self::assertSame(['original activity failure', self::FIRST], $observed);
    }

    public function testRetainedContextCannotBeReadAfterScopeExit(): void
    {
        $retained = null;
        $this->replayResult(static function (WorkflowContext $context) use (&$retained): string {
            $context->activity('anchor');
            $retained = $context;

            return $context->replayNow()->format('c');
        }, $this->activity(1, self::FIRST));

        self::assertInstanceOf(WorkflowContext::class, $retained);
        $this->expectException(ReplayClockUnavailable::class);
        $retained->replayNow();
    }

    #[DataProvider('ambiguousActivityHistories')]
    public function testUnprovedActivityLifecyclesDoNotSupplyTime(string $mutation): void
    {
        $history = $this->activity(1, self::FIRST);
        switch ($mutation) {
            case 'missing-resource': unset($history[1]['payload']['activity_execution_id']); break;
            case 'different-resource': $history[1]['payload']['activity_execution_id'] = 'other-activity'; break;
            case 'coerced-sequence':
                foreach ($history as &$event) { $event['payload']['sequence'] = '1'; }
                unset($event);
                break;
            case 'duplicate-terminal': $history[] = $history[1]; break;
            case 'conflicting-terminal':
                $history[] = [...$history[1], 'event_type' => 'ActivityFailed'];
                break;
            case 'local-mode': $history[1]['payload']['execution_mode'] = 'local'; break;
            case 'local-flag': $history[1]['payload']['local_activity'] = true; break;
            case 'missing-opening': array_shift($history); break;
            case 'duplicate-opening': array_unshift($history, $history[0]); break;
            case 'started-before-opening':
                array_unshift($history, [...$history[0], 'event_type' => 'ActivityStarted']);
                break;
            case 'partial-reuse': $history[1]['payload']['reused_from_run_id'] = 'source'; break;
        }

        $this->expectException($mutation === 'local-mode' ? NonDeterministicWorkflow::class : ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): string {
            $context->activity('anchor');

            return $context->replayNow()->format('c');
        }, $history);
    }

    public static function ambiguousActivityHistories(): array
    {
        return array_map(static fn (string $mutation): array => [$mutation], [
            'missing-resource', 'different-resource', 'coerced-sequence', 'duplicate-terminal',
            'conflicting-terminal', 'local-mode', 'local-flag', 'missing-opening', 'duplicate-opening',
            'started-before-opening', 'partial-reuse',
        ]);
    }

    public function testNestedInvocationCannotBorrowParentTimeAndAnErrorRestoresParentScope(): void
    {
        $failure = new \LogicException('original nested error');
        $observed = $this->replayResult(function (WorkflowContext $parent) use ($failure): array {
            $parent->activity('anchor');
            $before = $parent->replayNow();
            try {
                $this->replayResult(static function (WorkflowContext $nested) use ($parent, $failure): never {
                    $nested->activity('anchor');
                    self::assertSame('2026-11-06T01:00:00.000000+00:00', $nested->replayNow()->format('Y-m-d\TH:i:s.uP'));
                    try {
                        $parent->replayNow();
                        self::fail('A different workflow Fiber cannot read the parent clock.');
                    } catch (ReplayClockUnavailable) {
                        throw $failure;
                    }
                }, $this->activity(1, '2026-11-06T01:00:00Z'));
                self::fail('The nested error must propagate unchanged.');
            } catch (\LogicException $error) {
                self::assertSame($failure, $error);
            }

            return [$before->format('Y-m-d\TH:i:s.uP'), $parent->replayNow()->format('Y-m-d\TH:i:s.uP')];
        }, $this->activity(1, self::FIRST));

        self::assertSame([self::FIRST, self::FIRST], $observed);
    }

    public function testExecutorPredicateCannotBorrowTheWorkflowClock(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): bool {
            $context->activity('anchor');

            return $context->waitCondition(static fn (): bool => $context->replayNow() instanceof DateTimeImmutable, 'clock-condition');
        }, $this->activity(1, self::FIRST));
    }

    public function testGroupCaptureCallbackCannotBorrowTheWorkflowClock(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): array {
            $context->activity('anchor');

            return $context->all([static fn () => $context->replayNow()]);
        }, $this->activity(1, self::FIRST));
    }

    public function testForceClosedFinallyHasNoActiveBusinessClock(): void
    {
        $observed = null;
        (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use (&$observed): void {
                $context->activity('anchor');
                try {
                    $context->sleep(5);
                } finally {
                    try {
                        $observed = $context->replayNow()->format('c');
                    } catch (ReplayClockUnavailable) {
                        $observed = 'unavailable';
                    }
                }
            },
            $this->activity(1, self::FIRST), [], 'php-workers',
            ['workflow_id' => 'workflow', 'run_id' => 'run'],
        );
        gc_collect_cycles();

        self::assertSame('unavailable', $observed);
    }

    /** @return list<array<string, mixed>> */
    private function activity(int $sequence, string $timestamp): array
    {
        $payload = ['sequence' => $sequence, 'activity_execution_id' => 'activity-'.$sequence, 'activity_type' => 'anchor'];

        return [
            ['event_type' => 'ActivityScheduled', 'payload' => $payload],
            ['event_type' => 'ActivityCompleted', 'recorded_at' => $timestamp,
                'payload' => [...$payload, 'result' => (new AvroPayloadCodec())->envelope('fixed')]],
        ];
    }

    /** @return list<array<string, mixed>> */
    private static function timer(int $sequence, string $timestamp): array
    {
        $payload = ['sequence' => $sequence, 'timer_id' => 'timer-'.$sequence, 'delay_seconds' => 2];

        return [
            ['event_type' => 'TimerScheduled', 'payload' => $payload],
            ['event_type' => 'TimerFired', 'recorded_at' => $timestamp, 'payload' => $payload],
        ];
    }

    /** @param list<array<string, mixed>> $history */
    private function replayResult(callable $handler, array $history): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', ['workflow_id' => 'workflow', 'run_id' => 'run']);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }

    public function testDirectContextHasNoBusinessReplayTime(): void
    {
        $context = new WorkflowContext('workflow', 'run', [], new AvroPayloadCodec());

        $this->expectException(ReplayClockUnavailable::class);
        $context->replayNow();
    }

    public function testQuerySnapshotDoesNotGuessAConsumedPrefix(): void
    {
        $context = new QueryContext('workflow', 'run', [[
            'event_type' => 'ActivityCompleted',
            'recorded_at' => '2026-11-06T00:00:00.123456+00:00',
            'payload' => ['sequence' => 1, 'activity_execution_id' => 'activity'],
        ]], []);

        $this->expectException(ReplayClockUnavailable::class);
        $context->replayNow();
    }

    public function testOrdinaryEntryDoesNotSubstituteHistoryInsertionTime(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->replayNow(),
            [[
                'event_type' => 'WorkflowStarted',
                'recorded_at' => '2026-11-06T00:00:00.123456+00:00',
                'payload' => ['workflow_run_id' => 'run'],
            ]],
            [],
            'php-workers',
            ['workflow_id' => 'workflow', 'run_id' => 'run'],
        );
    }
}
