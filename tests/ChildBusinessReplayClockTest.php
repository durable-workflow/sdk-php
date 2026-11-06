<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ChildBusinessReplayClockTest extends TestCase
{
    private const TERMINAL_TIME = '2026-11-06T02:00:00.123456+00:00';

    #[DataProvider('terminalTypes')]
    public function testTheConsumedParentTerminalSuppliesTimeAndPreservesOutcome(string $type): void
    {
        $handler = static function (WorkflowContext $context): array {
            try {
                $outcome = $context->childWorkflow('clock-child');
            } catch (ChildWorkflowFailed $failure) {
                $outcome = [$failure->failureType, $failure->getMessage()];
            }

            return [$outcome, $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
        };
        $history = self::history($type);
        $expectedOutcome = $type === 'ChildRunCompleted' ? 'child-value' : [$type, 'original child failure'];

        self::assertSame([$expectedOutcome, self::TERMINAL_TIME], $this->replayResult($handler, $history));
        self::assertSame($this->replayResult($handler, $history), $this->replayResult($handler, $history));
    }

    public static function terminalTypes(): array
    {
        return [['ChildRunCompleted'], ['ChildRunFailed'], ['ChildRunCancelled'], ['ChildRunTerminated']];
    }

    public function testStartedHandleCannotReadAFutureTerminalButAwaitConsumesIt(): void
    {
        $observed = $this->replayResult(static function (WorkflowContext $context): array {
            $child = $context->startChildWorkflow('clock-child');
            try { $context->replayNow(); } catch (ReplayClockUnavailable) { $started = 'unavailable'; }
            $result = $child->await();

            return [$started ?? 'borrowed', $result, $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
        }, self::history('ChildRunCompleted'));

        self::assertSame(['unavailable', 'child-value', self::TERMINAL_TIME], $observed);
    }

    public function testScheduledAndTerminalAreSufficientWithoutAStartedRow(): void
    {
        $history = self::history('ChildRunCompleted');
        array_splice($history, 1, 1);

        self::assertSame(self::TERMINAL_TIME, $this->replayResult(self::timeHandler(), $history));
    }

    #[DataProvider('terminalInstants')]
    public function testTheExactParentInstantIsPreserved(string $input, string $expected): void
    {
        $history = self::history('ChildRunCompleted');
        $history[2]['recorded_at'] = $input;

        self::assertSame($expected, $this->replayResult(self::timeHandler(), $history));
    }

    public static function terminalInstants(): array
    {
        return [
            ['2026-11-06T01:00:00Z', '2026-11-06T01:00:00.000000+00:00'],
            ['2026-11-06T00:59:59.000001Z', '2026-11-06T00:59:59.000001+00:00'],
            ['2026-11-06T03:00:00.654321+01:00', '2026-11-06T02:00:00.654321+00:00'],
        ];
    }

    public function testOnlyTheConsumedChildCanSupplyTime(): void
    {
        $history = self::history('ChildRunCompleted');
        $second = self::history('ChildRunCompleted');
        foreach ($second as &$event) {
            $event['payload']['sequence'] = 2;
            $event['payload']['child_call_id'] = 'second-call';
            $event['payload']['child_workflow_instance_id'] = 'second-child';
            $event['payload']['child_workflow_run_id'] = 'second-run';
            $event['recorded_at'] = 'invalid future clock';
        }
        unset($event);
        $result = $this->replayResult(static function (WorkflowContext $context): array {
            $context->childWorkflow('clock-child');
            $first = $context->replayNow()->format('Y-m-d\TH:i:s.uP');
            $context->childWorkflow('clock-child');
            try { $context->replayNow(); } catch (ReplayClockUnavailable) { return [$first, 'unavailable']; }
            self::fail('Invalid consumed time must not retain the prior child clock.');
        }, [...$history, ...$second]);

        self::assertSame([self::TERMINAL_TIME, 'unavailable'], $result);
    }

    public function testChildCallsDoNotCreateATimeWithoutAnActiveCallback(): void
    {
        $retained = null;
        $this->replayResult(static function (WorkflowContext $context) use (&$retained): string {
            $context->childWorkflow('clock-child');
            $retained = $context;

            return $context->replayNow()->format('c');
        }, self::history('ChildRunCompleted'));

        $this->expectException(ReplayClockUnavailable::class);
        $retained->replayNow();
    }

    private static function timeHandler(): callable
    {
        return static function (WorkflowContext $context): string {
            $context->childWorkflow('clock-child');

            return $context->replayNow()->format('Y-m-d\TH:i:s.uP');
        };
    }

    #[DataProvider('unprovedLifecycles')]
    public function testUnprovedChildIdentityAndLifecycleCannotSupplyTime(string $mutation): void
    {
        $history = self::history('ChildRunCompleted');
        switch ($mutation) {
            case 'missing-call': unset($history[2]['payload']['child_call_id']); break;
            case 'wrong-call': $history[2]['payload']['child_call_id'] = 'other-call'; break;
            case 'wrong-run': $history[2]['payload']['child_workflow_run_id'] = 'other-run'; break;
            case 'wrong-instance': $history[2]['payload']['child_workflow_instance_id'] = 'other-instance'; break;
            case 'alias-only':
                $history[2]['payload']['workflow_run_id'] = $history[2]['payload']['child_workflow_run_id'];
                unset($history[2]['payload']['child_workflow_run_id']); break;
            case 'conflicting-alias': $history[2]['payload']['workflow_run_id'] = 'other-run'; break;
            case 'coerced-sequence': foreach ($history as &$row) { $row['payload']['sequence'] = '1'; } unset($row); break;
            case 'duplicate-terminal': $history[] = $history[2]; break;
            case 'conflicting-terminal': $history[] = [...$history[2], 'event_type' => 'ChildRunFailed']; break;
            case 'missing-opening': array_shift($history); break;
            case 'duplicate-opening': array_unshift($history, $history[0]); break;
            case 'duplicate-started': array_splice($history, 1, 0, [$history[1]]); break;
            case 'started-after-terminal': [$history[1], $history[2]] = [$history[2], $history[1]]; break;
            case 'replacement-run':
                $replacement = $history[1];
                $replacement['payload']['child_workflow_run_id'] = 'replacement';
                array_splice($history, 2, 0, [$replacement]);
                $history[3]['payload']['child_workflow_run_id'] = 'replacement'; break;
            case 'bad-time': $history[2]['recorded_at'] = '2026-02-30T00:00:00Z'; break;
        }

        $this->expectException(ReplayClockUnavailable::class);
        $this->replayResult(static function (WorkflowContext $context): string {
            self::assertSame('child-value', $context->childWorkflow('clock-child'));

            return $context->replayNow()->format('c');
        }, $history);
    }

    public static function unprovedLifecycles(): array
    {
        return array_map(static fn (string $mutation): array => [$mutation], [
            'missing-call', 'wrong-call', 'wrong-run', 'wrong-instance', 'alias-only', 'conflicting-alias', 'coerced-sequence',
            'duplicate-terminal', 'conflicting-terminal', 'missing-opening', 'duplicate-opening',
            'duplicate-started', 'started-after-terminal', 'replacement-run', 'bad-time',
        ]);
    }

    /** @return list<array<string, mixed>> */
    private static function history(string $terminal): array
    {
        $payload = ['sequence' => 1, 'child_call_id' => 'call', 'workflow_link_id' => 'call',
            'child_workflow_instance_id' => 'child-instance', 'child_workflow_run_id' => 'child-run',
            'child_workflow_type' => 'clock-child'];

        return [
            ['event_type' => 'ChildWorkflowScheduled', 'payload' => $payload],
            ['event_type' => 'ChildRunStarted', 'recorded_at' => '2026-11-06T01:00:00Z', 'payload' => $payload],
            ['event_type' => $terminal, 'recorded_at' => self::TERMINAL_TIME,
                'payload' => [...$payload, 'closed_at' => '1990-01-01T00:00:00Z',
                    'reused_recorded_at' => '2099-01-01T00:00:00Z',
                    'message' => 'original child failure', 'result' => (new AvroPayloadCodec())->envelope('child-value')]],
        ];
    }

    /** @param list<array<string, mixed>> $history */
    private function replayResult(callable $handler, array $history): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', ['workflow_id' => 'parent', 'run_id' => 'parent-run']);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
