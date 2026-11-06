<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ChildGroupBusinessReplayClockTest extends TestCase
{
    private const FORMAT = 'Y-m-d\TH:i:s.uP';
    private const FIRST = '2026-11-06T00:00:00.123400+00:00';
    private const SECOND = '2026-11-06T00:00:00.123499+00:00';

    #[DataProvider('successTimes')]
    public function testConsumedChildAndTimerUseCanonicalAuthoredOrder(string $second, string $expected): void
    {
        $handler = self::handler();
        $history = self::history($handler, times: [self::FIRST, $second]);
        $expectedResult = [['child-1', null], $expected];

        self::assertSame($expectedResult, $this->replay($handler, $history));
        self::assertSame($expectedResult, $this->replay($handler, $history));
    }

    public static function successTimes(): array
    {
        return [[self::SECOND, self::FIRST], ['2026-11-06T00:00:00.124500Z', '2026-11-06T00:00:00.124500+00:00'],
            ['2026-11-06T00:00:00.120000Z', self::FIRST]];
    }

    public function testTwoDeferredChildrenPreserveTheirResultsAndAuthoredTie(): void
    {
        $handler = self::handler(true);
        $history = self::history($handler);
        [$history[4], $history[5]] = [$history[5], $history[4]];

        self::assertSame([['child-1', 'child-2'], self::FIRST], $this->replay($handler, $history));
    }

    #[DataProvider('failures')]
    public function testCaughtChildFailureUsesOnlyItsParentTerminal(string $terminal): void
    {
        $handler = self::handler();
        $history = self::history($handler, $terminal, [self::FIRST, '2099-01-01T00:00:00Z']);

        self::assertSame([[$terminal, 'source child failure'], '2026-11-06T00:00:00.123000+00:00'], $this->replay($handler, $history));
    }

    public static function failures(): array
    {
        return [['ChildRunFailed'], ['ChildRunCancelled'], ['ChildRunTerminated']];
    }

    #[DataProvider('unprovedRows')]
    public function testEveryChildLifecycleRowMustProveIdentityPathAndScope(string $mutation): void
    {
        $handler = self::handler();
        $history = self::history($handler);
        switch ($mutation) {
            case 'opening-path': $history[0]['payload']['parallel_group_id'] = 'foreign'; break;
            case 'started-path': $history[1]['payload']['parallel_group_id'] = 'foreign'; break;
            case 'opening-scope': $history[0]['payload']['cancellation_scope_id'] = 'foreign'; break;
            case 'started-local': $history[1]['payload']['execution_mode'] = 'local'; break;
            case 'missing-call': unset($history[3]['payload']['child_call_id']); break;
            case 'wrong-instance': $history[3]['payload']['child_workflow_instance_id'] = 'foreign'; break;
            case 'wrong-run': $history[3]['payload']['child_workflow_run_id'] = 'foreign'; break;
            case 'alias-only':
                $history[3]['payload']['workflow_run_id'] = $history[3]['payload']['child_workflow_run_id'];
                unset($history[3]['payload']['child_workflow_run_id']); break;
            case 'duplicate-terminal': $history[] = $history[3]; break;
            case 'coerced-duplicate':
                $duplicate = $history[3]; $duplicate['payload']['sequence'] = '1'; $history[] = $duplicate; break;
            case 'duplicate-started': array_splice($history, 1, 0, [$history[1]]); break;
            case 'replacement':
                $replacement = $history[1]; $replacement['payload']['child_workflow_run_id'] = 'replacement';
                array_splice($history, 2, 0, [$replacement]);
                $history[4]['payload']['child_workflow_run_id'] = 'replacement'; break;
            case 'bad-time': $history[3]['recorded_at'] = '2026-02-30T00:00:00Z'; break;
        }

        if (in_array($mutation, ['opening-path', 'opening-scope'], true)) {
            $this->expectException($mutation === 'opening-path' ? NonDeterministicWorkflow::class : WorkflowClaimAborted::class);
        }
        self::assertSame([['child-1', null], null], $this->replay($handler, $history));
    }

    public static function unprovedRows(): array
    {
        return array_map(static fn (string $mutation): array => [$mutation], ['opening-path', 'started-path', 'opening-scope',
            'started-local', 'missing-call', 'wrong-instance', 'wrong-run', 'alias-only', 'duplicate-terminal',
            'coerced-duplicate', 'duplicate-started', 'replacement', 'bad-time']);
    }

    public function testNestedGroupsUseTheConsumedLeafPathAndOriginalPrecision(): void
    {
        $handler = static function (WorkflowContext $context): array {
            $values = $context->all([static fn (): array => $context->all([
                $context->deferChildWorkflow('clock-child'), $context->deferTimer(2),
            ]), $context->deferChildWorkflow('clock-child')]);

            return [$values, self::read($context)];
        };
        $history = self::history($handler, times: [self::FIRST, self::SECOND, self::SECOND]);

        self::assertSame([[['child-1', null], 'child-3'], self::FIRST], $this->replay($handler, $history));
    }

    public function testAConsumedChildGroupCannotLeaveTimeAvailableOutsideExecution(): void
    {
        $retained = null;
        $handler = static function (WorkflowContext $context) use (&$retained): string {
            $context->all([$context->deferChildWorkflow('clock-child'), $context->deferTimer(2)]);
            $retained = $context;

            return $context->replayNow()->format(self::FORMAT);
        };
        self::assertSame(self::FIRST, $this->replay($handler, self::history(self::handler())));
        $this->expectException(ReplayClockUnavailable::class);
        $retained->replayNow();
    }

    public function testTwoAuthoredChildrenCannotBorrowOnePhysicalCallIdentity(): void
    {
        $handler = self::handler(true);
        $history = self::history($handler);
        foreach ($history as &$event) {
            if (($event['payload']['sequence'] ?? null) === 2) {
                $event['payload']['child_call_id'] = 'call-1';
            }
        }
        unset($event);
        self::assertSame([['child-1', 'child-2'], null], $this->replay($handler, $history));
    }

    public function testDuplicateCallIdentityCannotSupplyACaughtFailureClock(): void
    {
        $handler = self::handler(true);
        $history = self::history($handler, 'ChildRunFailed');
        foreach ($history as &$event) {
            if (($event['payload']['sequence'] ?? null) === 2) {
                $event['payload']['child_call_id'] = 'call-1';
            }
        }
        unset($event);
        self::assertSame([['ChildRunFailed', 'source child failure'], null], $this->replay($handler, $history));
    }

    public function testDistinctCallsMayReuseTheSameChildRun(): void
    {
        $handler = self::handler(true);
        $history = self::history($handler);
        foreach ($history as &$event) {
            if (($event['payload']['sequence'] ?? null) === 2) {
                $event['payload']['child_workflow_instance_id'] = 'child-1';
                $event['payload']['child_workflow_run_id'] = 'run-1';
            }
        }
        unset($event);
        self::assertSame([['child-1', 'child-2'], self::FIRST], $this->replay($handler, $history));
    }

    private static function handler(bool $childrenOnly = false): callable
    {
        return static function (WorkflowContext $context) use ($childrenOnly): array {
            try {
                $values = $context->all([$context->deferChildWorkflow('clock-child'),
                    $childrenOnly ? $context->deferChildWorkflow('clock-child') : $context->deferTimer(2)]);
            } catch (ChildWorkflowFailed $failure) {
                $values = [$failure->failureType, $failure->getMessage()];
            }

            return [$values, self::read($context)];
        };
    }

    /** Test-only unavailable observation; the public accessor returns a time or throws. */
    private static function read(WorkflowContext $context): ?string
    {
        try { return $context->replayNow()->format(self::FORMAT); }
        catch (ReplayClockUnavailable) { return null; }
    }

    /** @param list<string> $times
     * @return list<array<string, mixed>>
     */
    private static function history(callable $handler, string $firstTerminal = 'ChildRunCompleted', array $times = [self::FIRST, self::SECOND]): array
    {
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($handler, [], [], 'php-workers')->commands;
        $history = [];
        $terminals = [];
        foreach ($commands as $offset => $command) {
            $sequence = $offset + 1;
            $body = array_intersect_key($command, array_flip(['parallel_group_id', 'parallel_group_kind', 'parallel_group_mode',
                'parallel_group_base_sequence', 'parallel_group_size', 'parallel_group_index', 'parallel_group_path']));
            $body['sequence'] = $sequence;
            $child = $command['type'] === 'start_child_workflow';
            if ($child) {
                $body += ['child_call_id' => 'call-'.$sequence, 'workflow_link_id' => 'link-'.$sequence,
                    'child_workflow_instance_id' => 'child-'.$sequence, 'child_workflow_run_id' => 'run-'.$sequence,
                    'child_workflow_type' => 'clock-child'];
                $history[] = ['event_type' => 'ChildWorkflowScheduled', 'payload' => $body];
                $history[] = ['event_type' => 'ChildRunStarted', 'recorded_at' => '1990-01-01T00:00:00Z', 'payload' => $body];
                $terminal = $offset === 0 ? $firstTerminal : 'ChildRunCompleted';
                $body += ['result' => $codec->envelope('child-'.$sequence), 'message' => 'source child failure',
                    'closed_at' => '2099-01-01T00:00:00Z', 'reused_recorded_at' => '2099-01-01T00:00:00Z'];
            } else {
                $body += ['timer_id' => 'timer-'.$sequence, 'delay_seconds' => 2];
                $history[] = ['event_type' => 'TimerScheduled', 'payload' => $body];
                $terminal = 'TimerFired';
            }
            $terminals[] = ['event_type' => $terminal, 'recorded_at' => $times[$offset], 'payload' => $body];
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
