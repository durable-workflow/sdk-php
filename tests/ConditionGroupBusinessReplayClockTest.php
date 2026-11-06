<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConditionGroupBusinessReplayClockTest extends TestCase
{
    private const FORMAT = 'Y-m-d\TH:i:s.uP';
    private const FIRST = '2026-11-06T00:00:00.123400+00:00';
    private const SECOND = '2026-11-06T00:00:00.123499+00:00';

    #[DataProvider('conditionOutcomes')]
    public function testPersistedConditionGroupPreservesResultsAndExactAuthoredTie(string $terminal, bool $backed): void
    {
        $handler = self::handler();
        $history = self::history($handler, $terminal, $backed);
        $expected = [[$terminal === 'ConditionWaitSatisfied', null], self::FIRST];

        self::assertSame($expected, $this->replay($handler, $history));
        self::assertSame($expected, $this->replay($handler, $history));
    }

    public static function conditionOutcomes(): array
    {
        return [['ConditionWaitSatisfied', false], ['ConditionWaitSatisfied', true], ['ConditionWaitTimedOut', true]];
    }

    public function testConditionAndSiblingUseTheirOwnConsumedTimes(): void
    {
        $handler = self::handler();
        $history = self::history($handler, times: [self::FIRST, '2026-11-06T00:00:00.124500Z']);
        // A later, unconsumed condition is not part of the first group's fold.
        $history[] = ['event_type' => 'ConditionWaitSatisfied', 'recorded_at' => '2099-01-01T00:00:00Z',
            'payload' => ['sequence' => 3, 'condition_wait_id' => 'future']];

        self::assertSame([[true, null], '2026-11-06T00:00:00.124500+00:00'], $this->replay($handler, $history));
    }

    public function testTwoConditionsCannotBorrowOnePhysicalWait(): void
    {
        $handler = self::handler(true);
        $history = self::history($handler);
        foreach ($history as &$event) {
            if (($event['payload']['sequence'] ?? null) === 2) {
                $event['payload']['condition_wait_id'] = 'wait-1';
            }
        }
        unset($event);

        self::assertSame([[true, true], null], $this->replay($handler, $history));
    }

    public function testNestedConditionGroupsUseTheAuthoredLeafPaths(): void
    {
        $handler = static function (WorkflowContext $context): array {
            $values = $context->all([static fn (): array => $context->all([
                $context->deferCondition(static fn (): bool => true, 'clock-condition', 5), $context->deferTimer(2),
            ]), $context->deferTimer(2)]);

            return [$values, self::read($context)];
        };

        self::assertSame([[[true, null], null], self::FIRST], $this->replay($handler,
            self::history($handler, times: [self::FIRST, self::SECOND, self::SECOND])));
    }

    #[DataProvider('unprovedRows')]
    public function testUnprovedConditionLifecycleCannotSupplyTheGroupClock(string $mutation): void
    {
        $handler = self::handler();
        $history = self::history($handler, backed: true);
        switch ($mutation) {
            case 'terminal-path': $history[3]['payload']['parallel_group_id'] = 'foreign'; break;
            case 'terminal-scope': $history[3]['payload']['cancellation_scope_id'] = 'foreign'; break;
            case 'terminal-local': $history[3]['payload']['execution_mode'] = 'local'; break;
            case 'foreign-wait': $history[3]['payload']['condition_wait_id'] = 'foreign'; break;
            case 'foreign-definition': $history[3]['payload']['condition_definition_fingerprint'] = 'foreign'; break;
            case 'foreign-occurrence': $history[3]['payload']['condition_wait_occurrence_id'] = 'foreign'; break;
            case 'timer-resource': $history[2]['payload']['timer_id'] = 'foreign'; break;
            case 'timer-kind': $history[2]['payload']['timer_kind'] = 'signal_timeout'; break;
            case 'timer-path': $history[1]['payload']['parallel_group_id'] = 'foreign'; break;
            case 'duplicate-timer': array_splice($history, 2, 0, [$history[1]]); break;
            case 'duplicate-terminal': $history[] = $history[3]; break;
            case 'coerced-duplicate':
                $duplicate = $history[3]; $duplicate['payload']['sequence'] = '1'; $history[] = $duplicate; break;
            case 'bad-time': $history[3]['recorded_at'] = '2026-02-30T00:00:00Z'; break;
        }

        if (in_array($mutation, ['terminal-path', 'terminal-scope'], true)) {
            $this->expectException($mutation === 'terminal-path' ? NonDeterministicWorkflow::class : WorkflowClaimAborted::class);
        }
        self::assertSame([[true, null], null], $this->replay($handler, $history));
    }

    public static function unprovedRows(): array
    {
        return array_map(static fn (string $mutation): array => [$mutation], ['terminal-path', 'terminal-scope',
            'terminal-local', 'foreign-wait', 'foreign-definition', 'foreign-occurrence', 'timer-resource',
            'timer-kind', 'timer-path', 'duplicate-timer', 'duplicate-terminal', 'coerced-duplicate', 'bad-time']);
    }

    public function testACompletedGroupCannotLeaveItsClockAvailableOutsideExecution(): void
    {
        $retained = null;
        $handler = static function (WorkflowContext $context) use (&$retained): ?string {
            $retained = $context;
            $context->all([$context->deferCondition(static fn (): bool => true, 'clock-condition', 5), $context->deferTimer(2)]);

            return self::read($context);
        };
        self::assertSame(self::FIRST, $this->replay($handler, self::history(self::handler())));
        $this->expectException(ReplayClockUnavailable::class);
        $retained->replayNow();
    }

    private static function handler(bool $conditionsOnly = false): callable
    {
        return static function (WorkflowContext $context) use ($conditionsOnly): array {
            $values = $context->all([$context->deferCondition(static fn (): bool => true, 'clock-condition', 5),
                $conditionsOnly ? $context->deferCondition(static fn (): bool => true, 'clock-condition', 5) : $context->deferTimer(2)]);

            return [$values, self::read($context)];
        };
    }

    private static function read(WorkflowContext $context): ?string
    {
        try { return $context->replayNow()->format(self::FORMAT); }
        catch (ReplayClockUnavailable) { return null; }
    }

    /** @param list<string> $times
     * @return list<array<string, mixed>>
     */
    private static function history(callable $handler, string $terminal = 'ConditionWaitSatisfied', bool $backed = false,
        array $times = [self::FIRST, self::SECOND]): array
    {
        $commands = (new Replayer(new AvroPayloadCodec()))->replay($handler, [], [], 'php-workers')->commands;
        $history = [];
        foreach ($commands as $offset => $command) {
            $sequence = $offset + 1;
            $metadata = array_intersect_key($command, array_flip(['parallel_group_id', 'parallel_group_kind', 'parallel_group_mode',
                'parallel_group_base_sequence', 'parallel_group_size', 'parallel_group_index', 'parallel_group_path']));
            $body = ['sequence' => $sequence];
            if ($command['type'] === 'open_condition_wait') {
                $body += ['condition_wait_id' => 'wait-'.$sequence, 'condition_key' => $command['condition_key'],
                    'condition_definition_fingerprint' => $command['condition_definition_fingerprint'], 'timeout_seconds' => 5];
                $history[] = ['event_type' => 'ConditionWaitOpened', 'payload' => $body + $metadata];
                if ($backed) {
                    // Core's timeout writer has wait identity but no parallel metadata.
                    $timer = $body + ['timer_id' => 'timer-'.$sequence, 'timer_kind' => 'condition_timeout', 'delay_seconds' => 5];
                    $history[] = ['event_type' => 'TimerScheduled', 'payload' => $timer];
                    $history[] = ['event_type' => $terminal === 'ConditionWaitTimedOut' ? 'TimerFired' : 'TimerCancelled',
                        'recorded_at' => $times[$offset], 'payload' => $timer];
                    $body['timer_id'] = $timer['timer_id'];
                }
                $history[] = ['event_type' => $terminal, 'recorded_at' => $times[$offset], 'payload' => $body + $metadata];
            } else {
                $body += ['timer_id' => 'timer-'.$sequence, 'delay_seconds' => 2];
                $history[] = ['event_type' => 'TimerScheduled', 'payload' => $body + $metadata];
                $history[] = ['event_type' => 'TimerFired', 'recorded_at' => $times[$offset], 'payload' => $body + $metadata];
            }
        }

        return $history;
    }

    /** @param list<array<string, mixed>> $history */
    private function replay(callable $handler, array $history): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', ['workflow_id' => 'parent', 'run_id' => 'run']);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
