<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\ConditionWaitDefinition;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConditionBusinessReplayClockTest extends TestCase
{
    private const TIME = '2026-11-06T04:00:05.123456+00:00';

    #[DataProvider('resolutions')]
    public function testTheConsumedConditionKeepsItsResultAndExactTime(string $terminal, bool $backed): void
    {
        $predicate = self::predicate();
        $handler = self::handler($predicate);
        $history = self::history($predicate, $terminal, $backed);
        $expected = [$terminal === 'ConditionWaitSatisfied', self::TIME];

        self::assertSame($expected, $this->replay($handler, $history));
        self::assertSame($expected, $this->replay($handler, $history));
    }

    public static function resolutions(): array
    {
        return [['ConditionWaitSatisfied', false], ['ConditionWaitSatisfied', true],
            ['ConditionWaitTimedOut', false], ['ConditionWaitTimedOut', true]];
    }

    public function testSignalReceiptWakesThePredicateButDoesNotSupplyItsTime(): void
    {
        $current = null;
        $predicate = static function () use (&$current): bool { return $current->signals('approve') !== []; };
        $history = self::history($predicate, 'ConditionWaitSatisfied', false);
        array_splice($history, 1, 0, [[
            'event_type' => 'SignalReceived', 'recorded_at' => '2099-01-01T00:00:00Z',
            'payload' => ['workflow_sequence' => 1, 'signal_name' => 'approve',
                'arguments' => (new AvroPayloadCodec())->envelope(['approval'])],
        ]]);
        $handler = static function (WorkflowContext $context) use (&$current, $predicate): array {
            $current = $context;
            $result = $context->waitCondition($predicate, key: 'clock-condition', timeout: 5);

            return [$result, $context->signals('approve'), $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
        };

        self::assertSame([true, [['approval']], self::TIME], $this->replay($handler, $history));
    }

    public function testAnImmediatePredicateAndItsCallbackCannotInventClockAuthority(): void
    {
        $observed = $this->replay(static function (WorkflowContext $context): array {
            $predicateUnavailable = false;
            $result = $context->waitCondition(static function () use ($context, &$predicateUnavailable): bool {
                try { $context->replayNow(); } catch (ReplayClockUnavailable) { $predicateUnavailable = true; }

                return true;
            });
            try { $context->replayNow(); } catch (ReplayClockUnavailable) {
                return [$result, $predicateUnavailable, 'unavailable'];
            }
            self::fail('An unrecorded predicate cannot supply time.');
        }, []);

        self::assertSame([true, true, 'unavailable'], $observed);
    }

    public function testAConditionTimeoutCannotInheritPlainTimerRedriveAuthority(): void
    {
        $fixture = json_decode((string) file_get_contents(
            __DIR__.'/fixtures/replay-regressions/business-replay-clock-canonical-redrive.json',
        ), true, flags: JSON_THROW_ON_ERROR);
        $start = $fixture['history'][0];
        $predicate = self::predicate();
        $history = self::history($predicate, 'ConditionWaitTimedOut', true);
        foreach ($history as &$row) { $row['recorded_at'] = $start['recorded_at']; }
        unset($row);
        $history[2]['payload']['reused_from_run_id'] = $start['payload']['continued_from_run_id'];
        $history[2]['payload']['reused_recorded_at'] = $start['payload']['replayed_started_at'];
        array_unshift($history, $start);
        foreach ($history as $index => &$row) { $row['sequence'] = $index + 1; }
        unset($row);

        $this->expectException(ReplayClockUnavailable::class);
        $this->replay(static function (WorkflowContext $context) use ($predicate): string {
            self::assertFalse($context->waitCondition($predicate, key: 'clock-condition', timeout: 5));

            return $context->replayNow()->format('c');
        }, $history, $fixture['workflow_tasks'][0]);
    }

    #[DataProvider('unprovedHistories')]
    public function testAmbiguousConditionEvidencePreservesTheResultAndRefusesTime(string $mutation): void
    {
        $predicate = self::predicate();
        $backed = str_starts_with($mutation, 'timer-');
        $terminal = $mutation === 'timer-conflicting-time' ? 'ConditionWaitTimedOut' : 'ConditionWaitSatisfied';
        $history = self::history($predicate, $terminal, $backed);
        $last = array_key_last($history);
        switch ($mutation) {
            case 'missing-id': unset($history[$last]['payload']['condition_wait_id']); break;
            case 'foreign-id': $history[$last]['payload']['condition_wait_id'] = 'foreign'; break;
            case 'foreign-key': $history[$last]['payload']['condition_key'] = 'foreign'; break;
            case 'foreign-definition': $history[$last]['payload']['condition_definition_fingerprint'] = 'foreign'; break;
            case 'wrong-occurrence': $history[$last]['payload']['condition_wait_occurrence_id'] = 'other'; break;
            case 'duplicate-terminal': $history[] = $history[$last]; break;
            case 'conflicting-terminal': $history[] = [...$history[$last], 'event_type' => 'ConditionWaitTimedOut']; break;
            case 'coerced-duplicate':
                $duplicate = $history[$last]; $duplicate['payload']['sequence'] = '1'; $history[] = $duplicate; break;
            case 'unknown-row': $history[] = [...$history[$last], 'event_type' => 'ConditionWaitFuture']; break;
            case 'bad-time': $history[$last]['recorded_at'] = '2026-02-30T00:00:00Z'; break;
            case 'timer-foreign-resource': $history[2]['payload']['timer_id'] = 'foreign'; break;
            case 'timer-wrong-kind': $history[2]['payload']['timer_kind'] = 'signal_timeout'; break;
            case 'timer-foreign-definition': $history[2]['payload']['condition_definition_fingerprint'] = 'foreign'; break;
            case 'timer-duplicate': array_splice($history, 2, 0, [$history[1]]); break;
            case 'timer-conflicting-time': $history[2]['recorded_at'] = '2026-11-06T04:00:04Z'; break;
        }

        $this->expectException(ReplayClockUnavailable::class);
        $this->replay(static function (WorkflowContext $context) use ($predicate, $terminal): string {
            self::assertSame($terminal === 'ConditionWaitSatisfied', $context->waitCondition($predicate, key: 'clock-condition', timeout: 5));

            return $context->replayNow()->format('c');
        }, $history);
    }

    public static function unprovedHistories(): array
    {
        return array_map(static fn (string $mutation): array => [$mutation], [
            'missing-id', 'foreign-id', 'foreign-key', 'foreign-definition', 'wrong-occurrence',
            'duplicate-terminal', 'conflicting-terminal', 'coerced-duplicate', 'unknown-row', 'bad-time',
            'timer-foreign-resource', 'timer-wrong-kind', 'timer-foreign-definition', 'timer-duplicate', 'timer-conflicting-time',
        ]);
    }

    private static function predicate(): Closure
    {
        return static fn (): bool => true;
    }

    private static function handler(Closure $predicate): Closure
    {
        return static function (WorkflowContext $context) use ($predicate): array {
            $result = $context->waitCondition($predicate, key: 'clock-condition', timeout: 5);

            return [$result, $context->replayNow()->format('Y-m-d\TH:i:s.uP')];
        };
    }

    /** @return list<array<string, mixed>> */
    private static function history(Closure $predicate, string $terminal, bool $backed): array
    {
        $body = ['sequence' => 1, 'condition_wait_id' => 'wait', 'condition_key' => 'clock-condition',
            'condition_definition_fingerprint' => ConditionWaitDefinition::fingerprint($predicate),
            'condition_wait_occurrence_id' => 'php:condition-wait:0', 'timeout_seconds' => 5];
        $history = [['event_type' => 'ConditionWaitOpened', 'recorded_at' => '2026-11-06T04:00:00Z', 'payload' => $body]];
        if ($backed) {
            $timer = [...$body, 'timer_id' => 'timer', 'timer_kind' => 'condition_timeout', 'delay_seconds' => 5];
            $history[] = ['event_type' => 'TimerScheduled', 'recorded_at' => '2026-11-06T04:00:00Z', 'payload' => $timer];
            $history[] = ['event_type' => $terminal === 'ConditionWaitSatisfied' ? 'TimerCancelled' : 'TimerFired',
                'recorded_at' => self::TIME, 'payload' => $timer];
            $body['timer_id'] = 'timer';
        }
        $history[] = ['event_type' => $terminal, 'recorded_at' => self::TIME, 'payload' => $body];

        return $history;
    }

    /** @param list<array<string, mixed>> $history */
    private function replay(callable $handler, array $history, array $task = ['workflow_id' => 'parent', 'run_id' => 'run']): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', $task);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
