<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ConditionWaitDefinition;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConditionWaitConsumedHistoryTest extends TestCase
{
    #[DataProvider('histories')]
    public function testRecordedOpenAfterConsumedResultKeepsItsOwnBudget(
        string $variant,
        int $secondTimeout,
        int $expectedTimeout,
    ): void {
        $codec = new AvroPayloadCodec();
        $predicate = null;
        $workflow = static function (WorkflowContext $context) use (&$predicate, $secondTimeout): bool {
            $signal = 'first';
            $predicate = static function () use ($context, &$signal): bool {
                return $context->signals($signal) !== [];
            };
            $context->waitCondition($predicate, key: 'approval', timeout: 30);
            $signal = 'second';

            return $context->waitCondition($predicate, key: 'approval', timeout: $secondTimeout);
        };
        (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
        self::assertInstanceOf(Closure::class, $predicate);
        $fingerprint = ConditionWaitDefinition::legacyFingerprint($predicate);
        self::assertNotNull($fingerprint);
        $event = static fn (string $type, int $seconds, array $payload): array => [
            'event_type' => $type,
            'recorded_at' => sprintf('2026-11-04T00:00:%02d.000000+00:00', $seconds),
            'payload' => $payload,
        ];
        $open = static fn (int $sequence, int $seconds, int $timeout, ?int $occurrence = null): array =>
            $event('ConditionWaitOpened', $seconds, array_filter([
                'sequence' => $sequence, 'condition_wait_id' => 'wait-'.$sequence,
                'condition_key' => 'approval', 'condition_definition_fingerprint' => $fingerprint,
                'timeout_seconds' => $timeout,
                'condition_wait_occurrence_id' => $occurrence === null ? null : 'php:condition-wait:'.$occurrence,
            ], static fn (mixed $value): bool => $value !== null));
        $satisfied = static fn (int $sequence, int $seconds): array => $event('ConditionWaitSatisfied', $seconds, [
            'sequence' => $sequence, 'condition_wait_id' => 'wait-'.$sequence, 'condition_key' => 'approval',
        ]);
        $canonical = $variant === 'canonical';
        $secondSequence = $variant === 'duplicate-before-satisfaction' ? 3 : 2;
        $history = [$open(1, 0, 30, $canonical ? 0 : null)];
        if ($variant === 'duplicate-before-satisfaction') {
            $history[] = $open(2, 8, 22);
        }
        if ($variant !== 'false-wake') {
            $history[] = $event('SignalReceived', 10, [
                'workflow_sequence' => 1, 'signal_name' => 'first', 'arguments' => $codec->envelope([]),
            ]);
        }
        $history[] = $satisfied(1, 10);
        $history[] = $open($secondSequence, 12, $secondTimeout,
            $canonical || $variant === 'mixed-next-occurrence' ? 1 : ($variant === 'mixed-same-occurrence' ? 0 : null));
        if ($variant === 'duplicate-second-open') {
            $history[] = $open(3, 18, 14);
        } elseif (str_starts_with($variant, 'second-false-wake')) {
            $history[] = $satisfied(2, 16);
            $history[] = $open(3, 18, $variant === 'second-false-wake-full' ? 20 : 14);
        }
        $history[] = $event('SignalReceived', 22, [
            'workflow_sequence' => $secondSequence, 'signal_name' => 'unrelated', 'arguments' => $codec->envelope([]),
        ]);

        $commands = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers')->commands;

        self::assertCount(1, $commands);
        self::assertSame('open_condition_wait', $commands[0]['type']);
        self::assertSame($expectedTimeout, $commands[0]['timeout_seconds']);
    }

    /** @return array<string, array{string, int, int}> */
    public static function histories(): array
    {
        return [
            'legacy authored B has its own deadline' => ['legacy', 20, 10],
            'equal authored timeouts are separate budgets' => ['legacy', 30, 20],
            'canonical zero/one stay separate' => ['canonical', 20, 10],
            'legacy first open then canonical next occurrence' => ['mixed-next-occurrence', 20, 10],
            'canonical tail still belongs to the consumed occurrence' => ['mixed-same-occurrence', 20, 20],
            'duplicate A before its satisfaction stays A' => ['duplicate-before-satisfaction', 20, 10],
            'duplicate B stays on Bs own first open' => ['duplicate-second-open', 20, 10],
            'B false wake with old full timeout' => ['second-false-wake-full', 20, 10],
            'B false wake with remaining timeout' => ['second-false-wake-remaining', 20, 10],
            'false predicate does not consume As terminal' => ['false-wake', 20, 8],
        ];
    }

    #[DataProvider('ambiguousHistories')]
    public function testAmbiguousLegacyIdentityIsAnExplicitQualificationConflict(string $variant): void
    {
        $fixture = json_decode((string) file_get_contents(
            __DIR__.'/fixtures/replay-regressions/condition-legacy-consumed-terminal-starts-new-budget.json',
        ), true, flags: JSON_THROW_ON_ERROR);
        switch ($variant) {
            case 'first-open-id-missing':
                unset($fixture['history'][0]['payload']['condition_wait_id']);
                break;
            case 'second-open-id-missing':
                unset($fixture['history'][3]['payload']['condition_wait_id']);
                break;
            case 'reused-id':
                $fixture['history'][3]['payload']['condition_wait_id'] = 'wait-1';
                break;
            case 'terminal-id-missing':
                unset($fixture['history'][2]['payload']['condition_wait_id']);
                break;
            case 'terminal-id-foreign':
                $fixture['history'][2]['payload']['condition_wait_id'] = 'another-wait';
                break;
            case 'terminal-sequence-foreign':
                $fixture['history'][2]['payload']['sequence'] = 99;
                break;
            case 'second-open-sequence-missing':
                unset($fixture['history'][3]['payload']['sequence']);
                break;
            case 'already-timed-out-tail':
                $fixture['history'][] = ['event_type' => 'ConditionWaitTimedOut',
                    'recorded_at' => '2026-11-04T00:00:32.000000+00:00',
                    'payload' => ['sequence' => 2, 'condition_wait_id' => 'wait-2', 'condition_key' => 'approval']];
                break;
        }
        $fixture['expected'] = ['type' => 'replay_error', 'reason' => 'legacy_condition_call_boundary_ambiguous'];

        $commands = ReplayRegressionFixture::execute($fixture);

        self::assertSame('legacy_condition_call_boundary_ambiguous', $commands[0]['reason']);
    }

    /** @return array<string, array{string}> */
    public static function ambiguousHistories(): array
    {
        return array_combine(
            $variants = ['first-open-id-missing', 'second-open-id-missing', 'reused-id', 'terminal-id-missing',
                'terminal-id-foreign', 'terminal-sequence-foreign', 'second-open-sequence-missing', 'already-timed-out-tail'],
            array_map(static fn (string $variant): array => [$variant], $variants),
        );
    }

    public function testEachConsumedCallCanReleaseTheNextRecordedLegacyOpen(): void
    {
        $codec = new AvroPayloadCodec();
        $predicate = null;
        $workflow = static function (WorkflowContext $context) use (&$predicate): bool {
            $signal = 'first';
            $predicate = static function () use ($context, &$signal): bool {
                return $context->signals($signal) !== [];
            };
            $context->waitCondition($predicate, key: 'approval', timeout: 30);
            $signal = 'second';
            $context->waitCondition($predicate, key: 'approval', timeout: 20);
            $signal = 'third';

            return $context->waitCondition($predicate, key: 'approval', timeout: 10);
        };
        (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
        self::assertInstanceOf(Closure::class, $predicate);
        $fingerprint = ConditionWaitDefinition::legacyFingerprint($predicate);
        $fixture = json_decode((string) file_get_contents(
            __DIR__.'/fixtures/replay-regressions/condition-legacy-consumed-terminal-starts-new-budget.json',
        ), true, flags: JSON_THROW_ON_ERROR);
        $history = array_slice($fixture['history'], 0, 4);
        foreach ($history as &$event) {
            if ($event['event_type'] === 'ConditionWaitOpened') {
                $event['payload']['condition_definition_fingerprint'] = $fingerprint;
            }
        }
        unset($event);
        $history[] = ['event_type' => 'SignalReceived', 'recorded_at' => '2026-11-04T00:00:18.000000+00:00',
            'payload' => ['workflow_sequence' => 2, 'signal_name' => 'second', 'arguments' => $codec->envelope([])]];
        $history[] = ['event_type' => 'ConditionWaitSatisfied', 'recorded_at' => '2026-11-04T00:00:18.000000+00:00',
            'payload' => ['sequence' => 2, 'condition_wait_id' => 'wait-2', 'condition_key' => 'approval']];
        $history[] = ['event_type' => 'ConditionWaitOpened', 'recorded_at' => '2026-11-04T00:00:24.000000+00:00',
            'payload' => ['sequence' => 3, 'condition_wait_id' => 'wait-3', 'condition_key' => 'approval',
                'condition_definition_fingerprint' => $fingerprint, 'timeout_seconds' => 10]];
        $history[] = ['event_type' => 'SignalReceived', 'recorded_at' => '2026-11-04T00:00:30.000000+00:00',
            'payload' => ['workflow_sequence' => 3, 'signal_name' => 'unrelated', 'arguments' => $codec->envelope([])]];

        $commands = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers')->commands;

        self::assertSame('open_condition_wait', $commands[0]['type']);
        self::assertSame(4, $commands[0]['timeout_seconds']);
    }
}
