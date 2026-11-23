<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class ConditionWaitOrderedInputTest extends TestCase
{
    public function testFirstAppliedPrefixSurvivesColdReplayAfterTheOriginalTimer(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): bool {
            return $context->waitCondition(static function () use ($context): bool {
                $signals = $context->signals('approval');

                return $signals !== [] && end($signals)[0] === true;
            }, key: 'approval', timeout: 2);
        };
        $worker = new Replayer($codec);
        $worker->useConditionOccurrenceDeadline(true);
        $legacyOpening = $worker->replay($workflow, [], [], 'test')->commands[0];
        self::assertArrayNotHasKey('condition_wait_input_order', $legacyOpening);
        $opening = $worker->replay($workflow, [], [], 'test', ['condition_wait_input_order_authoring' => true])->commands[0];
        self::assertSame('open_condition_wait', $opening['type']);
        self::assertSame('durable_receipt_order', $opening['condition_wait_input_order']);
        unset($opening['type']);
        $opening += ['sequence' => 1, 'condition_wait_id' => 'approval-wait'];
        $timer = $opening + ['timer_id' => 'original-timer', 'timer_kind' => 'condition_timeout', 'delay_seconds' => 2];
        $event = static fn (int $sequence, string $type, array $payload): array => [
            'sequence' => $sequence, 'event_type' => $type, 'payload' => $payload,
            'recorded_at' => '2026-11-23T22:00:00Z',
        ];
        $approval = ['workflow_command_id' => 'approve', 'signal_name' => 'approval'];
        $withdrawal = ['workflow_command_id' => 'withdraw', 'signal_name' => 'approval'];
        $history = [
            $event(3, 'ConditionWaitOpened', $opening),
            $event(4, 'TimerScheduled', $timer),
            $event(5, 'SignalReceived', $approval + ['arguments' => ['codec' => 'avro', 'blob' => $codec->encode([true])]]),
            $event(6, 'SignalReceived', $withdrawal + ['arguments' => ['codec' => 'avro', 'blob' => $codec->encode([false])]]),
            $event(7, 'TimerFired', $timer),
        ];
        $pending = $worker->replay($workflow, $history, [], 'test', ['condition_wait_pending_input_sequence' => 5]);
        self::assertSame([], $pending->commands);

        $history[] = $event(8, 'SignalApplied', $approval);
        $history[] = $event(9, 'SignalApplied', $withdrawal);
        $resolved = $worker->replay($workflow, $history, [], 'test', ['condition_wait_pending_input_sequence' => null]);
        self::assertSame([
            'type' => 'resolve_condition_wait', 'condition_wait_id' => 'approval-wait', 'satisfied' => true,
            'input_receipt_sequence' => 5,
        ], $resolved->commands[0]);
        self::assertTrue($codec->decodeEnvelope($resolved->commands[1]['result']));

        $history[] = $event(10, 'ConditionWaitSatisfied', $opening + [
            'input_receipt_sequence' => 5, 'timer_fired_sequence' => 7, 'timer_id' => 'original-timer',
        ]);
        $replayed = (new Replayer($codec))->replay($workflow, $history, [], 'test');
        self::assertSame(['complete_workflow'], array_column($replayed->commands, 'type'));
        self::assertTrue($codec->decodeEnvelope($replayed->commands[0]['result']));
    }
}
