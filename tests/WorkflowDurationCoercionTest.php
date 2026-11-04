<?php

// Deliberately without declare(strict_types=1): before string durations were accepted, these
// numeric strings were coerced to int|float here. They must keep converting to the same seconds.

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowDurationCoercionTest extends TestCase
{
    /** @return iterable<string, array{string, int|float}> */
    public static function numericStrings(): iterable
    {
        yield 'integer' => ['30', 30];
        yield 'fraction' => ['1.5', 1.5];
        yield 'leading whitespace' => [' 30', 30];
        yield 'trailing whitespace' => ['30 ', 30];
        yield 'exponent' => ['1e3', 1e3];
        yield 'leading dot' => ['.5', .5];
        yield 'negative' => ['-5', -5];
        yield 'zero' => ['0', 0];
    }

    #[DataProvider('numericStrings')]
    public function testSleepAndDeferTimerKeepNumericStringCoercion(string $string, int|float $number): void
    {
        self::assertSame(
            self::commandsFor(static fn (WorkflowContext $context) => $context->sleep($number)),
            self::commandsFor(static fn (WorkflowContext $context) => $context->sleep($string)),
        );
        self::assertSame(
            self::commandsFor(static fn (WorkflowContext $context) => $context->all([$context->deferTimer($number)])),
            self::commandsFor(static fn (WorkflowContext $context) => $context->all([$context->deferTimer($string)])),
        );
    }

    #[DataProvider('numericStrings')]
    public function testConditionTimeoutsKeepNumericStringCoercion(string $string, int|float $number): void
    {
        $wait = static fn (int|float|string $timeout) => static fn (WorkflowContext $context): bool => $context->waitCondition(
            static fn (): bool => false,
            key: 'approval',
            timeout: $timeout,
        );

        self::assertSame(self::commandsFor($wait($number)), self::commandsFor($wait($string)));
    }

    public function testSleepAcceptsNumericStringsAsBefore(): void
    {
        self::assertSame(30, self::commandsFor(static fn (WorkflowContext $context) => $context->sleep('30'))[0]['delay_seconds']);
        self::assertSame(2, self::commandsFor(static fn (WorkflowContext $context) => $context->sleep('1.5'))[0]['delay_seconds']);
        self::assertSame(0, self::commandsFor(static fn (WorkflowContext $context) => $context->sleep('-5'))[0]['delay_seconds']);
        self::assertSame(60, self::commandsFor(static fn (WorkflowContext $context) => $context->waitCondition(
            static fn (): bool => false,
            'approval',
            '60',
        ))[0]['timeout_seconds']);
    }

    public function testRecordedNumericStringTimerStillReplays(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 30]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 30]],
        ];
        $result = (new Replayer($codec))->replay(static function (WorkflowContext $context): string {
            $context->sleep('30');

            return 'fired';
        }, $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('fired', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testRecordedNumericStringConditionTimeoutStillReplays(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'ConditionWaitOpened', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'condition:1',
                'condition_key' => 'approval', 'timeout_seconds' => 60]],
            ['event_type' => 'ConditionWaitTimedOut', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'condition:1',
                'condition_key' => 'approval', 'timeout_seconds' => 60]],
        ];
        $result = (new Replayer($codec))->replay(
            static fn (WorkflowContext $context): bool => $context->waitCondition(static fn (): bool => false, 'approval', '60'),
            $history,
            [],
            'php-workers',
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertFalse($codec->decodeEnvelope($result->commands[0]['result']));
    }

    /** Remote activity and child workflow options passed numeric strings through to the server. */
    public function testActivityAndChildOptionsPassNumericStringsThroughUnchanged(): void
    {
        foreach (['30', '1.5', '3600', ' 30'] as $value) {
            $activity = self::commandsFor(static fn (WorkflowContext $context) => $context->activity('send-email', [], [
                'start_to_close_timeout' => $value,
                'schedule_to_start_timeout' => $value,
                'schedule_to_close_timeout' => $value,
                'heartbeat_timeout' => $value,
            ]))[0];
            foreach (['start_to_close_timeout', 'schedule_to_start_timeout', 'schedule_to_close_timeout', 'heartbeat_timeout'] as $field) {
                self::assertSame($value, $activity[$field]);
            }

            $child = self::commandsFor(static fn (WorkflowContext $context) => $context->childWorkflow('child', [], [
                'execution_timeout_seconds' => $value,
                'run_timeout_seconds' => $value,
            ]))[0];
            self::assertSame($value, $child['execution_timeout_seconds']);
            self::assertSame($value, $child['run_timeout_seconds']);
        }
    }

    /** Local activity and service waits required integers before, and still reject numeric strings the same way. */
    public function testLocalActivityAndServiceOptionsKeepRejectingNumericStrings(): void
    {
        try {
            WorkflowCommand::canonicalLocalActivityOptions(['start_to_close_timeout' => '30']);
            self::fail('Expected the local activity timeout string to be rejected.');
        } catch (\InvalidArgumentException $error) {
            self::assertSame('Local activity start_to_close_timeout must be a positive integer when provided.', $error->getMessage());
        }

        $this->expectExceptionMessage('Service wait_timeout_seconds must be a non-negative integer.');
        WorkflowCommand::serviceOperation('billing', 'invoices', 'create', [], ['wait_timeout_seconds' => '30']);
    }

    /**
     * @param callable(WorkflowContext): mixed $workflow
     * @return list<array<string, mixed>>
     */
    private static function commandsFor(callable $workflow): array
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, [], [], 'php-workers')->commands;
    }
}
