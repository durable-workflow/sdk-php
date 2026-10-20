<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\CancellationPolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RemoteActivityOptionsTest extends TestCase
{
    public function testDocumentedRetryAndTimeoutReachTheScheduledActivity(): void
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay(
            static fn (WorkflowContext $context): mixed => $context->activity('payments.capture', [17, 2500], [
                'start_to_close_timeout' => 30,
                'retry_policy' => ['max_attempts' => 5, 'backoff_seconds' => [1, 2, 4, 8]],
            ]),
            [], [], 'payments',
        );

        self::assertCount(1, $result->commands);
        $command = $result->commands[0];
        self::assertSame('schedule_activity', $command['type']);
        self::assertSame('payments.capture', $command['activity_type']);
        self::assertSame(30, $command['start_to_close_timeout']);
        self::assertSame(['max_attempts' => 5, 'backoff_seconds' => [1, 2, 4, 8]], $command['retry_policy']);
        self::assertSame([17, 2500], $codec->decodeEnvelope($command['arguments']));
    }

    #[DataProvider('unsupportedOptions')]
    public function testUnknownOptionsFailBeforeAnActivityCanBeScheduled(array $options, string $field): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($field);

        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): mixed => $context->activity('payments.capture', [17], $options),
            [], [], 'payments',
        );
    }

    public static function unsupportedOptions(): iterable
    {
        yield 'timeout alias' => [['start_to_close_timeout_seconds' => 30], 'start_to_close_timeout_seconds'];
        yield 'unknown option' => [['unknown_option' => true], 'unknown_option'];
        yield 'attempt alias' => [['retry_policy' => ['maximum_attempts' => 5]], 'maximum_attempts'];
        yield 'initial interval alias' => [['retry_policy' => ['initial_interval_seconds' => 1]], 'initial_interval_seconds'];
        yield 'maximum interval alias' => [['retry_policy' => ['maximum_interval_seconds' => 20]], 'maximum_interval_seconds'];
        yield 'unknown nested field' => [['retry_policy' => ['max_attempts' => 5, 'unknown_retry' => 1]], 'unknown_retry'];
        yield 'invalid option with cancellation' => [['cancellation_policy' => CancellationPolicy::TryCancel, 'unknown_option' => 1], 'unknown_option'];
    }

    public function testOtherCanonicalRemoteOptionsArePreserved(): void
    {
        $options = [
            'connection' => 'durable', 'queue' => 'payments',
            'start_to_close_timeout' => 30, 'schedule_to_start_timeout' => 10,
            'schedule_to_close_timeout' => 120, 'heartbeat_timeout' => 5,
            'retry_policy' => ['max_attempts' => 5, 'backoff_seconds' => [1, 2, 4, 8],
                'non_retryable_error_types' => ['InvalidPayment']],
            'worker_session' => ['session_id' => 'payments-session'],
            'cancellation_policy' => CancellationPolicy::TryCancel,
        ];
        $expected = $options;
        $expected['cancellation_policy'] = 'try_cancel';

        self::assertSame($expected, WorkflowCommand::canonicalRemoteActivityOptions($options));
        self::assertSame([], WorkflowCommand::canonicalRemoteActivityOptions([]));
        self::assertSame(['retry_policy' => null], WorkflowCommand::canonicalRemoteActivityOptions(['retry_policy' => null]));
    }
}
