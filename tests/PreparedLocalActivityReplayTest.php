<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityReplayTest extends TestCase
{
    public function test_admission_preserves_prefix_without_starting_application_code(): void
    {
        $codec = new AvroPayloadCodec();
        $localCalls = 0;
        $sideEffects = 0;
        $handler = static function (WorkflowContext $context) use (&$sideEffects): mixed {
            $value = $context->sideEffect(static function () use (&$sideEffects): string {
                ++$sideEffects;
                return 'prefix';
            });
            return $context->localActivity('local-write', [$value], ['retry_policy' => ['max_attempts' => 2]]);
        };
        $local = static function () use (&$localCalls): array {
            ++$localCalls;
            return ['outcome' => 'completed', 'result' => 'unsafe'];
        };
        $replayer = new Replayer($codec);
        $fresh = $replayer->replay($handler, [], [], 'prepared', localActivityExecutor: $local, prepareLocalActivities: true);
        self::assertSame(['record_side_effect'], array_column($fresh->commands, 'type'));
        self::assertSame(2, $fresh->preparedLocalActivity->sequence);
        self::assertFalse($fresh->preparedLocalActivity->recover);
        $descriptor = $fresh->preparedLocalActivity->descriptor($codec);
        self::assertSame(['prefix'], $codec->decodeEnvelope($descriptor['arguments']));
        self::assertSame('local', $descriptor['execution_mode']);
        self::assertSame(['max_attempts' => 2], $descriptor['retry_policy']);
        self::assertArrayNotHasKey('outcome', $descriptor);
        self::assertArrayNotHasKey('attempts', $descriptor);
        self::assertSame(0, $localCalls);
        self::assertSame(1, $sideEffects);

        $history = [['event_type' => 'SideEffectRecorded', 'payload' => [
            'sequence' => 1, 'result' => $codec->envelope('prefix'),
        ]]];
        $checkpointed = $replayer->replay($handler, $history, [], 'prepared', localActivityExecutor: $local, prepareLocalActivities: true);
        self::assertSame([], $checkpointed->commands);
        self::assertSame($descriptor, $checkpointed->preparedLocalActivity->descriptor($codec));
        self::assertSame(1, $sideEffects, 'Refreshing committed history repeated the prefix side effect.');

        $history[] = ['event_type' => 'ActivityScheduled', 'payload' => [
            'sequence' => 2, 'activity_type' => 'local-write', 'execution_mode' => 'local',
        ]];
        $history[] = ['event_type' => 'ActivityStarted', 'payload' => ['sequence' => 2]];
        $pending = $replayer->replay($handler, $history, [], 'prepared', localActivityExecutor: $local, prepareLocalActivities: true);
        self::assertSame([], $pending->commands);
        self::assertTrue($pending->preparedLocalActivity->recover);
        self::assertSame(2, $pending->preparedLocalActivity->sequence);
        self::assertSame(0, $localCalls, 'An unresolved prepared callback must be recovered, not invoked again.');

        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => [
            'sequence' => 2, 'result' => $codec->envelope('durable-result'),
        ]];
        $finished = $replayer->replay($handler, $history, [], 'prepared', localActivityExecutor: $local, prepareLocalActivities: true);
        self::assertNull($finished->preparedLocalActivity);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame('durable-result', $codec->decodeEnvelope($finished->commands[0]['result']));
        self::assertSame(0, $localCalls);
        self::assertSame(1, $sideEffects);
    }

    public function test_a_durable_retry_requests_new_admission_without_running_an_inline_retry(): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'local', 'execution_mode' => 'local']],
            ['event_type' => 'ActivityStarted', 'payload' => ['sequence' => 1]],
            ['event_type' => 'ActivityRetryScheduled', 'payload' => ['sequence' => 1]],
        ];
        $result = (new Replayer($codec))->replay(
            static fn (WorkflowContext $context): mixed => $context->localActivity('local'),
            $history, [], 'prepared', localActivityExecutor: static function (): never {
                self::fail('Replay cannot run an unadmitted retry.');
            }, prepareLocalActivities: true,
        );
        self::assertSame([], $result->commands);
        self::assertFalse($result->preparedLocalActivity->recover);
        self::assertSame(1, $result->preparedLocalActivity->sequence);
    }
}
