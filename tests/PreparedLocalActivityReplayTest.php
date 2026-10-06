<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityReplayTest extends TestCase
{
    public function test_parallel_local_calls_refuse_before_application_code_until_atomic_admission_exists(): void
    {
        $this->expectException(\DurableWorkflow\Worker\WorkflowClaimAborted::class);
        $this->expectExceptionMessage('prepared_local_parallel_admission_unavailable');
        (new Replayer(new AvroPayloadCodec()))->replay(static fn (WorkflowContext $context) => $context->parallel([
            static fn () => $context->childWorkflow('python.child'),
            static fn () => $context->localActivity('php.local'),
        ]), [], [], 'prepared', localActivityExecutor: static function (): never {
            self::fail('Parallel callbacks must not fall through to legacy inline execution.');
        }, prepareLocalActivities: true);
    }

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

    public function test_shielded_cleanup_cold_replay_keeps_the_canonical_delivery_and_root_budget(): void
    {
        $codec = new AvroPayloadCodec();
        $history = self::cleanupHistory();
        $calls = 0;
        $replayer = new Replayer($codec);
        $local = static function () use (&$calls): array { ++$calls; return ['outcome' => 'completed', 'result' => 'unsafe']; };
        $cold = $replayer->replay(self::cleanupWorkflow(), $history, [], 'prepared', self::task(), $local, true);
        $fresh = $replayer->replay(self::cleanupWorkflow(), array_slice($history, 0, 2), [], 'prepared', self::task(), $local, true);
        self::assertSame(0, $calls);
        self::assertTrue($cold->preparedLocalActivity->recover);
        self::assertFalse($fresh->preparedLocalActivity->recover);
        self::assertSame(2, $cold->preparedLocalActivity->sequence);
        self::assertSame($fresh->preparedLocalActivity->descriptor($codec), $cold->preparedLocalActivity->descriptor($codec));
        self::assertArrayHasKey('cancellation_cleanup', $cold->preparedLocalActivity->descriptor($codec));
        self::assertSame(['request_id' => 'child-request', 'delivery_history_event_id' => 'canonical-delivery'],
            $cold->preparedLocalActivity->descriptor($codec)['cancellation_cleanup']);
        self::assertSame(['request_id' => 'child-request', 'root_request_id' => 'root-request',
            'delivery_history_event_id' => 'canonical-delivery', 'cleanup_deadline_at' => '2026-10-02T00:00:30.000000Z'],
            $cold->preparedLocalActivity->cleanupSnapshot());
        self::assertArrayNotHasKey('cleanup_deadline_at', $cold->preparedLocalActivity->descriptor($codec)['cancellation_cleanup']);
    }

    public function test_prepared_candidate_fixture_executes_the_actual_replayer_contract(): void
    {
        $commands = ReplayRegressionFixture::executeFile(__DIR__.'/fixtures/prepared-local/cleanup-cold-replay.json');
        self::assertSame('prepare_local_activity', $commands[0]['type']);
        self::assertSame('root-request', $commands[0]['cancellation_cleanup']['root_request_id']);
    }

    public function test_cleanup_prefix_is_replayed_once_before_callback_admission(): void
    {
        $codec = new AvroPayloadCodec();
        $history = array_slice(self::cleanupHistory(), 0, 2);
        $prefixCalls = 0;
        $workflow = static function (WorkflowContext $context) use (&$prefixCalls): void {
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static function () use ($context, &$prefixCalls): void {
                    $context->sideEffect(static function () use (&$prefixCalls): string { ++$prefixCalls; return 'once'; });
                    $context->localActivity('golden.cleanup');
                });
                throw $cancelled;
            }
        };
        $replayer = new Replayer($codec);
        $local = static function (): never { self::fail('Application code ran without prepared admission.'); };
        $first = $replayer->replay($workflow, $history, [], 'prepared', self::task(), $local, true);
        self::assertSame(3, $first->preparedLocalActivity->sequence);
        self::assertSame(['record_side_effect'], array_column($first->commands, 'type'));
        $history[] = ['event_type' => 'SideEffectRecorded', 'payload' => ['sequence' => 2, 'result' => $codec->envelope('once')]];
        $next = $replayer->replay($workflow, $history, [], 'prepared', self::task(), $local, true);
        self::assertSame([], $next->commands);
        self::assertSame($first->preparedLocalActivity->cleanupSnapshot(), $next->preparedLocalActivity->cleanupSnapshot());
        self::assertSame(1, $prefixCalls);
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 3,
            'activity_type' => 'golden.cleanup', 'execution_mode' => 'local', 'result' => $codec->envelope('cleaned')]];
        try {
            $replayer->replay($workflow, $history, [], 'prepared', self::task(), $local, true);
            self::fail('Cleanup completion lost the original cancellation outcome.');
        } catch (WorkflowCancelled $cancelled) {
            self::assertSame('child-request', $cancelled->requestId);
            self::assertSame('root-request', $cancelled->context->rootRequestId);
            self::assertSame(1, $prefixCalls);
        }
    }

    public function test_observation_without_delivery_cannot_admit_shielded_local_cleanup(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->cancellationShield(static fn () => $context->localActivity('golden.cleanup')),
            [self::cleanupHistory()[0]], [], 'prepared', self::task(), static fn () => [], true,
        );
    }

    public function test_local_call_after_delivery_requires_explicit_shielding(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context): void {
            try { $context->sleep(10); } catch (WorkflowCancelled) { $context->localActivity('golden.cleanup'); }
        }, self::cleanupHistory(), [], 'prepared', self::task(), static fn () => [], true);
    }

    public function test_missing_canonical_delivery_identity_cannot_authorize_cleanup(): void
    {
        $history = self::cleanupHistory();
        unset($history[1]['id']);
        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay(self::cleanupWorkflow(), $history, [], 'prepared', self::task(), static fn () => [], true);
    }

    public function test_legacy_delivery_without_rich_context_cannot_invent_prepared_cleanup_authority(): void
    {
        $history = self::cleanupHistory();
        unset($history[0]['payload']['cancellation'], $history[1]['payload']['cancellation']);
        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay(self::cleanupWorkflow(), $history, [], 'prepared', self::task(), static fn () => [], true);
    }

    private static function cleanupHistory(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/prepared-local/cleanup-cold-replay.json'),
            true, flags: JSON_THROW_ON_ERROR)['history'];
    }

    private static function task(): array
    {
        return ['run_id' => 'regression-inline', 'workflow_id' => 'regression-workflow'];
    }

    private static function cleanupWorkflow(): \Closure
    {
        return static function (WorkflowContext $context): void {
            try { $context->sleep(10); } catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->localActivity('golden.cleanup'));
                throw $cancelled;
            }
        };
    }
}
