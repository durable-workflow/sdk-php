<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeReplayAdmissionTest extends TestCase
{
    #[DataProvider('unsupportedScopeHistory')]
    public function test_unqualified_worker_refuses_scoped_history_before_entering_workflow_code(array $event): void
    {
        $entered = false;
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): string {
                $entered = true;

                return 'must not run';
            }, [$event], [], 'php-workers');
            self::fail('An unqualified worker must not skip a scope boundary or silently execute scoped work as root.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('cancellation_scope_execution_not_supported', $error->getMessage());
            self::assertStringContainsString('PHP worker', $error->getMessage());
            self::assertFalse($entered);
        }
    }

    public static function unsupportedScopeHistory(): array
    {
        $cases = [];
        foreach (['CancellationScopeOpened', 'CancellationScopeRequested', 'CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered', 'CancellationScopeRequestConflicted'] as $kind) {
            $cases[$kind] = [['event_type' => $kind, 'payload' => ['scope_id' => 'canonical-scope']]];
        }
        foreach (['ActivityScheduled', 'TimerScheduled', 'ChildWorkflowScheduled', 'ActivityStarted'] as $kind) {
            $cases[$kind] = [['event_type' => $kind, 'payload' => ['sequence' => 1, 'cancellation_scope_id' => 'canonical-scope']]];
        }
        foreach (['activity', 'timer', 'child_workflow'] as $snapshot) {
            $cases['nested '.$snapshot] = [['event_type' => 'ActivityCompleted', 'payload' => [
                'sequence' => 1, $snapshot => ['cancellation_scope_id' => 'canonical-scope'],
            ]]];
        }
        foreach ([null, '', false, 1, ['scope']] as $index => $invalid) {
            $cases['malformed '.$index] = [['event_type' => 'ActivityScheduled', 'payload' => ['cancellation_scope_id' => $invalid]]];
        }

        return $cases;
    }

    public function test_historical_absence_and_explicit_root_membership_replay_normally(): void
    {
        $codec = new AvroPayloadCodec();
        foreach ([[], ['cancellation_scope_id' => 'root']] as $membership) {
            $calls = 0;
            $result = (new Replayer($codec))->replay(static function (WorkflowContext $context) use (&$calls): mixed {
                ++$calls;

                return $context->activity('root-activity');
            }, [['event_type' => 'ActivityCompleted', 'payload' => [
                ...$membership, 'sequence' => 1, 'activity_type' => 'root-activity',
                'activity' => $membership, 'result' => $codec->envelope('root result'),
            ]]], [], 'php-workers');
            self::assertSame(1, $calls);
            self::assertSame('complete_workflow', $result->commands[0]['type']);
            self::assertSame('root result', $codec->decodeEnvelope($result->commands[0]['result']));
        }
    }

    public function test_application_payloads_named_scope_do_not_claim_runtime_membership(): void
    {
        $codec = new AvroPayloadCodec();
        $value = ['cancellation_scope_id' => 'application-value', 'activity' => ['cancellation_scope_id' => 'application-value']];
        $history = [['event_type' => 'SideEffectRecorded', 'payload' => ['sequence' => 1, 'result' => $codec->envelope($value)]]];
        $result = (new Replayer($codec))->replay(static fn (WorkflowContext $context): mixed =>
            $context->sideEffect(static function (): never {
                throw new \LogicException('Recorded application value must replay.');
            }),
            $history, [], 'php-workers');
        self::assertSame($value, $codec->decodeEnvelope($result->commands[0]['result']));
    }
}
