<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\DurableOperationCancelled;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationRemainingTimeTest extends TestCase
{
    public function testDetachedSnapshotRefusesTheHostClock(): void
    {
        $this->expectException(LogicException::class);
        CancellationContext::fromArray(self::snapshot())->remaining();
    }

    public function testFirstExecutionAndColdReplayKeepEarlierDecisionsAndIgnoreFutureHistory(): void
    {
        $codec = new AvroPayloadCodec();
        $seen = [];
        $snapshot = null;
        $effects = 0;
        $workflow = static function (WorkflowContext $workflow) use (&$seen, &$snapshot, &$effects): array {
            try {
                $workflow->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                $snapshot = $cancelled->context;
                return $workflow->cancellationShield(static function () use ($workflow, $snapshot, &$seen, &$effects): array {
                    $seen[] = $snapshot->remaining();
                    $workflow->sideEffect(static function () use (&$effects): bool { ++$effects; return true; });
                    $seen[] = $snapshot->remaining();
                    $workflow->activity('cleanup');
                    $seen[] = $snapshot->remaining();
                    return $seen;
                });
            }
            return [];
        };
        $history = self::history();
        $first = self::replay($workflow, $history);
        self::assertCount(2, $first->commands);
        self::assertSame([22.123456, 22.123456], $seen);
        self::assertSame(1, $effects);
        self::assertEquals(self::snapshot(), $snapshot->toArray());
        $history[] = ['event_type' => 'SideEffectRecorded', 'timestamp' => '2026-10-01T00:00:12Z',
            'payload' => ['sequence' => 2, 'result' => $codec->envelope(true)]];
        $history[] = self::completed(3, 'cleanup', '2026-10-01T00:00:25Z');
        $history[] = ['event_type' => 'SignalReceived', 'timestamp' => '2026-10-01T00:00:29Z',
            'payload' => ['signal_name' => 'later', 'arguments' => $codec->envelope([])]];
        $seen = [];
        $cold = self::replay($workflow, $history);
        self::assertSame([22.123456, 22.123456, 5.123456], $seen);
        self::assertSame($seen, $codec->decodeEnvelope($cold->commands[0]['result']));
        self::assertSame(1, $effects, 'The synchronous side effect is not rerun.');
        self::assertEquals(self::snapshot(), $snapshot->toArray());
        $this->expectException(LogicException::class);
        $snapshot->remaining();
    }

    public function testTimerResumeUsesItsCommittedTimeAndPreservesMicroseconds(): void
    {
        $workflow = static function (WorkflowContext $workflow): array {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): array {
                    $before = $cancelled->context->remaining();
                    $workflow->sleep(1);
                    return [$before, $cancelled->context->remaining()];
                });
            }
            return [];
        };
        $history = self::history();
        $history[] = ['event_type' => 'TimerFired', 'timestamp' => '2026-09-30T20:00:09.654321-04:00',
            'payload' => ['sequence' => 2, 'seconds' => 1]];
        $result = self::replay($workflow, $history);
        self::assertSame([22.123456, 20.469135], (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testParallelCompletionUsesConsumedMembersAndDoesNotRegressOnClockSkew(): void
    {
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): float {
                    $workflow->all([$workflow->deferActivity('first'), $workflow->deferActivity('second')]);
                    return $cancelled->context->remaining();
                });
            }
            return -1;
        };
        $history = self::history();
        $pending = self::replay($workflow, $history);
        $history[] = self::completed(2, 'first', '2026-10-01T00:00:20Z', $pending->commands[0]);
        $history[] = self::completed(3, 'second', '2026-10-01T00:00:15Z', $pending->commands[1]);
        $result = self::replay($workflow, $history);
        self::assertSame(10.123456, (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testParallelFailureDoesNotConsumeALaterSiblingResolution(): void
    {
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): float {
                    try { $workflow->all([$workflow->deferActivity('first'), $workflow->deferActivity('second')]); }
                    catch (ActivityFailed) { return $cancelled->context->remaining(); }
                    return -1;
                });
            }
            return -1;
        };
        $history = self::history();
        $pending = self::replay($workflow, $history);
        $history[] = ['event_type' => 'ActivityFailed', 'timestamp' => '2026-10-01T00:00:12Z',
            'payload' => [...$pending->commands[0], 'sequence' => 2, 'activity_type' => 'first',
                'message' => 'cleanup failure', 'exception_class' => 'CleanupFailure']];
        $history[] = self::completed(3, 'second', '2026-10-01T00:00:25Z', $pending->commands[1]);
        $result = self::replay($workflow, $history);
        self::assertSame(18.123456, (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testExpiredRecordedBudgetClampsToZero(): void
    {
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): float {
                    try { $workflow->activity('cleanup'); } catch (ActivityFailed) { }
                    return $cancelled->context->remaining();
                });
            }
            return -1;
        };
        $history = self::history();
        $history[] = ['event_type' => 'ActivityTimedOut', 'timestamp' => '2026-10-01T00:00:31Z',
            'payload' => ['sequence' => 2, 'activity_type' => 'cleanup', 'message' => 'cleanup timeout']];
        $result = self::replay($workflow, $history);
        self::assertSame(0.0, (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testSelectionUsesItsMarkerThenTheAwaitedHandleWithoutReadingLaterLosers(): void
    {
        $workflow = static function (WorkflowContext $workflow): array {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): array {
                    $selected = $workflow->select([
                        'slow' => static fn () => $workflow->activity('slow'),
                        'fast' => static fn () => $workflow->activity('fast'),
                    ]);
                    $atSelection = $cancelled->context->remaining();
                    $selected->handles['fast']->await();
                    $atOldWinner = $cancelled->context->remaining();
                    $selected->handles['slow']->await();
                    return [$atSelection, $atOldWinner, $cancelled->context->remaining()];
                });
            }
            return [];
        };
        $pending = self::replay($workflow, self::history());
        $history = self::selectionHistory($pending->commands);
        $history[] = self::completed(2, 'slow', '2026-10-01T00:00:20Z', $pending->commands[0]);
        $result = self::replay($workflow, $history);
        self::assertSame([18.123456, 18.123456, 10.123456],
            (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testAwaitedCancellationKeepsTheFirstReceiptTimeAcrossDuplicateMarkers(): void
    {
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): float {
                    $selected = $workflow->select([
                        'slow' => static fn () => $workflow->activity('slow'),
                        'fast' => static fn () => $workflow->activity('fast'),
                    ]);
                    $selected->handles['slow']->cancel();
                    try { $selected->handles['slow']->await(); }
                    catch (DurableOperationCancelled) { return $cancelled->context->remaining(); }
                    return -1;
                });
            }
            return -1;
        };
        $pending = self::replay($workflow, self::history());
        $history = self::selectionHistory($pending->commands);
        $cancelling = self::replay($workflow, $history);
        self::assertSame('cancel_selection_operation', $cancelling->commands[0]['type']);
        $receipt = ['event_type' => 'SelectionOperationCancelled', 'timestamp' => '2026-10-01T00:00:15Z',
            'payload' => $cancelling->commands[0]];
        $history[] = $receipt;
        $history[] = [...$receipt, 'timestamp' => '2026-10-01T00:00:28Z'];
        $result = self::replay($workflow, $history);
        self::assertSame(15.123456, (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testSynchronousLocalCallbackDoesNotMoveItsClockWhenItsResultIsPersisted(): void
    {
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) {
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled): float {
                    $workflow->localActivity('cleanup');
                    return $cancelled->context->remaining();
                });
            }
            return -1;
        };
        $calls = 0;
        $executor = static function () use (&$calls): array {
            ++$calls;
            return ['outcome' => 'completed', 'result' => 'cleaned'];
        };
        $replayer = new Replayer(new AvroPayloadCodec());
        $first = $replayer->replay($workflow, self::history(), [], 'php-workers', ['run_id' => 'run-1'], $executor);
        $history = [...self::history(), self::completed(2, 'cleanup', '2026-10-01T00:00:25Z', ['execution_mode' => 'local'])];
        $cold = $replayer->replay($workflow, $history, [], 'php-workers', ['run_id' => 'run-1'], $executor);
        $codec = new AvroPayloadCodec();
        self::assertSame(22.123456, $codec->decodeEnvelope($first->commands[1]['result']));
        self::assertSame(22.123456, $codec->decodeEnvelope($cold->commands[0]['result']));
        self::assertSame(1, $calls);
    }

    #[DataProvider('unavailableTimeProvider')]
    public function testUnavailableDeliveryTimeRefusesToGuess(?string $timestamp): void
    {
        $history = self::history();
        $history[1]['timestamp'] = $timestamp;
        $workflow = static function (WorkflowContext $workflow): float {
            try { $workflow->sleep(10); }
            catch (WorkflowCancelled $cancelled) { return $cancelled->context->remaining(); }
            return -1;
        };
        $this->expectException(LogicException::class);
        self::replay($workflow, $history);
    }

    public static function unavailableTimeProvider(): array
    {
        return [[null], ['next Thursday'], ['2026-02-30T00:00:08Z'], ['2026-10-01T00:00:08']];
    }

    private static function snapshot(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/cooperative-cancellation-context.json'),
            true, flags: JSON_THROW_ON_ERROR);
    }

    private static function history(): array
    {
        return [
            ['event_type' => 'CooperativeCancellationRequested', 'timestamp' => '2026-10-01T00:00:05Z', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_instance_id' => 'child-instance',
                'workflow_command_id' => 'request-1', 'cleanup_deadline_at' => '2026-10-01T00:00:30.123456Z',
                'reason' => 'maintenance', 'cancellation' => self::snapshot(),
            ]],
            ['event_type' => 'CooperativeCancellationDelivered', 'timestamp' => '2026-10-01T00:00:08Z', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1',
                'sequence' => 1, 'call_kind' => 'timer', 'cancellation' => self::snapshot(),
            ]],
        ];
    }

    private static function completed(int $sequence, string $activity, string $timestamp, array $metadata = []): array
    {
        return ['event_type' => 'ActivityCompleted', 'timestamp' => $timestamp,
            'payload' => [...$metadata, 'sequence' => $sequence, 'activity_type' => $activity,
                'result' => (new AvroPayloadCodec())->envelope('cleaned')]];
    }

    private static function selectionHistory(array $commands): array
    {
        $history = self::history();
        foreach ($commands as $index => $command) {
            $history[] = ['event_type' => 'ActivityScheduled', 'timestamp' => '2026-10-01T00:00:09Z',
                'payload' => [...$command, 'sequence' => 2 + $index,
                    'activity_execution_id' => $index === 0 ? 'slow-work' : 'fast-work']];
        }
        $history[] = ['id' => 'fast-completed', ...self::completed(3, 'fast', '2026-10-01T00:00:11Z', $commands[1])];
        $history[] = ['event_type' => 'SelectionResolved', 'timestamp' => '2026-10-01T00:00:12Z', 'payload' => [
            'selection_group_id' => 'select-calls:2:2', 'selection_group_base_sequence' => 2,
            'selection_group_size' => 2, 'member_key' => 'fast', 'member_index' => 1,
            'member_base_sequence' => 3, 'member_size' => 1, 'operation_kind' => 'activity',
            'operation_identity' => 'fast-work', 'outcome' => 'completed',
            'resolution_event_id' => 'fast-completed', 'resolution_event_type' => 'ActivityCompleted',
        ]];
        return $history;
    }

    private static function replay(callable $workflow, array $history): ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'run-1']);
    }
}
