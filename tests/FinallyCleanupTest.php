<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Worker\DeferredWorkflowOperation;
use DurableWorkflow\Worker\DurableOperationHandle;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use Fiber;
use FiberError;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use WeakReference;

final class FinallyCleanupTest extends TestCase
{
    /** @return iterable<string, array{string}> */
    public static function cleanupKinds(): iterable
    {
        foreach (['activity', 'timer', 'memo', 'handle-await', 'handle-cancel'] as $kind) {
            yield $kind => [$kind];
        }
    }

    #[DataProvider('cleanupKinds')]
    public function testGarbageCollectionDoesNotEmitCleanupOrRetainFibers(string $kind): void
    {
        $reference = null;
        $afterCleanup = false;
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use ($kind, &$reference, &$afterCleanup): void {
                $reference = WeakReference::create(Fiber::getCurrent());
                try {
                    $context->sleep(1);
                } finally {
                    match ($kind) {
                        'activity' => $context->activity('cleanup'),
                        'timer' => $context->sleep(2),
                        'memo' => $context->upsertMemo(['cleaned' => true]),
                        'handle-await' => self::handle()->await(),
                        'handle-cancel' => self::handle()->cancel(),
                    };
                    $afterCleanup = true;
                }
            },
            [],
            [],
            'cleanup-queue',
        );

        self::assertSame(['start_timer'], array_column($result->commands, 'type'));
        gc_collect_cycles();
        self::assertFalse($afterCleanup);
        self::assertNull($reference->get());
        self::assertNull($result->terminalFailure);
    }

    /** @return iterable<string, array{bool}> */
    public static function outcomes(): iterable
    {
        yield 'success' => [false];
        yield 'handled failure' => [true];
    }

    #[DataProvider('outcomes')]
    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testActivityTimerAndMemoCleanupColdReplay(bool $fail): void
    {
        $codec = new AvroPayloadCodec();
        $history = [
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'work']],
            ['event_type' => $fail ? 'ActivityFailed' : 'ActivityCompleted', 'payload' => [
                'sequence' => 1,
                'activity_type' => 'work',
                'result' => $codec->envelope('success'),
                'message' => 'work failed',
                'exception_type' => 'WorkFailed',
            ]],
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'cleanup']],
            ['event_type' => 'ActivityCompleted', 'payload' => [
                'sequence' => 2, 'result' => $codec->envelope('cleaned'),
            ]],
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 3, 'delay_seconds' => 1]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 3, 'delay_seconds' => 1]],
            ['event_type' => 'MemoUpserted', 'payload' => [
                'sequence' => 4, 'entries' => $codec->envelope(['cleanup' => 'cleaned']),
                'merged' => $codec->envelope(['cleanup' => 'cleaned']),
            ]],
        ];
        $workflow = static function (WorkflowContext $context): string {
            try {
                $result = $context->activity('work');
            } catch (ActivityFailed) {
                $result = 'handled';
            } finally {
                $cleanup = $context->activity('cleanup');
                $context->sleep(1);
                $context->upsertMemo(['cleanup' => $cleanup]);
            }

            return $result;
        };

        $result = (new Replayer($codec))->replay($workflow, $history, [], 'cleanup-queue');
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame($fail ? 'handled' : 'success', $codec->decodeEnvelope($result->commands[0]['result']));

        foreach ([
            0 => ['schedule_activity'],
            2 => ['schedule_activity'],
            4 => ['start_timer'],
            6 => ['upsert_memo', 'complete_workflow'],
        ] as $length => $commands) {
            $pending = (new Replayer($codec))->replay($workflow, array_slice($history, 0, $length), [], 'cleanup-queue');
            self::assertSame($commands, array_column($pending->commands, 'type'));
            gc_collect_cycles();
            self::assertNull($pending->terminalFailure);
        }
    }

    public function testOtherFiberErrorsAreNotSwallowed(): void
    {
        $this->expectException(FiberError::class);
        $this->expectExceptionMessage('Cannot resume a fiber that is not suspended');
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn () => Fiber::getCurrent()->resume(), [], [], 'cleanup-queue',
        );
    }

    private static function handle(): DurableOperationHandle
    {
        return new DurableOperationHandle(
            'cleanup', 0, 'activity', 'cleanup-id', 1, 1, 'cleanup-group',
            new DeferredWorkflowOperation(WorkflowCommand::activity('cleanup', [])),
            'workflow', 'run',
        );
    }
}
