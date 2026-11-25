<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\WorkflowReplayState;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use RuntimeException;
use Stringable;

final class WorkflowLoggerTest extends TestCase
{
    /** @var list<string> */
    private static array $states = [];

    public function testEachStepLogsOnceAcrossFreshReplayersAndAColdReplay(): void
    {
        $codec = new AvroPayloadCodec();
        $logger = new WorkflowLogRecorder();
        $task = ['workflow_id' => 'order-1', 'run_id' => 'run-1', 'workflow_type' => 'orders'];
        $history = [];
        self::$states = [];

        // Every workflow task replays from history with a new Replayer, as a
        // restarted worker does.
        $first = self::replayer($codec, $logger)->replay(self::workflow(), $history, ['Ada'], 'php-workers', $task);
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));

        $history[] = ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'reserve']];
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('reserved')]];
        $second = self::replayer($codec, $logger)->replay(self::workflow(), $history, ['Ada'], 'php-workers', $task);
        self::assertSame(['schedule_activity'], array_column($second->commands, 'type'));

        $history[] = ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'charge']];
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 2, 'result' => $codec->envelope('charged')]];
        $third = self::replayer($codec, $logger)->replay(self::workflow(), $history, ['Ada'], 'php-workers', $task);
        self::assertSame(['complete_workflow'], array_column($third->commands, 'type'));

        self::assertSame(
            ['order started', 'order reserved', 'order charged'],
            array_column($logger->records, 'message'),
        );
        foreach ($logger->records as $record) {
            self::assertSame('info', $record['level']);
            self::assertSame('order-1', $record['context']['workflow_id']);
            self::assertSame('run-1', $record['context']['workflow_run_id']);
            self::assertSame('orders', $record['context']['workflow_type']);
            self::assertSame('payments', $record['context']['namespace']);
        }
        self::assertSame('reserved', $logger->records[1]['context']['result']);

        // The opening line ran in every task but was new only in the first.
        self::assertSame(
            ['start:executing', 'start:replaying', 'after-reserve:executing', 'start:replaying',
                'after-reserve:replaying', 'after-charge:executing'],
            self::$states,
        );

        // A cold replay of the completed run writes nothing.
        $history[] = ['event_type' => 'WorkflowCompleted', 'payload' => ['result' => $codec->envelope('done')]];
        self::$states = [];
        self::replayer($codec, $logger)->replay(self::workflow(), $history, ['Ada'], 'php-workers', $task);
        self::assertCount(3, $logger->records);
        self::assertSame(['start:replaying', 'after-reserve:replaying', 'after-charge:replaying'], self::$states);
    }

    public function testNewWorkflowWorkInsideOneTaskIsLoggedOnce(): void
    {
        $codec = new AvroPayloadCodec();
        $logger = new WorkflowLogRecorder();
        $workflow = static function (WorkflowContext $context): string {
            $value = $context->sideEffect(static fn (): string => 'stamped');
            $context->logger()->notice('side effect recorded', ['value' => $value]);

            return $value;
        };

        $result = self::replayer($codec, $logger)->replay($workflow, [], [], 'php-workers', ['workflow_type' => 'stamp']);

        self::assertContains('complete_workflow', array_column($result->commands, 'type'));
        self::assertSame(['side effect recorded'], array_column($logger->records, 'message'));
        self::assertSame('stamped', $logger->records[0]['context']['value']);
    }

    public function testReplayStateIsUnavailableOutsideTheWorkflowFiber(): void
    {
        $codec = new AvroPayloadCodec();
        $logger = new WorkflowLogRecorder();
        $retained = null;
        $workflow = static function (WorkflowContext $context) use (&$retained): string {
            $retained = $context;

            return 'done';
        };

        self::replayer($codec, $logger)->replay($workflow, [], [], 'php-workers');

        self::assertInstanceOf(WorkflowContext::class, $retained);
        self::assertSame(WorkflowReplayState::Unavailable, $retained->replayState());
        self::assertFalse($retained->isReplaying());
        $retained->logger()->info('outside');
        self::assertSame([], $logger->records);
    }

    public function testReplayerWithoutALoggerAndFailingLoggersAreSilent(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): string {
            $context->logger()->error('ignored');

            return 'done';
        };

        $plain = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');
        self::assertSame(['complete_workflow'], array_column($plain->commands, 'type'));

        $broken = self::replayer($codec, new class() extends AbstractLogger {
            public function log($level, string|Stringable $message, array $context = []): void
            {
                throw new RuntimeException('log handler down');
            }
        })->replay($workflow, [], [], 'php-workers');
        self::assertSame(['complete_workflow'], array_column($broken->commands, 'type'));
    }

    public function testWorkerRoutesWorkflowLogsThroughItsLoggerOncePerStep(): void
    {
        $logger = new WorkflowLogRecorder();
        $codec = new AvroPayloadCodec();
        $tasks = [
            self::workflowTask('task-1', []),
            self::workflowTask('task-2', [
                ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'reserve']],
                ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('reserved')]],
            ]),
        ];

        // Two Worker instances: the second task runs after a worker restart.
        foreach ($tasks as $task) {
            $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$task): array {
                if (str_ends_with($uri, '/register')) {
                    return ['registered' => true];
                }
                if (str_ends_with($uri, '/workflow-tasks/poll') && $task !== null) {
                    $leased = $task;
                    $task = null;

                    return ['poll_status' => 'leased', 'task' => $leased];
                }
                if (preg_match('#/workflow-tasks/([^/]+)/heartbeat$#', $uri, $match) === 1) {
                    return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                        'renewed' => true, 'reason' => null];
                }
                if (str_contains($uri, '/workflow-tasks/') && !str_ends_with($uri, '/poll')) {
                    return ['completed' => true];
                }

                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            });
            $worker = new Worker(new Client('https://server.example', namespace: 'payments', transport: $transport),
                'php-workers', workerId: 'worker-1', logger: $logger);
            $worker->registerWorkflow('orders', self::workflow());
            $worker->run(0);
        }

        $workflowLines = array_values(array_filter(
            $logger->records,
            static fn (array $record): bool => str_starts_with((string) $record['message'], 'order '),
        ));
        self::assertSame(['order started', 'order reserved'], array_column($workflowLines, 'message'));
        self::assertSame('order-1', $workflowLines[1]['context']['workflow_id']);
        self::assertSame('payments', $workflowLines[1]['context']['namespace']);
    }

    private static function replayer(AvroPayloadCodec $codec, \Psr\Log\LoggerInterface $logger): Replayer
    {
        $replayer = new Replayer($codec);
        $replayer->useWorkflowLogger($logger, 'payments');

        return $replayer;
    }

    private static function workflow(): \Closure
    {
        return static function (WorkflowContext $context, string $name): string {
            self::$states[] = 'start:'.$context->replayState()->value;
            $context->logger()->info('order started', ['name' => $name]);
            $reserved = $context->activity('reserve', [$name]);
            self::$states[] = 'after-reserve:'.$context->replayState()->value;
            $context->logger()->info('order reserved', ['result' => $reserved]);
            $charged = $context->activity('charge', [$name]);
            self::$states[] = 'after-charge:'.$context->replayState()->value;
            $context->logger()->info('order charged', ['result' => $charged]);

            return 'done';
        };
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return array<string, mixed>
     */
    private static function workflowTask(string $id, array $history): array
    {
        $codec = new AvroPayloadCodec();

        return [
            'task_id' => $id,
            'workflow_task_attempt' => 1,
            'lease_owner' => 'worker-1',
            'workflow_id' => 'order-1',
            'run_id' => 'run-1',
            'workflow_type' => 'orders',
            'payload_codec' => 'avro',
            'arguments' => $codec->envelope(['Ada']),
            'history_events' => $history,
        ];
    }
}

final class WorkflowLogRecorder extends AbstractLogger
{
    /** @var list<array{level: mixed, message: string|Stringable, context: array<array-key, mixed>}> */
    public array $records = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->records[] = ['level' => $level, 'message' => $message, 'context' => $context];
    }
}
