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
        $first = self::flushed($codec, $logger, self::workflow(), $history, ['Ada'], $task);
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));

        $history[] = ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'reserve']];
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'result' => $codec->envelope('reserved')]];
        $second = self::flushed($codec, $logger, self::workflow(), $history, ['Ada'], $task);
        self::assertSame(['schedule_activity'], array_column($second->commands, 'type'));

        $history[] = ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 2, 'activity_type' => 'charge']];
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 2, 'result' => $codec->envelope('charged')]];
        $third = self::flushed($codec, $logger, self::workflow(), $history, ['Ada'], $task);
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
        self::flushed($codec, $logger, self::workflow(), $history, ['Ada'], $task);
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

        $result = self::flushed($codec, $logger, $workflow, [], [], ['workflow_type' => 'stamp']);

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

        self::flushed($codec, $logger, $workflow, [], []);

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

        // A replay only collects lines; nothing is written until the Worker
        // submits it, and a superseded replay's lines are discarded.
        $replayer = new Replayer($codec);
        $replayer->useWorkflowLogger(new WorkflowLogRecorder(), 'payments');
        $replayer->replay($workflow, [], [], 'php-workers');
        $replayer->replay($workflow, [], [], 'php-workers');
        self::assertSame(['ignored'], array_column($replayer->takeWorkflowLogs(), 'message'));
        self::assertSame([], $replayer->takeWorkflowLogs());
    }

    public function testCodeBeforeAwaitingAStartedChildLogsOnce(): void
    {
        $codec = new AvroPayloadCodec();
        $logger = new WorkflowLogRecorder();
        $workflow = static function (WorkflowContext $context): string {
            $context->logger()->info('child flow started');
            $child = $context->startChildWorkflow('billing.reconcile', ['acct-1']);
            $context->logger()->info('child flow awaiting');
            $result = (string) $child->await();
            $context->logger()->info('child flow finished', ['result' => $result]);

            return $result;
        };
        $identity = ['sequence' => 1, 'child_call_id' => 'call-1', 'workflow_link_id' => 'call-1',
            'child_workflow_instance_id' => 'child-instance', 'child_workflow_run_id' => 'child-run-1',
            'child_workflow_type' => 'billing.reconcile'];
        $started = [
            ['event_type' => 'ChildWorkflowScheduled', 'payload' => $identity],
            ['event_type' => 'ChildRunStarted', 'payload' => $identity],
        ];
        $completed = ['event_type' => 'ChildRunCompleted', 'payload' => [...$identity, 'result' => $codec->envelope('reconciled')]];

        self::assertSame(['start_child_workflow'], array_column(self::flushed($codec, $logger, $workflow, [], [])->commands, 'type'));
        // The child is running: the await parks without a command or a new step,
        // so the code after the start runs again in the next task.
        self::assertSame([], self::flushed($codec, $logger, $workflow, $started, [])->commands);
        self::assertSame(['child flow started'], array_column($logger->records, 'message'));

        $done = self::flushed($codec, $logger, $workflow, [...$started, $completed], []);
        self::assertSame(['complete_workflow'], array_column($done->commands, 'type'));
        self::assertSame(
            ['child flow started', 'child flow awaiting', 'child flow finished'],
            array_column($logger->records, 'message'),
        );
    }

    public function testTwoChildAwaitsLogEachLineOnce(): void
    {
        $codec = new AvroPayloadCodec();
        $logger = new WorkflowLogRecorder();
        $workflow = static function (WorkflowContext $context): array {
            $a = $context->startChildWorkflow('billing.a');
            $b = $context->startChildWorkflow('billing.b');
            $context->logger()->info('fan-out started both');
            $first = (string) $a->await();
            $context->logger()->info('fan-out first done');
            $second = (string) $b->await();
            $context->logger()->info('fan-out second done');

            return [$first, $second];
        };
        $child = static fn (int $sequence, string $type): array => ['sequence' => $sequence, 'child_call_id' => "call-{$sequence}",
            'workflow_link_id' => "call-{$sequence}", 'child_workflow_instance_id' => "child-{$sequence}",
            'child_workflow_run_id' => "run-{$sequence}", 'child_workflow_type' => $type];
        $startedA = [['event_type' => 'ChildWorkflowScheduled', 'payload' => $child(1, 'billing.a')],
            ['event_type' => 'ChildRunStarted', 'payload' => $child(1, 'billing.a')]];
        $startedB = [['event_type' => 'ChildWorkflowScheduled', 'payload' => $child(2, 'billing.b')],
            ['event_type' => 'ChildRunStarted', 'payload' => $child(2, 'billing.b')]];
        $doneA = ['event_type' => 'ChildRunCompleted', 'payload' => [...$child(1, 'billing.a'), 'result' => $codec->envelope('a')]];
        $doneB = ['event_type' => 'ChildRunCompleted', 'payload' => [...$child(2, 'billing.b'), 'result' => $codec->envelope('b')]];

        self::flushed($codec, $logger, $workflow, [], []);
        self::flushed($codec, $logger, $workflow, $startedA, []);
        // Both children run: the first await parks before any new step.
        self::assertSame([], self::flushed($codec, $logger, $workflow, [...$startedA, ...$startedB], [])->commands);
        self::assertSame([], array_column($logger->records, 'message'));
        // The first child is done; the second await parks after an await that
        // resumed without recording a step.
        self::assertSame([], self::flushed($codec, $logger, $workflow, [...$startedA, ...$startedB, $doneA], [])->commands);
        self::assertSame([], array_column($logger->records, 'message'));

        $done = self::flushed($codec, $logger, $workflow, [...$startedA, ...$startedB, $doneA, $doneB], []);
        self::assertSame(['complete_workflow'], array_column($done->commands, 'type'));
        self::assertSame(
            ['fan-out started both', 'fan-out first done', 'fan-out second done'],
            array_column($logger->records, 'message'),
        );
    }

    public function testLinesLeftByAFailedTaskAreNeverWrittenByTheNextTask(): void
    {
        $logger = new WorkflowLogRecorder();
        $codec = new AvroPayloadCodec();
        $tasks = [
            self::workflowTask('task-broken', []) + ['workflow_type' => 'broken'],
            ['task_id' => 'task-update', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                'workflow_id' => 'counter-1', 'run_id' => 'run-c', 'workflow_type' => 'counter', 'payload_codec' => 'avro',
                'workflow_update_id' => 'update-1', 'history_events' => [[
                    'event_type' => 'UpdateAccepted',
                    'payload' => ['update_id' => 'update-1', 'update_name' => 'increment', 'arguments' => $codec->envelope([1])],
                ]]],
        ];
        $tasks[0]['workflow_type'] = 'broken';
        $tasks[0]['arguments'] = $codec->envelope([]);
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$tasks): array {
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $task = array_shift($tasks);

                return $task === null ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['poll_status' => 'leased', 'task' => $task];
            }
            if (preg_match('#/workflow-tasks/([^/]+)/heartbeat$#', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                    'renewed' => true, 'reason' => null];
            }
            if (str_contains($uri, '/workflow-tasks/') && !str_ends_with($uri, '/poll')) {
                return ['completed' => true, 'failed' => true];
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = new Worker(new Client('https://server.example', namespace: 'payments', transport: $transport),
            'php-workers', workerId: 'worker-1', logger: $logger);
        $worker->registerWorkflow('broken', static function (WorkflowContext $context): string {
            $context->logger()->info('broken line');

            throw new \DurableWorkflow\Exception\NonDeterministicWorkflow('History no longer matches.');
        });
        $worker->registerWorkflow('counter', static fn (WorkflowContext $context): string => 'waiting')
            ->registerUpdate('counter', 'increment', static fn (\DurableWorkflow\Worker\QueryContext $context, int $value): int => $value + 1);

        $worker->run(0);

        self::assertNotContains('broken line', array_column($logger->records, 'message'));
        $uris = array_column($transport->requests, 'uri');
        self::assertNotEmpty(array_filter($uris, static fn (string $uri): bool => str_contains($uri, '/task-broken/fail')),
            implode(', ', $uris));
        self::assertNotEmpty(array_filter($uris, static fn (string $uri): bool => str_contains($uri, '/task-update/complete')),
            implode(', ', $uris));
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

    /**
     * Replay like the Worker: the lines of a submitted replay are written after it returns.
     *
     * @param list<array<string, mixed>> $history
     * @param list<mixed> $input
     * @param array<string, mixed> $task
     */
    private static function flushed(
        AvroPayloadCodec $codec,
        \Psr\Log\LoggerInterface $logger,
        callable $workflow,
        array $history,
        array $input,
        array $task = [],
    ): \DurableWorkflow\Worker\ReplayResult {
        $replayer = new Replayer($codec);
        $replayer->useWorkflowLogger($logger, 'payments');
        $result = $replayer->replay($workflow, $history, $input, 'php-workers', $task);
        foreach ($replayer->takeWorkflowLogs() as $record) {
            $logger->log($record['level'], $record['message'], $record['context']);
        }

        return $result;
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

