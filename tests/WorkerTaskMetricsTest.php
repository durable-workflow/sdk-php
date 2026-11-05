<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\MetricsObserver;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * Worker task metrics record the worker's processing outcome, once per task,
 * never the workflow's terminal state (matching the Python SDK).
 */
final class WorkerTaskMetricsTest extends TestCase
{
    public function testCompletedAndFailedActivityTasksAreObservedOnce(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $worker = new Worker(new Client('https://server.example', transport: $this->activityTransport(), metrics: $recorder),
            'orders', workerId: 'worker-1', clock: static fn (): float => 0);
        $calls = 0;
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$calls): string {
            if (++$calls === 2) {
                throw new \RuntimeException('declined');
            }

            return 'charged';
        });

        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));

        self::assertSame([['activity', 'orders', 'completed'], ['activity', 'orders', 'failed']], $this->tasks($recorder));
    }

    public function testAStaleAttemptIsAbortedAndTheNextTaskCompletes(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $worker = new Worker(new Client('https://server.example', transport: $this->activityTransport(staleFirst: true), metrics: $recorder),
            'orders', workerId: 'worker-1', clock: static fn (): float => 0);
        $worker->registerActivity('orders.charge', static fn (ActivityContext $context): string => 'charged');

        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));

        self::assertSame([['activity', 'orders', 'aborted'], ['activity', 'orders', 'completed']], $this->tasks($recorder));
    }

    public function testAFailingWorkflowStillRecordsACompletedWorkflowTask(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $completions = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completions): ?array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                static $served = false;
                if ($served) {
                    return ['task' => null, 'poll_status' => 'empty'];
                }
                $served = true;

                return ['task' => ['task_id' => 'workflow-task-1', 'workflow_id' => 'private-workflow-id',
                    'run_id' => 'private-run-id', 'workflow_type' => 'orders.failing', 'workflow_task_attempt' => 1,
                    'payload_codec' => 'avro', 'history_events' => []], 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/complete')) {
                $completions[] = $body;

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['renewed' => true, 'task_id' => 'workflow-task-1', 'lease_owner' => $body['lease_owner'],
                    'workflow_task_attempt' => $body['workflow_task_attempt']];
            }
            throw new \LogicException('Unexpected request '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport, metrics: $recorder),
            'orders', workerId: 'worker-1', clock: static fn (): float => 0);
        $worker->registerWorkflow('orders.failing', static function (WorkflowContext $context): never {
            throw new \DomainException('order rejected');
        });

        self::assertTrue($worker->tick(0));

        // The workflow failed, but the worker processed its task successfully.
        self::assertCount(1, $completions);
        self::assertSame('fail_workflow', $completions[0]['commands'][0]['type'] ?? null);
        self::assertSame([['workflow', 'orders', 'completed']], $this->tasks($recorder));
        self::assertStringNotContainsString('private-', json_encode($recorder->events, JSON_THROW_ON_ERROR));
    }

    public function testAForkedChildCannotRecordThroughTheInheritedRecorder(): void
    {
        if (!function_exists('pcntl_fork')) {
            self::markTestSkipped('Fork isolation needs ext-pcntl.');
        }
        $recorder = new RecordingMetricsRecorder();
        $observer = new MetricsObserver($recorder);
        $pipe = tempnam(sys_get_temp_dir(), 'dw-metrics-fork-');
        $pid = pcntl_fork();
        if ($pid === 0) {
            $observer->observe('worker_task', 0.1, ['task_kind' => 'activity', 'task_queue' => 'orders', 'outcome' => 'completed']);
            file_put_contents($pipe, json_encode(['enabled' => $observer->enabled(), 'events' => count($recorder->events)]));
            exit(0);
        }
        pcntl_waitpid($pid, $status);
        $child = json_decode((string) file_get_contents($pipe), true, flags: JSON_THROW_ON_ERROR);
        unlink($pipe);

        self::assertSame(['enabled' => false, 'events' => 0], $child);
        self::assertTrue($observer->enabled());
    }

    /** @return list<array{string, string, string}> task kind, queue and outcome for each observed task */
    private function tasks(RecordingMetricsRecorder $recorder): array
    {
        $tasks = [];
        $events = array_values(array_filter($recorder->events,
            static fn (array $event): bool => str_starts_with($event['name'], 'durable_workflow_worker_task')));
        self::assertSame(0, count($events) % 2, 'Each task records a counter and a duration together.');
        for ($i = 0; $i < count($events); $i += 2) {
            self::assertSame('durable_workflow_worker_tasks', $events[$i]['name']);
            self::assertSame('durable_workflow_worker_task_duration_seconds', $events[$i + 1]['name']);
            self::assertSame($events[$i]['tags'], $events[$i + 1]['tags']);
            self::assertGreaterThanOrEqual(0.0, $events[$i + 1]['value']);
            $tasks[] = [$events[$i]['tags']['task_kind'], $events[$i]['tags']['task_queue'], $events[$i]['tags']['outcome']];
        }

        return $tasks;
    }

    private function activityTransport(bool $staleFirst = false): FakeTransport
    {
        $polls = 0;

        return new FakeTransport(handler: static function (string $method, string $uri) use (&$polls, $staleFirst): ?array {
            if (str_ends_with($uri, '/workflow-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                $number = ++$polls;

                return ['task' => ['task_id' => 'activity-'.$number, 'activity_attempt_id' => 'attempt-'.$number,
                    'lease_owner' => 'worker-1', 'activity_type' => 'orders.charge', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased'];
            }
            if ($staleFirst && str_ends_with($uri, '/activity-1/complete')) {
                $proof = ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1',
                    'reason' => 'stale_attempt', 'outcome' => 'completed', 'recorded' => false,
                    'attempt_status' => 'failed', 'activity_status' => 'failed', 'task_status' => 'cancelled',
                    'can_continue' => false, 'heartbeat_recorded' => false];
                throw TransportException::fromResponse(409, $proof, json_encode($proof, JSON_THROW_ON_ERROR));
            }
            if (str_ends_with($uri, '/complete') || str_ends_with($uri, '/fail')) {
                return ['recorded' => true, 'outcome' => 'completed'];
            }

            throw new \LogicException('Unexpected request: '.$method.' '.$uri);
        });
    }
}
