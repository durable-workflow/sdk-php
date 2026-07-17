<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerTerminalTaskRaceTest extends TestCase
{
    #[DataProvider('workflowTerminalAcknowledgementProvider')]
    public function testManagedWorkerProcessesTheNextWorkflowAfterATerminalAcknowledgementRace(
        string $failurePoint,
    ): void {
        $workflowPolls = 0;
        $fallbackAcknowledgements = 0;
        $nextWorkflowCompleted = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (
            $failurePoint,
            &$workflowPolls,
            &$fallbackAcknowledgements,
            &$nextWorkflowCompleted,
        ): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                ++$workflowPolls;

                $response = self::leasedWorkflowTask($workflowPolls === 1 ? 'closed-task' : 'next-task');
                if ($workflowPolls === 1 && $failurePoint === 'fallback') {
                    $response['task']['history_events'] = [[
                        'event_type' => 'ActivityScheduled',
                        'payload' => ['sequence' => 1, 'activity_type' => 'unresolved.activity'],
                    ]];
                }

                return $response;
            }

            if (str_ends_with($uri, '/workflow-tasks/closed-task/heartbeat')) {
                if ($failurePoint === 'heartbeat') {
                    throw self::workflowRunClosedConflict('closed-task');
                }

                return ['acknowledged' => true];
            }

            if (str_ends_with($uri, '/workflow-tasks/closed-task/complete')) {
                if ($failurePoint === 'completion') {
                    throw self::workflowRunClosedConflict('closed-task');
                }

                self::fail('The closed workflow task should not complete.');
            }

            if (str_ends_with($uri, '/workflow-tasks/closed-task/fail')) {
                ++$fallbackAcknowledgements;
                throw self::workflowRunClosedConflict('closed-task');
            }

            if (str_ends_with($uri, '/workflow-tasks/next-task/heartbeat')) {
                return ['acknowledged' => true];
            }

            if (str_ends_with($uri, '/workflow-tasks/next-task/complete')) {
                $nextWorkflowCompleted = true;

                return ['completed' => true];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return $nextWorkflowCompleted
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }

            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );
        $worker->registerWorkflow(
            'race.workflow',
            static fn (WorkflowContext $context): string => 'completed',
        );

        $worker->run(0);

        self::assertTrue($nextWorkflowCompleted);
        self::assertSame($failurePoint === 'fallback' ? 1 : 0, $fallbackAcknowledgements);
    }

    /** @return iterable<string, array{string}> */
    public static function workflowTerminalAcknowledgementProvider(): iterable
    {
        yield 'workflow heartbeat' => ['heartbeat'];
        yield 'workflow completion' => ['completion'];
        yield 'fallback workflow failure' => ['fallback'];
    }

    #[DataProvider('activityTerminalAcknowledgementProvider')]
    public function testManagedWorkerProcessesTheNextActivityAfterATerminalAcknowledgementRace(
        string $failurePoint,
    ): void {
        $activityPolls = 0;
        $fallbackAcknowledgements = 0;
        $nextActivityCompleted = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (
            $failurePoint,
            &$activityPolls,
            &$fallbackAcknowledgements,
            &$nextActivityCompleted,
        ): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                ++$activityPolls;

                return self::leasedActivityTask($activityPolls === 1 ? 'closed-activity' : 'next-activity');
            }

            if (str_ends_with($uri, '/activity-tasks/closed-activity/heartbeat')) {
                return [
                    'cancel_requested' => true,
                    'can_continue' => false,
                    'reason' => 'run_terminated',
                ];
            }

            if (str_ends_with($uri, '/activity-tasks/closed-activity/complete')) {
                if ($failurePoint === 'completion') {
                    throw self::activityRunClosedConflict('closed-activity');
                }

                self::fail('The closed activity task should not complete.');
            }

            if (str_ends_with($uri, '/activity-tasks/closed-activity/fail')) {
                ++$fallbackAcknowledgements;
                throw self::activityRunClosedConflict('closed-activity');
            }

            if (str_ends_with($uri, '/activity-tasks/next-activity/complete')) {
                $nextActivityCompleted = true;

                return ['completed' => true];
            }

            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return $nextActivityCompleted
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );
        $worker->registerActivity(
            'race.activity',
            static function (ActivityContext $context) use ($failurePoint): string {
                if ($context->taskId === 'closed-activity') {
                    if ($failurePoint === 'heartbeat') {
                        $context->heartbeat();
                    }
                    if ($failurePoint === 'fallback') {
                        throw new \RuntimeException('Activity execution failed while the run closed.');
                    }
                }

                return 'completed';
            },
        );

        $worker->run(0);

        self::assertTrue($nextActivityCompleted);
        self::assertSame($failurePoint === 'completion' ? 0 : 1, $fallbackAcknowledgements);
    }

    /** @return iterable<string, array{string}> */
    public static function activityTerminalAcknowledgementProvider(): iterable
    {
        yield 'activity heartbeat and fallback failure' => ['heartbeat'];
        yield 'activity completion' => ['completion'];
        yield 'fallback activity failure' => ['fallback'];
    }

    #[DataProvider('queryTerminalAcknowledgementProvider')]
    public function testManagedWorkerProcessesTheNextQueryAfterATerminalAcknowledgementRace(
        string $failurePoint,
    ): void {
        $queryPolls = 0;
        $fallbackAcknowledgements = 0;
        $nextQueryCompleted = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (
            $failurePoint,
            &$queryPolls,
            &$fallbackAcknowledgements,
            &$nextQueryCompleted,
        ): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }

            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return $nextQueryCompleted
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['task' => null, 'poll_status' => 'empty'];
            }

            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                ++$queryPolls;

                return self::leasedQueryTask($queryPolls === 1 ? 'timed-out-query' : 'next-query');
            }

            if (str_ends_with($uri, '/query-tasks/timed-out-query/complete')) {
                throw self::queryTimedOutConflict('timed-out-query');
            }

            if (str_ends_with($uri, '/query-tasks/timed-out-query/fail')) {
                ++$fallbackAcknowledgements;
                throw self::queryTimedOutConflict('timed-out-query');
            }

            if (str_ends_with($uri, '/query-tasks/next-query/complete')) {
                $nextQueryCompleted = true;

                return ['completed' => true];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );
        $worker->registerQuery(
            'race.workflow',
            'status',
            static function (QueryContext $context) use ($failurePoint): string {
                if ($context->workflowId === 'timed-out-workflow' && $failurePoint === 'fallback') {
                    throw new \RuntimeException('Query execution failed while its deadline expired.');
                }

                return 'ready';
            },
        );

        $worker->run(0);

        self::assertTrue($nextQueryCompleted);
        self::assertSame($failurePoint === 'fallback' ? 1 : 0, $fallbackAcknowledgements);
    }

    /** @return iterable<string, array{string}> */
    public static function queryTerminalAcknowledgementProvider(): iterable
    {
        yield 'query completion' => ['completion'];
        yield 'fallback query failure' => ['fallback'];
    }

    #[DataProvider('unrelatedAcknowledgementFailureProvider')]
    public function testUnrelatedAcknowledgementFailureStillStopsTheWorker(
        string $failurePoint,
        int $expectedStatus,
        string $expectedReason,
        bool $expectedFallbackAcknowledgement,
    ): void
    {
        $fallbackAcknowledged = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use ($failurePoint, &$fallbackAcknowledged): ?array {
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $response = self::leasedWorkflowTask('owned-task');
                if ($failurePoint === 'fallback') {
                    $response['task']['history_events'] = [[
                        'event_type' => 'ActivityScheduled',
                        'payload' => ['sequence' => 1, 'activity_type' => 'unresolved.activity'],
                    ]];
                }

                return $response;
            }

            if (str_ends_with($uri, '/workflow-tasks/owned-task/heartbeat')) {
                if ($failurePoint === 'primary') {
                    throw TransportException::fromResponse(
                        503,
                        ['error' => 'Temporary acknowledgement failure.', 'reason' => 'backend_unavailable'],
                        '',
                    );
                }

                return ['acknowledged' => true];
            }

            if (str_ends_with($uri, '/workflow-tasks/owned-task/fail')) {
                $fallbackAcknowledged = true;
                $response = [
                    'error' => 'Workflow task lease is owned by another worker.',
                    'reason' => 'lease_owner_mismatch',
                    'lease_owner' => 'worker-2',
                ];
                throw TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR));
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'worker-1',
        );
        $worker->registerWorkflow(
            'race.workflow',
            static fn (WorkflowContext $context): string => 'completed',
        );

        try {
            $worker->tick(0);
            self::fail('An unrelated acknowledgement failure must remain fatal to the managed worker.');
        } catch (ServerException $exception) {
            self::assertSame($expectedStatus, $exception->status);
            self::assertSame($expectedReason, $exception->reason);
        }
        self::assertSame($expectedFallbackAcknowledgement, $fallbackAcknowledged);
    }

    /** @return iterable<string, array{string, int, string, bool}> */
    public static function unrelatedAcknowledgementFailureProvider(): iterable
    {
        yield 'primary server failure' => ['primary', 503, 'backend_unavailable', false];
        yield 'fallback lease conflict' => ['fallback', 409, 'lease_owner_mismatch', true];
    }

    /** @return array{poll_status: string, task: array<string, mixed>} */
    private static function leasedWorkflowTask(string $taskId): array
    {
        return [
            'poll_status' => 'leased',
            'task' => [
                'task_id' => $taskId,
                'workflow_task_attempt' => 1,
                'lease_owner' => 'worker-1',
                'workflow_id' => $taskId.'-workflow',
                'run_id' => $taskId.'-run',
                'workflow_type' => 'race.workflow',
                'history_events' => [],
            ],
        ];
    }

    /** @return array{poll_status: string, task: array<string, mixed>} */
    private static function leasedActivityTask(string $taskId): array
    {
        return [
            'poll_status' => 'leased',
            'task' => [
                'task_id' => $taskId,
                'activity_attempt_id' => $taskId.'-attempt',
                'lease_owner' => 'worker-1',
                'activity_type' => 'race.activity',
            ],
        ];
    }

    /** @return array{poll_status: string, task: array<string, mixed>} */
    private static function leasedQueryTask(string $taskId): array
    {
        return [
            'poll_status' => 'leased',
            'task' => [
                'query_task_id' => $taskId,
                'query_task_attempt' => 1,
                'lease_owner' => 'worker-1',
                'workflow_id' => $taskId === 'timed-out-query' ? 'timed-out-workflow' : 'next-workflow',
                'run_id' => $taskId.'-run',
                'workflow_type' => 'race.workflow',
                'query_name' => 'status',
                'history_events' => [],
            ],
        ];
    }

    private static function workflowRunClosedConflict(string $taskId): TransportException
    {
        $response = [
            'task_id' => $taskId,
            'workflow_task_attempt' => 1,
            'error' => 'Workflow run is already closed.',
            'reason' => 'run_closed',
            'stop_reason' => 'run_terminated',
            'task_status' => 'cancelled',
            'can_continue' => false,
        ];

        return TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }

    private static function activityRunClosedConflict(string $taskId): TransportException
    {
        $response = [
            'task_id' => $taskId,
            'activity_attempt_id' => $taskId.'-attempt',
            'error' => 'Activity outcome ignored because the workflow run is already closed.',
            'reason' => 'run_terminated',
            'task_status' => 'cancelled',
            'can_continue' => false,
        ];

        return TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }

    private static function queryTimedOutConflict(string $taskId): TransportException
    {
        $response = [
            'query_task_id' => $taskId,
            'outcome' => 'rejected',
            'reason' => 'query_task_timed_out',
            'error' => 'Query task timed out before completion.',
        ];

        return TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }
}
