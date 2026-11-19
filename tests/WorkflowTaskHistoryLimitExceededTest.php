<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowTaskCompletionTooLarge;
use DurableWorkflow\Exception\WorkflowTaskHistoryLimitExceeded;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * .github#228: a completion whose history transaction would exceed the Server's
 * history budget is refused with 409 `history_transaction_limit_exceeded`
 * (native Server 3.13.0 names `limit_kind`, `current_value` and
 * `configured_limit`). Replaying gives the same commands, so the worker fails the
 * run once with a structural-limit failure instead of retrying the task forever.
 */
final class WorkflowTaskHistoryLimitExceededTest extends TestCase
{
    private const LIMIT = 65_536;
    private const MEASURED = 214_380;

    public function testTheTypedRefusalBecomesATypedErrorWithTheServerMeasure(): void
    {
        $transport = new FakeTransport([self::typedRefusal()]);

        try {
            (new Client('https://server.example', transport: $transport))
                ->completeWorkflowTask('task-1', 'worker-1', 2, [['type' => 'complete_workflow']]);
            self::fail('The typed refusal must raise.');
        } catch (WorkflowTaskHistoryLimitExceeded $error) {
            self::assertInstanceOf(ServerException::class, $error);
            self::assertNotInstanceOf(WorkflowTaskCompletionTooLarge::class, $error);
            self::assertSame(409, $error->status);
            self::assertSame('history_transaction_limit_exceeded', $error->reason);
            self::assertSame('history_transaction_bytes', $error->limitKind);
            self::assertSame(self::MEASURED, $error->currentValue);
            self::assertSame(self::LIMIT, $error->configuredLimit);
            self::assertStringStartsWith('Structural limit exceeded: history_transaction_bytes:', $error->serverMessage);
            self::assertStringContainsString(self::MEASURED.' history bytes', $error->getMessage());
            self::assertStringContainsString('server limit of '.self::LIMIT.' history bytes', $error->getMessage());
            self::assertStringContainsString('will not be sent again', $error->getMessage());
        }
        self::assertCount(1, $transport->requests);
    }

    public function testAnUntypedBudgetRefusalNamesItsReason(): void
    {
        foreach (WorkflowTaskHistoryLimitExceeded::REASONS as $reason) {
            $error = WorkflowTaskHistoryLimitExceeded::fromResponse(['reason' => $reason, 'recorded' => false]);

            self::assertSame($reason, $error->limitKind);
            self::assertSame($reason, $error->reason);
            self::assertNull($error->currentValue);
            self::assertNull($error->configuredLimit);
            $command = $error->failWorkflowCommand();
            self::assertStringStartsWith("Structural limit exceeded: {$reason}: ", $command['message']);
            self::assertStringContainsString('more than the limit allows', $command['message']);
            self::assertStringContainsString('over the server limit.', $command['message']);
        }
    }

    public function testTheServerMessageIsCappedAndUntrustedFieldsAreIgnored(): void
    {
        $error = WorkflowTaskHistoryLimitExceeded::fromResponse(['reason' => 'history_transaction_limit_exceeded',
            'limit_kind' => "bad kind\n", 'current_value' => '12', 'configured_limit' => -1, 'message' => str_repeat('é', 400)]);

        self::assertSame('history_transaction_limit_exceeded', $error->limitKind);
        self::assertNull($error->currentValue);
        self::assertNull($error->configuredLimit);
        self::assertLessThanOrEqual(WorkflowTaskCompletionTooLarge::SERVER_MESSAGE_MAX_BYTES + 3, strlen($error->serverMessage));
        self::assertSame(1, preg_match('//u', $error->serverMessage));
        self::assertStringNotContainsString('é', $error->failWorkflowCommand()['message']);
    }

    /** @param array<string, mixed>|null $response */
    #[DataProvider('ordinaryRefusals')]
    public function testOtherRefusalsKeepTheirServerException(int $status, ?array $response, string $path): void
    {
        $transport = new FakeTransport([TransportException::fromResponse($status, $response, (string) json_encode($response))]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            if ($path === 'complete') {
                $client->completeWorkflowTask('task-1', 'worker-1', 2, [['type' => 'complete_workflow']]);
            } else {
                $client->failWorkflowTask('task-1', 'worker-1', 2, 'boom');
            }
            self::fail('The refusal must raise.');
        } catch (ServerException $error) {
            self::assertNotInstanceOf(WorkflowTaskHistoryLimitExceeded::class, $error);
            self::assertSame($status, $error->status);
        }
    }

    /** @return iterable<string, array{int, array<string, mixed>|null, string}> */
    public static function ordinaryRefusals(): iterable
    {
        $typed = self::typedRefusal()->response;
        yield 'other 409 reason' => [409, ['reason' => 'invalid_commands'], 'complete'];
        yield 'recorded receipt' => [409, ['recorded' => true] + (array) $typed, 'complete'];
        yield 'typed body with another status' => [422, $typed, 'complete'];
        yield 'typed body on a non-completion route' => [409, $typed, 'fail'];
        yield '409 without a body' => [409, null, 'complete'];
    }

    public function testTheWorkerFailsTheRunOnceAndNeverResendsTheRefusedCompletion(): void
    {
        $events = [];
        $transport = self::workflowTransport([self::typedRefusal(), ['completed' => true]]);
        $worker = self::fanOutWorker($transport, $events);

        self::assertTrue($worker->tick(0));

        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(2, $completions);
        self::assertSame(['schedule_activity', 'schedule_activity', 'schedule_activity'],
            array_column($completions[0]['body']['commands'], 'type'));
        $failure = $completions[1]['body'];
        self::assertSame('worker-1', $failure['lease_owner']);
        self::assertSame(3, $failure['workflow_task_attempt']);
        self::assertSame([[
            'type' => 'fail_workflow',
            'message' => 'Structural limit exceeded: history_transaction_bytes: the workflow task completion would write '
                .self::MEASURED.' history bytes in one history transaction, over the server limit of '.self::LIMIT
                .' history bytes. '.WorkflowTaskHistoryLimitExceeded::GUIDANCE,
            'exception_type' => 'StructuralLimitExceeded',
            'non_retryable' => true,
        ]], $failure['commands']);
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task-1/fail'));

        $logged = array_values(array_filter($events, static fn (array $event): bool
            => $event[0] === 'worker.workflow_task_history_limit_exceeded'));
        self::assertCount(1, $logged);
        self::assertSame('history_transaction_bytes', $logged[0][1]['limit_kind']);
        self::assertSame(self::MEASURED, $logged[0][1]['current_value']);
        self::assertSame(self::LIMIT, $logged[0][1]['configured_limit']);
        self::assertSame('run-1', $logged[0][1]['run_id']);
    }

    #[DataProvider('followUpRefusals')]
    public function testAFailedStructuralFailureFallsThroughWithoutAnotherAttempt(string $kind): void
    {
        $events = [];
        $secondary = match ($kind) {
            'server_error' => TransportException::fromResponse(500, ['message' => 'Server Error'], '{"message":"Server Error"}'),
            'second_typed_409' => self::typedRefusal(),
        };
        $transport = self::workflowTransport([self::typedRefusal(), $secondary]);
        $worker = self::fanOutWorker($transport, $events);

        try {
            $worker->tick(0);
            self::fail('The failed structural failure must surface.');
        } catch (ServerException $error) {
            self::assertSame($secondary->status, $error->status);
        }
        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(2, $completions);
        self::assertSame('fail_workflow', $completions[1]['body']['commands'][0]['type']);
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task-1/fail'));
    }

    /** @return iterable<string, array{string}> */
    public static function followUpRefusals(): iterable
    {
        yield 'server_error' => ['server_error'];
        yield 'second_typed_409' => ['second_typed_409'];
    }

    public function testAnOtherwiseConflictingCompletionKeepsItsWorkerBehavior(): void
    {
        $events = [];
        $response = ['reason' => 'invalid_commands', 'message' => 'Unsupported command.'];
        $transport = self::workflowTransport([TransportException::fromResponse(409, $response, (string) json_encode($response)),
            ['recorded' => true]]);
        $worker = self::fanOutWorker($transport, $events);

        try {
            $worker->tick(0);
        } catch (ServerException $error) {
            self::assertNotInstanceOf(WorkflowTaskHistoryLimitExceeded::class, $error);
        }
        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(1, $completions);
        self::assertSame([], array_filter($events, static fn (array $event): bool
            => $event[0] === 'worker.workflow_task_history_limit_exceeded'));
    }

    public function testAContinuationOverTheHistoryBudgetCompletesTheActivityAlone(): void
    {
        $caps = ['activity_completion_workflow_poll' => true, 'activity_workflow_continuation' => ['version' => 1,
            'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
            'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
            'supported_commands' => ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'],
            'max_history_events' => 256, 'max_commands' => 16]];
        $activities = 0;
        $events = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($caps, &$activities): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activities === 1 ? ['task' => ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
                    'activity_execution_id' => 'execution-1', 'attempt_number' => 1, 'lease_owner' => 'worker',
                    'activity_type' => 'test.activity', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased', 'server_capabilities' => $caps] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity/continuation/history')) {
                return ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
                    'continuation' => ['version' => 1, 'eligible' => true, 'reason' => null],
                    'workflow_id' => 'workflow-instance', 'run_id' => 'run', 'workflow_type' => 'test.workflow',
                    'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
                    'history_through_sequence' => 1, 'history_last_event_id' => 'event-1',
                    'history_events' => [['id' => 'event-1', 'sequence' => 1, 'event_type' => 'ActivityScheduled',
                        'payload' => ['sequence' => 1, 'activity_execution_id' => 'execution-1', 'activity_type' => 'test.activity',
                            'activity' => ['type' => 'test.activity']],
                        'recorded_at' => '2026-11-09T12:00:00.000000Z', 'workflow_task_id' => 'workflow-0', 'workflow_command_id' => null]]];
            }
            if (str_ends_with($uri, '/activity-tasks/activity/continuation/complete')) {
                // Native Server answers a continuation over the budget without a workflow_task_attempt.
                $response = ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt', 'outcome' => 'completed',
                    'recorded' => false, 'reason' => 'history_transaction_limit_exceeded', 'limit_kind' => 'history_transaction_size',
                    'current_value' => 2, 'configured_limit' => 1];
                throw TransportException::fromResponse(409, $response, (string) json_encode($response));
            }
            if (str_ends_with($uri, '/activity-tasks/activity/complete')) {
                return ['task_id' => 'activity', 'recorded' => true, 'workflow_poll' => ['leased' => false]];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail('Unexpected request: '.$uri);
        });
        $worker = new Worker(new Client('https://continuation.invalid', transport: $transport), 'queue', workerId: 'worker',
            enableActivityContinuation: true, diagnosticListener: static function (string $event, array $context) use (&$events): void {
                $events[] = [$event, $context];
            });
        $worker->registerWorkflow('test.workflow', static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        $worker->registerActivity('test.activity', static fn (ActivityContext $context): int => 7);

        self::assertTrue($worker->tick(0));

        self::assertCount(1, self::requests($transport, '/activity-tasks/activity/continuation/complete'));
        $plain = self::requests($transport, '/activity-tasks/activity/complete');
        self::assertCount(1, $plain);
        self::assertArrayNotHasKey('continuation', $plain[0]['body']);
        self::assertSame(7, (new AvroPayloadCodec())->decodeEnvelope($plain[0]['body']['result']));
        $logged = array_values(array_filter($events, static fn (array $event): bool
            => $event[0] === 'worker.activity_continuation_history_limit_exceeded'));
        self::assertCount(1, $logged);
        self::assertSame('history_transaction_size', $logged[0][1]['limit_kind']);
    }

    /**
     * The history native Server 3.13.0 recorded for this failure (StartAccepted,
     * WorkflowStarted, then the structural WorkflowFailed) replays through the
     * official Replayer to the same refused commands. The failure adds no command
     * or history shape, so it is neither a replay block nor a nondeterminism
     * failure, and histories recorded before this change replay unchanged.
     */
    public function testTheRecordedStructuralFailureReplaysToTheRefusedCommands(): void
    {
        $recorded = json_decode((string) file_get_contents(__DIR__.'/fixtures/history-transaction-limit-recorded-failure.json'),
            true, flags: JSON_THROW_ON_ERROR);
        $types = array_column($recorded['history'], 'event_type');
        self::assertSame(['StartAccepted', 'WorkflowStarted', 'WorkflowFailed'], $types);
        $failed = $recorded['history'][2]['payload'];
        self::assertSame('structural_limit', $failed['failure_category']);
        self::assertTrue($failed['non_retryable']);
        self::assertStringStartsWith('Structural limit exceeded: history_transaction_bytes: ', $failed['message']);

        $replay = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context, int $count): array {
                $operations = [];
                for ($index = 0; $index < $count; ++$index) {
                    $operations[] = $context->deferActivity('history.length', [str_repeat('x', 16)]);
                }

                return $context->all($operations);
            },
            $recorded['history'],
            $recorded['workflow']['input'],
            'history-limit',
            ['workflow_type' => $recorded['workflow']['type'], 'workflow_id' => 'history-limit-workflow', 'run_id' => 'history-limit-run'],
        );

        self::assertSame($recorded['replayed_commands'], array_column($replay->commands, 'type'));
        self::assertSame(['history.length'], array_values(array_unique(array_column($replay->commands, 'activity_type'))));
    }

    /** @param array<string, mixed> $events */
    private static function fanOutWorker(FakeTransport $transport, array &$events): Worker
    {
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker-1',
            diagnosticListener: static function (string $event, array $context) use (&$events): void {
                $events[] = [$event, $context];
            });
        $worker->registerWorkflow('orders.workflow', static function (WorkflowContext $context): mixed {
            return $context->all([
                static fn (): mixed => $context->activity('orders.charge', [1]),
                static fn (): mixed => $context->activity('orders.charge', [2]),
                static fn (): mixed => $context->activity('orders.charge', [3]),
            ]);
        });

        return $worker;
    }

    /** @param list<array<string, mixed>|\Throwable> $completions */
    private static function workflowTransport(array $completions): FakeTransport
    {
        $polled = false;

        return new FakeTransport(handler: static function (string $method, string $uri) use (&$polled, &$completions): ?array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if ($polled) {
                    return ['task' => null, 'poll_status' => 'empty'];
                }
                $polled = true;

                return ['poll_status' => 'leased', 'task' => ['task_id' => 'workflow-task-1', 'workflow_task_attempt' => 3,
                    'lease_owner' => 'worker-1', 'workflow_id' => 'workflow-1', 'run_id' => 'run-1',
                    'workflow_type' => 'orders.workflow', 'payload_codec' => 'avro', 'history_events' => []]];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/heartbeat')) {
                return ['task_id' => 'workflow-task-1', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1',
                    'renewed' => true, 'lease_expires_at' => '2026-07-18T00:40:00Z', 'run_status' => 'running',
                    'task_status' => 'leased', 'reason' => null];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/complete')) {
                $next = array_shift($completions) ?? self::fail('Unexpected extra completion.');
                if ($next instanceof \Throwable) {
                    throw $next;
                }

                return $next;
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task-1/fail')) {
                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll') || str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail('Unexpected request: '.$uri);
        });
    }

    /** Native Server 3.13.0's refusal, field for field. */
    private static function typedRefusal(): TransportException
    {
        $response = [
            'task_id' => 'workflow-task-1',
            'workflow_task_attempt' => 3,
            'outcome' => 'completed',
            'recorded' => false,
            'run_id' => null,
            'run_status' => null,
            'created_task_ids' => [],
            'reason' => 'history_transaction_limit_exceeded',
            'limit_kind' => 'history_transaction_bytes',
            'current_value' => self::MEASURED,
            'configured_limit' => self::LIMIT,
            'message' => 'Structural limit exceeded: history_transaction_bytes: this workflow task completion would write '
                .self::MEASURED.' history bytes in one transaction (limit '.self::LIMIT.').',
        ];

        return TransportException::fromResponse(409, $response, (string) json_encode($response));
    }

    /** @return list<array{method: string, uri: string, headers: array<string, string>, body: ?array<string, mixed>}> */
    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
