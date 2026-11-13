<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowTaskCompletionTooLarge;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\JsonDocument;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * .github#228: Server 3.6.0 refuses a workflow-task completion over its aggregate
 * limit with a typed 413. The worker reports it as a structural-limit failure of
 * the run and never sends the oversized completion again.
 */
final class WorkflowTaskCompletionTooLargeTest extends TestCase
{
    private const LIMIT = 33_554_432;

    public function testTheTypedRefusalBecomesATypedErrorWithTheLimitAndTheSentSize(): void
    {
        $transport = new FakeTransport([self::typedRefusal()]);
        $commands = [['type' => 'complete_workflow', 'result' => (new AvroPayloadCodec())->envelope('done')]];

        try {
            (new Client('https://server.example', transport: $transport))
                ->completeWorkflowTask('task-1', 'worker-1', 2, $commands);
            self::fail('The typed refusal must raise.');
        } catch (WorkflowTaskCompletionTooLarge $error) {
            self::assertInstanceOf(ServerException::class, $error);
            self::assertSame(413, $error->status);
            self::assertSame('payload_too_large', $error->reason);
            self::assertSame(self::LIMIT, $error->limitBytes);
            self::assertSame(JsonDocument::size($transport->requests[0]['body']), $error->sizeBytes);
            self::assertSame('workflow_task_completion_bytes', $error->details['limit_kind'] ?? null);
            self::assertStringContainsString((string) self::LIMIT, $error->getMessage());
            self::assertStringContainsString((string) $error->sizeBytes, $error->getMessage());
            self::assertStringContainsString('child workflows', $error->getMessage());
        }
        self::assertCount(1, $transport->requests);
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
            self::assertNotInstanceOf(WorkflowTaskCompletionTooLarge::class, $error);
            self::assertSame($status, $error->status);
        }
        self::assertCount(1, $transport->requests);
    }

    /** @return iterable<string, array{int, array<string, mixed>|null, string}> */
    public static function ordinaryRefusals(): iterable
    {
        yield 'untyped 413' => [413, ['message' => 'Request payload exceeds the maximum allowed size of 2097152 bytes.',
            'reason' => 'payload_too_large', 'limit' => 2097152], 'complete'];
        yield 'other limit kind' => [413, ['reason' => 'payload_too_large', 'limit_kind' => 'something_else'], 'complete'];
        yield '413 without a body' => [413, null, 'complete'];
        yield 'typed body on a non-completion route' => [413, self::typedRefusal()->response, 'fail'];
        yield '500' => [500, ['message' => 'Server Error'], 'complete'];
        yield '503' => [503, null, 'complete'];
    }

    public function testTheWorkerFailsTheRunOnceAndNeverResendsTheOversizedCompletion(): void
    {
        $events = [];
        $transport = self::workflowTransport([self::typedRefusal(), ['completed' => true]]);
        $worker = self::fanOutWorker($transport, $events);

        self::assertTrue($worker->tick(0));

        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(2, $completions);
        self::assertCount(3, $completions[0]['body']['commands']);
        self::assertSame('schedule_activity', $completions[0]['body']['commands'][0]['type']);
        $failure = $completions[1]['body'];
        self::assertSame('worker-1', $failure['lease_owner']);
        self::assertSame(3, $failure['workflow_task_attempt']);
        self::assertCount(1, $failure['commands']);
        $command = $failure['commands'][0];
        self::assertSame('fail_workflow', $command['type']);
        self::assertStringStartsWith('Structural limit exceeded: workflow_task_completion_bytes: ', $command['message']);
        self::assertStringContainsString((string) self::LIMIT, $command['message']);
        self::assertStringContainsString(JsonDocument::size($completions[0]['body']).' bytes', $command['message']);
        self::assertStringContainsString('continue-as-new', $command['message']);
        self::assertSame('StructuralLimitExceeded', $command['exception_type']);
        self::assertTrue($command['non_retryable']);
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task-1/fail'));

        $logged = array_values(array_filter($events, static fn (array $event): bool
            => $event[0] === 'worker.workflow_task_completion_too_large'));
        self::assertCount(1, $logged);
        self::assertSame(self::LIMIT, $logged[0][1]['limit_bytes']);
        self::assertSame(JsonDocument::size($completions[0]['body']), $logged[0][1]['size_bytes']);
    }

    public function testAFailedStructuralFailureFallsThroughWithoutAnotherAttempt(): void
    {
        $events = [];
        $secondary = TransportException::fromResponse(500, ['message' => 'Server Error'], '{"message":"Server Error"}');
        $transport = self::workflowTransport([self::typedRefusal(), $secondary]);
        $worker = self::fanOutWorker($transport, $events);

        try {
            $worker->tick(0);
            self::fail('The failed structural failure must surface.');
        } catch (ServerException $error) {
            self::assertNotInstanceOf(WorkflowTaskCompletionTooLarge::class, $error);
            self::assertSame(500, $error->status);
        }
        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(2, $completions);
        self::assertSame('fail_workflow', $completions[1]['body']['commands'][0]['type']);
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task-1/fail'));
    }

    /** @param array<string, mixed>|null $response */
    #[DataProvider('unchangedWorkerRefusals')]
    public function testOtherCompletionRefusalsKeepTheirWorkerBehavior(int $status, ?array $response): void
    {
        $events = [];
        $transport = self::workflowTransport([TransportException::fromResponse($status, $response, (string) json_encode($response))]);
        $worker = self::fanOutWorker($transport, $events);

        try {
            $worker->tick(0);
            self::fail('The refusal must surface as before.');
        } catch (ServerException $error) {
            self::assertNotInstanceOf(WorkflowTaskCompletionTooLarge::class, $error);
            self::assertSame($status, $error->status);
        }
        $completions = self::requests($transport, '/workflow-tasks/workflow-task-1/complete');
        self::assertCount(1, $completions);
        self::assertSame('schedule_activity', $completions[0]['body']['commands'][0]['type']);
        self::assertSame([], array_filter($events, static fn (array $event): bool
            => $event[0] === 'worker.workflow_task_completion_too_large'));
    }

    /** @return iterable<string, array{int, array<string, mixed>|null}> */
    public static function unchangedWorkerRefusals(): iterable
    {
        yield 'untyped 413' => [413, ['message' => 'Request payload exceeds the maximum allowed size of 2097152 bytes.',
            'reason' => 'payload_too_large', 'limit' => 2097152]];
        yield '500' => [500, ['message' => 'Server Error']];
    }

    public function testAnOversizedActivityContinuationCompletesTheActivityAlone(): void
    {
        $caps = ['activity_completion_workflow_poll' => true, 'activity_workflow_continuation' => ['version' => 1,
            'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
            'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
            'supported_commands' => ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'],
            'max_history_events' => 256, 'max_commands' => 16]];
        $activities = $workflows = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($caps, &$activities, &$workflows): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                ++$workflows;

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
                throw self::typedRefusal();
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
            enableActivityContinuation: true);
        $worker->registerWorkflow('test.workflow', static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        $worker->registerActivity('test.activity', static fn (ActivityContext $context): int => 7);

        self::assertTrue($worker->tick(0));

        self::assertCount(1, self::requests($transport, '/activity-tasks/activity/continuation/complete'));
        $plain = self::requests($transport, '/activity-tasks/activity/complete');
        self::assertCount(1, $plain);
        self::assertArrayNotHasKey('continuation', $plain[0]['body']);
        self::assertSame(7, (new AvroPayloadCodec())->decodeEnvelope($plain[0]['body']['result']));
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
            if (str_ends_with($uri, '/query-tasks/poll') || str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail('Unexpected request: '.$uri);
        });
    }

    private static function typedRefusal(): TransportException
    {
        $response = [
            'message' => 'Workflow task completion exceeds the maximum allowed size of 33554432 bytes. Each command\'s arguments '
                .'may be up to 2097152 bytes; offload larger payloads or split the work across workflow tasks.',
            'reason' => 'payload_too_large',
            'limit' => self::LIMIT,
            'limit_kind' => 'workflow_task_completion_bytes',
            'protocol_version' => '1.20',
            'server_capabilities' => [],
        ];

        return TransportException::fromResponse(413, $response, (string) json_encode($response));
    }

    /** @return list<array{method: string, uri: string, headers: array<string, string>, body: ?array<string, mixed>}> */
    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
