<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ClientContractTest extends TestCase
{
    public function testStartUsesControlPlaneHeadersAndOfficialAvroEnvelope(): void
    {
        $transport = new FakeTransport([[
            'workflow_id' => 'order-1',
            'run_id' => 'run-1',
            'workflow_type' => 'order',
        ]]);
        $client = new Client('https://server.example/', transport: $transport, token: 'token', namespace: 'tenant-a');

        $handle = $client->startWorkflow('order', 'order-1', 'workers', [['sku' => 'A']]);

        self::assertSame('run-1', $handle->selectedRunId);
        self::assertSame('https://server.example/api/workflows', $transport->requests[0]['uri']);
        self::assertSame('2', $transport->requests[0]['headers']['X-Durable-Workflow-Control-Plane-Version']);
        self::assertSame('tenant-a', $transport->requests[0]['headers']['X-Namespace']);
        self::assertSame('Bearer token', $transport->requests[0]['headers']['Authorization']);
        self::assertSame('avro', $transport->requests[0]['body']['input']['codec']);
        self::assertSame([['sku' => 'A']], $client->payloadCodec()->decodeEnvelope($transport->requests[0]['body']['input']));
    }

    public function testSelectedRunSignalUsesGuardedRoute(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('https://server.example', transport: $transport);

        $client->workflowHandle('order/1', 'run/2')->signalSelectedRun('approve now', ['Ada']);

        self::assertSame(
            'https://server.example/api/workflows/order%2F1/runs/run%2F2/signal/approve%20now',
            $transport->requests[0]['uri'],
        );
    }

    public function testWorkerRequestsAdvertiseWorkerProtocol(): void
    {
        $transport = new FakeTransport([['task' => null]]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertNull($client->pollActivityTask('worker-1', 'queue', 0));
        self::assertSame('1.13', $transport->requests[0]['headers']['X-Durable-Workflow-Protocol-Version']);
        self::assertSame('php-activity-poll', substr($transport->requests[0]['body']['poll_request_id'], 0, 17));
    }

    public function testTaskPollResponseMethodsPreserveTheCompleteEnvelope(): void
    {
        $workflowResponse = [
            'task' => null,
            'poll_status' => 'stale_worker_registration',
            'reason' => 'worker_heartbeat_stale',
            'protocol_version' => '1.13',
            'server_capabilities' => ['worker_heartbeats' => true],
            'worker_id' => 'worker-1',
        ];
        $activityResponse = [
            'task' => null,
            'poll_status' => 'stale_worker_registration',
            'reason' => 'worker_heartbeat_stale',
            'protocol_version' => '1.13',
            'request_metadata' => ['poll_request_replayed' => false],
        ];
        $queryResponse = [
            'task' => null,
            'poll_status' => 'stale_worker_registration',
            'reason' => 'worker_heartbeat_stale',
            'protocol_version' => '1.13',
            'server_capabilities' => ['query_tasks' => true],
        ];
        $transport = new FakeTransport([$workflowResponse, $activityResponse, $queryResponse]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($workflowResponse, $client->pollWorkflowTaskResponse('worker-1', 'queue', 0));
        self::assertSame($activityResponse, $client->pollActivityTaskResponse('worker-1', 'queue', 0));
        self::assertSame($queryResponse, $client->pollQueryTaskResponse('worker-1', 'queue', 0));
    }

    public function testTaskOnlyPollMethodsDelegateTaskAndEmptyResponses(): void
    {
        $workflowTask = ['task_id' => 'workflow-task'];
        $activityTask = ['task_id' => 'activity-task'];
        $queryTask = ['query_task_id' => 'query-task'];
        $transport = new FakeTransport([
            ['task' => $workflowTask, 'poll_status' => 'leased'],
            ['task' => $activityTask, 'poll_status' => 'leased'],
            ['task' => $queryTask, 'poll_status' => 'leased'],
            ['task' => null, 'poll_status' => 'empty'],
            ['task' => null, 'poll_status' => 'timeout'],
            ['task' => null, 'poll_status' => 'workflow_task_pending'],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($workflowTask, $client->pollWorkflowTask('worker-1', 'queue', 0));
        self::assertSame($activityTask, $client->pollActivityTask('worker-1', 'queue', 0));
        self::assertSame($queryTask, $client->pollQueryTask('worker-1', 'queue', 0));
        self::assertNull($client->pollWorkflowTask('worker-1', 'queue', 0));
        self::assertNull($client->pollActivityTask('worker-1', 'queue', 0));
        self::assertNull($client->pollQueryTask('worker-1', 'queue', 0));
    }

    public function testTerminalConflictPollResponseRemainsAnEnvelope(): void
    {
        $response = [
            'task' => null,
            'poll_status' => 'draining',
            'reason' => 'worker_draining',
            'worker_status' => 'draining',
            'drain_intent' => 'draining',
        ];
        $transport = new FakeTransport([
            TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR)),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($response, $client->pollWorkflowTaskResponse('worker-1', 'queue', 0));
    }

    public function testNonTerminalConflictPollResponseRemainsAnError(): void
    {
        $response = [
            'task' => null,
            'poll_status' => 'no_compatible_worker',
            'reason' => 'build_id_incompatible',
        ];
        $transport = new FakeTransport([
            TransportException::fromResponse(409, $response, json_encode($response, JSON_THROW_ON_ERROR)),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        $this->expectException(ServerException::class);
        $client->pollWorkflowTaskResponse('worker-1', 'queue', 0);
    }

    public function testEmptyWorkflowTaskCompletionAcknowledgesWaitingForHistory(): void
    {
        $transport = new FakeTransport([['outcome' => 'waiting_for_history']]);
        $client = new Client('https://server.example', transport: $transport);

        $response = $client->completeWorkflowTask('task/1', 'worker-1', 3, []);

        self::assertSame('waiting_for_history', $response['outcome']);
        self::assertSame('POST', $transport->requests[0]['method']);
        self::assertSame('https://server.example/api/worker/workflow-tasks/task%2F1/fail', $transport->requests[0]['uri']);
        self::assertSame([
            'lease_owner' => 'worker-1',
            'workflow_task_attempt' => 3,
            'failure' => [
                'message' => 'Workflow task waiting for scheduled history.',
                'type' => 'WorkflowTaskWaitingForHistory',
            ],
        ], $transport->requests[0]['body']);
    }
}
