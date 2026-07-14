<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Model\SchedulePage;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class ControlPlaneParityTest extends TestCase
{
    public function testNamespaceSelectionAndWorkflowVisibilityAreImmutableAndTyped(): void
    {
        $transport = new FakeTransport([[
            'workflows' => [[
                'workflow_id' => 'order-1',
                'run_id' => 'run-1',
                'workflow_type' => 'orders.process',
                'business_key' => 'customer-42',
                'status' => 'running',
                'status_bucket' => 'running',
                'is_terminal' => false,
                'task_queue' => 'orders',
                'started_at' => '2026-07-13T20:00:00Z',
                'search_attributes' => ['CustomerId' => '42'],
            ]],
            'workflow_count' => 1,
            'next_page_token' => 'page-2',
        ]]);
        $client = new Client('https://server.example', transport: $transport, token: 'secret', namespace: 'default');

        $tenant = $client->withNamespace('orders-prod');
        $page = $tenant->listWorkflows(
            workflowType: 'orders.process',
            status: 'running',
            query: 'CustomerId = "42"',
            pageSize: 25,
            nextPageToken: 'page-1',
        );

        self::assertSame('default', $client->namespace);
        self::assertSame('orders-prod', $tenant->namespace);
        self::assertSame('page-2', $page->nextPageToken);
        self::assertSame(1, $page->workflowCount);
        self::assertSame('customer-42', $page->executions[0]->businessKey);
        self::assertSame(['CustomerId' => '42'], $page->executions[0]->searchAttributes);
        self::assertSame('GET', $transport->requests[0]['method']);
        self::assertSame(
            'https://server.example/api/workflows?workflow_type=orders.process&status=running&query=CustomerId%20%3D%20%2242%22&page_size=25&next_page_token=page-1',
            $transport->requests[0]['uri'],
        );
        self::assertSame('orders-prod', $transport->requests[0]['headers']['X-Namespace']);
        self::assertSame('Bearer secret', $transport->requests[0]['headers']['Authorization']);
    }

    public function testSearchAttributesAndExternalStorageUseNamespaceControlRoutes(): void
    {
        $transport = new FakeTransport([
            [
                'system_attributes' => ['WorkflowId' => 'keyword'],
                'custom_attributes' => ['CustomerId' => 'keyword'],
            ],
            ['name' => 'OrderTotal', 'type' => 'double', 'outcome' => 'created'],
            ['name' => 'Temporary Field', 'outcome' => 'deleted'],
            [
                'name' => 'orders-prod',
                'external_payload_storage' => [
                    'driver' => 's3',
                    'enabled' => true,
                    'threshold_bytes' => 2097152,
                    'config' => ['bucket' => 'workflow-payloads'],
                ],
            ],
        ]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'orders-prod');

        $attributes = $client->listSearchAttributes();
        $created = $client->createSearchAttribute('OrderTotal', 'double');
        $deleted = $client->deleteSearchAttribute('Temporary Field');
        $namespace = $client->setNamespaceExternalStorage(
            'orders-prod',
            's3',
            thresholdBytes: 2097152,
            config: ['bucket' => 'workflow-payloads'],
        );

        self::assertSame('keyword', $attributes->systemAttributes['WorkflowId']);
        self::assertSame('double', $created->type);
        self::assertSame('deleted', $deleted->outcome);
        self::assertSame('s3', $namespace->externalPayloadStorage['driver'] ?? null);
        self::assertSame([
            ['GET', 'https://server.example/api/search-attributes', null],
            ['POST', 'https://server.example/api/search-attributes', ['name' => 'OrderTotal', 'type' => 'double']],
            ['DELETE', 'https://server.example/api/search-attributes/Temporary%20Field', null],
            ['PUT', 'https://server.example/api/namespaces/orders-prod/external-storage', [
                'driver' => 's3',
                'enabled' => true,
                'threshold_bytes' => 2097152,
                'config' => ['bucket' => 'workflow-payloads'],
            ]],
        ], array_map(
            static fn (array $request): array => [$request['method'], $request['uri'], $request['body']],
            $transport->requests,
        ));
        foreach ($transport->requests as $request) {
            self::assertSame('orders-prod', $request['headers']['X-Namespace']);
        }
    }

    public function testServiceOperationsEncodeAvroAndExposeStartExecuteDescribeAndCancel(): void
    {
        $transport = new FakeTransport([
            ['service_call_id' => 'call-1', 'accepted' => true, 'status' => 'accepted', 'outcome' => 'accepted'],
            ['service_call_id' => 'call-2', 'accepted' => true, 'status' => 'completed', 'outcome' => 'completed'],
            ['service_call_id' => 'call-1', 'found' => true, 'status' => 'started', 'outcome' => 'started'],
            ['service_call_id' => 'call-1', 'accepted' => true, 'status' => 'cancelled', 'outcome' => 'cancelled'],
        ]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'tenant-a');
        $options = new ServiceOperationOptions(
            waitTimeoutSeconds: 15,
            idempotencyKey: 'order-42-charge',
            callerNamespace: 'tenant-a',
            callerWorkflowId: 'order-42',
            callerRunId: 'run-42',
            targetWorkflowId: 'payment-42',
            taskQueue: 'payments',
            businessKey: 'order-42',
            searchAttributes: ['CustomerId' => '42'],
        );

        $handle = $client->startServiceOperation(
            'payment gateway',
            'Payments/V2',
            'authorize card',
            ['amount' => 4200, 'currency' => 'USD'],
            $options,
        );
        $executed = $client->executeServiceOperation(
            'payment gateway',
            'Payments/V2',
            'capture card',
            ['amount' => 4200],
            new ServiceOperationOptions(modeOverride: 'sync'),
        );
        $described = $handle->describe();
        $cancelled = $handle->cancel('customer request');

        self::assertSame('call-1', $handle->serviceCallId);
        self::assertSame('completed', $executed->status);
        self::assertSame('started', $described->status);
        self::assertSame('cancelled', $cancelled->status);

        $start = $transport->requests[0];
        self::assertSame('POST', $start['method']);
        self::assertSame(
            'https://server.example/api/service-endpoints/payment%20gateway/services/Payments%2FV2/operations/authorize%20card/execute',
            $start['uri'],
        );
        self::assertSame('avro', $start['body']['payload_codec']);
        self::assertSame(
            ['amount' => 4200, 'currency' => 'USD'],
            $client->payloadCodec()->decode($start['body']['arguments']),
        );
        self::assertSame('async', $start['body']['mode_override']);
        self::assertSame('accepted', $start['body']['wait_for']);
        self::assertSame('order-42-charge', $start['body']['idempotency_key']);
        self::assertSame('order-42', $start['body']['caller_workflow_instance_id']);
        self::assertSame('payments', $start['body']['queue']);
        self::assertSame('tenant-a', $start['headers']['X-Namespace']);

        self::assertSame('sync', $transport->requests[1]['body']['mode_override']);
        self::assertSame('completed', $transport->requests[1]['body']['wait_for']);
        self::assertSame('GET', $transport->requests[2]['method']);
        self::assertSame(
            'https://server.example/api/service-endpoints/payment%20gateway/services/Payments%2FV2/operations/authorize%20card/service-calls/call-1',
            $transport->requests[2]['uri'],
        );
        self::assertSame('POST', $transport->requests[3]['method']);
        self::assertSame(['reason' => 'customer request'], $transport->requests[3]['body']);
    }

    public function testSchedulePageMapsDescriptionsContinuationAndRawEnvelope(): void
    {
        $response = [
            'schedules' => [[
                'schedule_id' => 'nightly',
                'status' => 'paused',
                'action' => ['workflow_type' => 'rollup'],
            ]],
            'next_page_token' => 'schedule-page-2',
            'request_id' => 'request-42',
        ];
        $transport = new FakeTransport([$response]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'ops');

        $page = $client->listSchedules();

        self::assertInstanceOf(SchedulePage::class, $page);
        self::assertSame('nightly', $page->schedules[0]->scheduleId);
        self::assertSame('rollup', $page->schedules[0]->action['workflow_type'] ?? null);
        self::assertSame('schedule-page-2', $page->nextPageToken);
        self::assertSame($response, $page->raw);
        self::assertSame('ops', $transport->requests[0]['headers']['X-Namespace']);
    }

    public function testScheduleListPreservesUnfilteredServerResultsAndPublicRoutes(): void
    {
        $transport = new FakeTransport([
            [
                'schedules' => [
                    [
                        'schedule_id' => 'nightly',
                        'status' => 'paused',
                        'action' => ['workflow_type' => 'rollup'],
                    ],
                    [
                        'schedule_id' => 'hourly',
                        'status' => 'active',
                        'action' => ['workflow_type' => 'sync'],
                    ],
                ],
                'next_page_token' => null,
            ],
            ['schedule_id' => 'nightly', 'events' => [['sequence' => 8]], 'has_more' => false],
            [
                'server_id' => 'cluster-a',
                'version' => '2.0.0',
                'default_namespace' => 'default',
                'capabilities' => ['service_execution' => true],
                'control_plane' => ['version' => '2'],
            ],
            ['workflow_id' => 'order/1', 'run_id' => 'current-run', 'workflow_type' => 'order'],
            ['workflow_id' => 'order/1', 'run_id' => 'selected/run', 'workflow_type' => 'order'],
            [],
            [],
        ]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'ops');

        $schedulePage = $client->listSchedules(['status' => 'paused', 'page_size' => 20]);
        $history = $client->scheduleHistory('nightly', 100, 7);
        $cluster = $client->clusterInfo(includeDiagnostics: true);
        $handle = $client->workflowHandle('order/1', 'selected/run');
        $current = $handle->describe();
        $selected = $handle->describeSelectedRun();
        $handle->cancel();
        $handle->cancelSelectedRun('selected only');

        // The current server ignores list query parameters; preserve every returned item.
        self::assertSame(['nightly', 'hourly'], array_map(
            static fn ($schedule): string => $schedule->scheduleId,
            $schedulePage->schedules,
        ));
        self::assertSame(['paused', 'active'], array_map(
            static fn ($schedule): ?string => $schedule->status,
            $schedulePage->schedules,
        ));
        self::assertNull($schedulePage->nextPageToken);
        self::assertSame(8, $history['events'][0]['sequence']);
        self::assertTrue($cluster->capabilities['service_execution']);
        self::assertSame('current-run', $current->runId);
        self::assertSame('selected/run', $selected->runId);
        self::assertSame('GET', $transport->requests[0]['method']);
        self::assertSame('GET', $transport->requests[2]['method']);
        self::assertSame('ops', $transport->requests[2]['headers']['X-Namespace']);
        self::assertSame('2', $transport->requests[2]['headers']['X-Durable-Workflow-Control-Plane-Version']);
        self::assertSame([
            'https://server.example/api/schedules?status=paused&page_size=20',
            'https://server.example/api/schedules/nightly/history?limit=100&after_sequence=7',
            'https://server.example/api/cluster/info?include=diagnostics',
            'https://server.example/api/workflows/order%2F1',
            'https://server.example/api/workflows/order%2F1/runs/selected%2Frun',
            'https://server.example/api/workflows/order%2F1/cancel',
            'https://server.example/api/workflows/order%2F1/runs/selected%2Frun/cancel',
        ], array_column($transport->requests, 'uri'));
        self::assertSame(['reason' => 'selected only'], $transport->requests[6]['body']);
    }

    public function testEveryAddedSurfacePreservesNonSuccessEvidence(): void
    {
        $operations = [
            'workflow visibility' => static fn (Client $client) => $client->listWorkflows(),
            'search attributes' => static fn (Client $client) => $client->listSearchAttributes(),
            'namespace storage' => static fn (Client $client) => $client->setNamespaceExternalStorage('orders', 's3'),
            'service operations' => static fn (Client $client) => $client->startServiceOperation('payments', 'Cards', 'charge'),
            'schedule list' => static fn (Client $client) => $client->listSchedules(),
            'cluster discovery' => static fn (Client $client) => $client->clusterInfo(),
        ];

        foreach ($operations as $surface => $operation) {
            $transport = new FakeTransport([
                new TransportException('Access denied.', 403, [
                    'reason' => 'authorization_failed',
                    'evidence' => ['required_role' => 'operator'],
                ]),
            ]);
            $client = new Client('https://server.example', transport: $transport);

            try {
                $operation($client);
                self::fail("{$surface} did not preserve the transport failure.");
            } catch (ServerException $exception) {
                self::assertSame(403, $exception->status, $surface);
                self::assertSame('authorization_failed', $exception->reason, $surface);
                self::assertSame(
                    'operator',
                    $exception->details['evidence']['required_role'] ?? null,
                    $surface,
                );
            }
        }
    }

    public function testDefaultTransportTreatsEveryNonTwoHundredResponseAsErrorEvidence(): void
    {
        $http = new class implements ClientInterface {
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                return new Response(409, ['Content-Type' => 'application/json'], json_encode([
                    'message' => 'The definition already exists.',
                    'reason' => 'attribute_already_exists',
                    'name' => 'CustomerId',
                ], JSON_THROW_ON_ERROR));
            }
        };
        $client = new Client('https://server.example', transport: new Psr18Transport($http));

        try {
            $client->createSearchAttribute('CustomerId', 'keyword');
            self::fail('The conflict response was not raised as a typed server error.');
        } catch (ServerException $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('attribute_already_exists', $exception->reason);
            self::assertSame('CustomerId', $exception->details['name'] ?? null);
        }
    }
}
