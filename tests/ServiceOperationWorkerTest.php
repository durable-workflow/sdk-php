<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class ServiceOperationWorkerTest extends TestCase
{
    public function testWorkerRequiresAdvertisedServiceSupportBeforeSubmittingItsCommand(): void
    {
        foreach ([false, true] as $supported) {
            $commands = null;
            $failure = null;
            $transport = new FakeTransport(handler: static function (
                string $method, string $uri, array $headers, ?array $body,
            ) use ($supported, &$commands, &$failure): array {
                if (str_ends_with($uri, '/workflow-tasks/poll')) {
                    return ['server_capabilities' => ['supported_workflow_task_commands' => $supported ? ['start_service_operation'] : []],
                        'poll_status' => 'leased', 'task' => ['task_id' => 'call-task', 'workflow_task_attempt' => 1,
                            'lease_owner' => 'worker-1', 'workflow_id' => 'caller', 'run_id' => 'caller-run',
                            'workflow_type' => 'nexus.caller', 'payload_codec' => 'avro', 'history_events' => []]];
                }
                if (str_ends_with($uri, '/call-task/heartbeat')) {
                    return ['task_id' => 'call-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true];
                }
                if (str_ends_with($uri, '/call-task/complete')) {
                    $commands = $body['commands'];
                    return ['completed' => true];
                }
                if (str_ends_with($uri, '/call-task/fail')) {
                    $failure = $body['failure']['message'] ?? null;
                    return ['recorded' => true];
                }
                if (str_ends_with($uri, '/activity-tasks/poll')) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }
                self::fail("Unexpected request {$method} {$uri}");
            });
            $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'callers', workerId: 'worker-1');
            $worker->registerWorkflow('nexus.caller', static fn (WorkflowContext $context) =>
                $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42]));
            self::assertTrue($worker->tick(0));
            if ($supported) {
                self::assertNull($failure);
                self::assertSame('start_service_operation', $commands[0]['type']);
                self::assertSame(['invoice_id' => 42], (new AvroPayloadCodec())->decodeEnvelope($commands[0]['request_payload']));
            } else {
                self::assertNull($commands);
                self::assertStringContainsString('service_operations_unavailable', (string) $failure);
            }
        }
    }

    public function testHistoryPayloadsUseBoundedAuthenticatedDownloadsAndKeepUserMetadataOpaque(): void
    {
        $codec = new AvroPayloadCodec();
        $blob = $codec->encode(['large' => str_repeat('x', 8192)]);
        $envelope = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
        $downloads = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use ($blob, $envelope, &$downloads) {
            self::assertFalse($options['allow_redirects']);
            if (str_contains($request->getUri()->getPath(), '/external-payloads/')) {
                ++$downloads;
                self::assertSame('Bearer worker-key', $request->getHeaderLine('Authorization'));
                self::assertSame('caller', $request->getHeaderLine('X-Namespace'));
                self::assertTrue($options['stream']);
                return Create::promiseFor(new Response(200, ['Content-Type' => 'application/octet-stream',
                    'X-Durable-Workflow-Payload-Codec' => 'avro', 'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                    'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob)], $blob));
            }
            return Create::promiseFor(new Response(200, [], json_encode(['task' => ['history_events' => [[
                'event_type' => 'ServiceCallCompleted', 'payload' => ['sequence' => 1,
                    'request_payload' => $envelope, 'response_payload' => $envelope,
                    'service_call' => ['result_envelope' => $envelope, 'metadata' => ['user_reference' => $envelope]],
                ],
            ]]]], JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://runtime.test', namespace: 'caller', workerToken: 'worker-key', transport: new Psr18Transport($http));
        $poll = $client->pollWorkflowTaskResponse('worker-1', 'callers', 0);
        $payload = $poll['task']['history_events'][0]['payload'];
        self::assertSame($blob, $payload['request_payload']['blob']);
        self::assertSame($blob, $payload['response_payload']['blob']);
        self::assertSame($blob, $payload['service_call']['result_envelope']['blob']);
        self::assertSame($envelope, $payload['service_call']['metadata']['user_reference']);
        self::assertSame(1, $downloads, 'One request must fetch each exact retained object at most once.');
    }
}
