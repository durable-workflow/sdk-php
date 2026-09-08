<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RuntimePayloads;
use DurableWorkflow\Transport\RuntimePayloadUploads;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class RuntimePayloadUploadTest extends TestCase
{
    public function testClientUploadsEncodedBytesWithNamespaceAndClientCredential(): void
    {
        [$client, $http] = $this->client();
        $value = str_repeat('value', 80);
        $blob = $client->payloadCodec()->encode([$value]);
        $client->startWorkflow('echo', 'one', 'queue', [$value]);
        $client->startWorkflow('echo', 'two', 'queue', [$value]);

        self::assertCount(5, $http->requests);
        self::assertSame('/api/runtime/v1/namespaces/fixture/api/cluster/info', $http->requests[0]->getUri()->getPath());
        foreach ($http->requests as $request) {
            self::assertSame('tenant-one', $request->getHeaderLine('X-Namespace'));
            self::assertSame('Bearer fixture-client', $request->getHeaderLine('Authorization'));
        }
        $upload = $http->requests[1];
        self::assertSame($blob, (string) $upload->getBody());
        self::assertSame('application/octet-stream', $upload->getHeaderLine('Content-Type'));
        self::assertSame((string) strlen($blob), $upload->getHeaderLine('X-Durable-Workflow-Payload-Size'));
        self::assertSame(hash('sha256', $blob), $upload->getHeaderLine('X-Durable-Workflow-Payload-SHA256'));
        $first = $this->body($http->requests[2]);
        self::assertSame(['codec' => 'avro', 'external_payload' => self::reference($blob)], $first['input']);
        self::assertSame($first['input'], $this->body($http->requests[4])['input']);
    }

    public function testWorkerOnlyCredentialSupportsDiscoveryAndDeduplicatedCommandUploads(): void
    {
        [$client, $http] = $this->client(workerOnly: true);
        $blob = $client->payloadCodec()->encode([str_repeat('large', 100)]);
        $commands = [];
        foreach (RuntimePayloadUploads::COMMAND_FIELDS as $type => $fields) {
            foreach ($fields as $field) {
                $commands[] = ['type' => $type, $field => ['codec' => 'avro', 'blob' => $blob]];
            }
        }
        $commands[] = ['type' => 'record_side_effect', 'result' => $blob];
        $client->completeWorkflowTask('task', 'worker', 1, $commands);

        self::assertCount(3, $http->requests);
        self::assertSame('2', $http->requests[0]->getHeaderLine('X-Durable-Workflow-Control-Plane-Version'));
        self::assertSame('', $http->requests[0]->getHeaderLine('X-Durable-Workflow-Protocol-Version'));
        foreach ($http->requests as $request) {
            self::assertSame('Bearer fixture-worker', $request->getHeaderLine('Authorization'));
        }
        $sent = $this->body($http->requests[2])['commands'];
        foreach ($sent as $command) {
            $field = array_keys($command)[1];
            self::assertSame(self::reference($blob), $command[$field]['external_payload']);
        }
        self::assertSame($blob, $commands[0]['result']['blob'], 'Planning must not mutate caller-owned commands.');
    }

    public function testDiscoveryIsSeparateForNamespacesAndRoles(): void
    {
        [$client, $http] = $this->client();
        $client->startWorkflow('echo', 'one', 'queue');
        $client->completeActivityTask('task', 'attempt', 'worker', 'small');
        $client->withNamespace('tenant-two')->startWorkflow('echo', 'two', 'queue');
        $discovery = array_values(array_filter($http->requests,
            static fn (RequestInterface $r): bool => str_ends_with($r->getUri()->getPath(), '/cluster/info')));
        self::assertCount(3, $discovery);
        self::assertSame(['tenant-one', 'tenant-one', 'tenant-two'], array_map(static fn ($r) => $r->getHeaderLine('X-Namespace'), $discovery));
        self::assertSame(['Bearer fixture-client', 'Bearer fixture-worker', 'Bearer fixture-client'], array_map(static fn ($r) => $r->getHeaderLine('Authorization'), $discovery));
    }

    public function testAggregateInlineBatchIsOffloadedBeforeOrdinaryRequestLimit(): void
    {
        $policy = self::discovery();
        $policy['namespace']['external_payload_storage']['threshold_bytes'] = 1500;
        $policy['limits']['max_payload_bytes'] = 1100;
        [$client, $http] = $this->client($policy);
        $blob = $client->payloadCodec()->encode(str_repeat('a', 600));
        $client->completeWorkflowTask('task', 'worker', 1, [
            ['type' => 'record_side_effect', 'result' => $blob],
            ['type' => 'complete_workflow', 'result' => ['codec' => 'avro', 'blob' => $blob]],
        ]);
        self::assertCount(3, $http->requests);
        self::assertLessThanOrEqual(1100, strlen((string) $http->requests[2]->getBody()));
    }

    public function testOversizedMetadataIsRejectedBeforeAnyUpload(): void
    {
        $policy = self::discovery();
        $policy['limits']['max_payload_bytes'] = 500;
        [$client, $http] = $this->client($policy);
        try {
            $client->startWorkflow('echo', 'one', 'queue', [str_repeat('large', 100)], memo: ['note' => str_repeat('x', 600)]);
            self::fail('Expected metadata limit failure.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('payload_too_large', $exception->reason);
        }
        self::assertCount(1, $http->requests);
    }

    public function testExactThresholdRemainsInlineAndOverLimitNeverUploads(): void
    {
        $blob = (new AvroPayloadCodec())->encode(['limit']);
        $policy = self::discovery();
        $policy['namespace']['external_payload_storage']['threshold_bytes'] = strlen($blob);
        $policy['namespace']['external_payload_storage']['transport']['limits']['max_payload_bytes'] = strlen($blob);
        [$client, $http] = $this->client($policy);
        $client->startWorkflow('echo', 'one', 'queue', ['limit']);
        self::assertSame($blob, $this->body($http->requests[1])['input']['blob']);
        try {
            $client->startWorkflow('echo', 'two', 'queue', [str_repeat('larger', 10)]);
            self::fail('Expected encoded size limit failure.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_oversized', $exception->reason);
        }
        self::assertCount(2, $http->requests);
    }

    public function testUnavailableStorageStillAllowsSmallRequests(): void
    {
        $policy = self::discovery();
        $policy['namespace']['external_payload_storage']['status'] = 'disabled';
        [$client, $http] = $this->client($policy);
        $client->startWorkflow('echo', 'one', 'queue');
        try {
            $client->startWorkflow('echo', 'two', 'queue', [str_repeat('x', 200)]);
            self::fail('Expected unavailable storage failure.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_unavailable', $exception->reason);
        }
        self::assertCount(2, $http->requests);
    }

    public function testCodecLookingApplicationDataIsNeverInterpreted(): void
    {
        [$client, $http] = $this->client();
        $metadata = ['codec' => 'avro', 'blob' => str_repeat('x', 300)];
        $client->startWorkflow('echo', 'one', 'queue', [$metadata], memo: $metadata);
        $body = $this->body($http->requests[2]);
        self::assertSame($metadata, $body['memo']);
        self::assertSame([$metadata], $client->payloadCodec()->decode((string) $http->requests[1]->getBody()));
        $client->heartbeatActivityTask('task', 'attempt', 'worker', $metadata);
        self::assertSame($metadata, $this->body($http->requests[3])['details']);
    }

    #[DataProvider('payloadRequestProvider')]
    public function testOtherPayloadRequestsUseRuntimeUploads(string $method, string $path, array $body, bool $worker): void
    {
        [, $http, $transport] = $this->client();
        $prepared = (new RuntimePayloadUploads($transport, 'https://runtime.test'))->request($body, $method, $path, $worker, []);
        self::assertCount(2, $http->requests);
        self::assertStringContainsString('external_payload', json_encode($prepared));
    }

    public static function payloadRequestProvider(): iterable
    {
        $value = (new AvroPayloadCodec())->envelope([str_repeat('x', 300)]);
        foreach (['/workflows/id/signal/event', '/workflows/id/runs/run/query/value', '/workflows/id/update/change',
            '/workflows/id/message-streams/inbox/messages', '/activities'] as $path) {
            yield $path => ['POST', $path, ['input' => $value], false];
        }
        yield 'schedule create' => ['POST', '/schedules', ['action' => ['input' => $value]], false];
        yield 'schedule update' => ['PATCH', '/schedules/id', ['action' => ['input' => $value]], false];
        yield 'service operation' => ['POST', '/service-endpoints/e/services/s/operations/o/execute', ['arguments' => $value['blob']], false];
        yield 'activity result' => ['POST', '/worker/activity-tasks/task/complete', ['result' => $value], true];
        yield 'activity failure' => ['POST', '/worker/activity-tasks/task/fail', ['failure' => ['details' => $value]], true];
        yield 'workflow failure' => ['POST', '/worker/workflow-tasks/task/complete', ['commands' => [['type' => 'fail_workflow', 'exception' => ['details' => $value]]]], true];
    }

    public function testExternalQueryResultDoesNotSendDuplicateRawProjection(): void
    {
        [$client, $http] = $this->client();
        $client->completeQueryTask('task', 'worker', 1, str_repeat('large', 100));
        self::assertNull($this->body($http->requests[2])['result']);
        self::assertArrayHasKey('external_payload', $this->body($http->requests[2])['result_envelope']);
    }

    public function testStreamPayloadBecomesReferenceFieldNotNestedEnvelope(): void
    {
        [$client, $http] = $this->client();
        $client->appendWorkflowStream('wf', 'run', 'stream', [new WorkflowStreamAppendItem(str_repeat('x', 200), idempotencyKey: 'one')]);
        $item = $this->body($http->requests[2])['items'][0];
        self::assertArrayNotHasKey('payload', $item);
        self::assertSame('one', $item['idempotency_key']);
        self::assertSame(RuntimePayloads::SCHEMA, $item['payload_reference']['schema']);
    }

    public function testRuntimeOwnedLowLevelEnvelopeIsAcceptedAndStrictlyValidated(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('https://server.example', transport: $transport);
        $ref = self::reference($client->payloadCodec()->encode('value'));
        $command = ['type' => 'complete_workflow', 'result' => ['codec' => 'avro', 'external_payload' => $ref]];
        $client->completeWorkflowTask('task', 'worker', 1, [$command]);
        self::assertSame($command, $transport->requests[0]['body']['commands'][0]);
        $command['result']['external_payload']['uri'] = 'https://unexpected.example';
        $this->expectException(CodecException::class);
        $client->completeWorkflowTask('task', 'worker', 1, [$command]);
    }

    #[DataProvider('invalidResponseProvider')]
    public function testInvalidUploadResponsePreventsCompletion(array $response, string $reason): void
    {
        [$client, $http] = $this->client(uploadResponse: new Response(201, [], json_encode($response)));
        try {
            $client->completeActivityTask('task', 'attempt', 'worker', str_repeat('x', 200));
            self::fail('Expected upload response validation failure.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame($reason, $exception->reason);
        }
        self::assertCount(2, $http->requests);
    }

    public static function invalidResponseProvider(): iterable
    {
        $blob = (new AvroPayloadCodec())->encode(str_repeat('x', 200));
        $response = ['schema' => 'durable-workflow.v2.runtime-external-payload-upload.v1', 'version' => 1, 'reference' => self::reference($blob)];
        yield 'wrong version' => [array_replace($response, ['version' => 2]), 'external_payload_unsupported'];
        foreach (['size_bytes' => 1, 'sha256' => str_repeat('0', 64), 'reference_id' => 'https://outside.example', 'codec' => 'json'] as $field => $value) {
            $changed = $response;
            $changed['reference'][$field] = $value;
            yield $field => [$changed, in_array($field, ['size_bytes', 'sha256'], true) ? 'external_payload_integrity_mismatch' : 'external_payload_unsupported'];
        }
    }

    public function testAdmissionFailurePreservesIdentityAndReasonOnRetry(): void
    {
        [$client, $http] = $this->client(uploadResponse: new Response(429, [], '{"reason":"external_payload_namespace_bytes_exhausted","retryable":true}'));
        for ($attempt = 0; $attempt < 2; ++$attempt) {
            try {
                $client->completeActivityTask('task', 'attempt', 'worker', str_repeat('x', 200));
                self::fail('Expected admission failure.');
            } catch (ServerException $exception) {
                self::assertSame(429, $exception->status);
                self::assertSame('external_payload_namespace_bytes_exhausted', $exception->reason);
                self::assertTrue($exception->details['retryable']);
            }
        }
        self::assertCount(3, $http->requests);
        self::assertSame((string) $http->requests[1]->getBody(), (string) $http->requests[2]->getBody());
        self::assertSame($http->requests[1]->getHeaders(), $http->requests[2]->getHeaders());
    }

    public function testDiscoveryCannotRedirectUploadToAnotherHost(): void
    {
        $policy = self::discovery();
        $policy['namespace']['external_payload_storage']['transport']['upload']['path'] = 'https://unexpected.example';
        [$client, $http] = $this->client($policy);
        try {
            $client->startWorkflow('echo', 'one', 'queue', [str_repeat('x', 200)]);
            self::fail('Expected invalid discovery failure.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_unsupported', $exception->reason);
        }
        self::assertCount(1, $http->requests);
    }

    public function testUploadRedirectIsNotFollowed(): void
    {
        [$client, $http] = $this->client(uploadResponse: new Response(307, ['Location' => 'https://unexpected.example'], '{}'));
        try {
            $client->startWorkflow('echo', 'one', 'queue', [str_repeat('x', 200)]);
            self::fail('Expected redirect rejection.');
        } catch (ServerException $exception) {
            self::assertSame(307, $exception->status);
        }
        self::assertCount(2, $http->requests);
        self::assertFalse($http->options[1]['allow_redirects']);
        self::assertSame(45, $http->options[1]['timeout']);
    }

    private function client(?array $discovery = null, bool $workerOnly = false, ?Response $uploadResponse = null): array
    {
        $http = new class($discovery ?? self::discovery(), $uploadResponse) {
            public array $requests = [];
            public array $options = [];
            private string $uploadBody;
            public function __construct(private array $discovery, private ?Response $uploadResponse)
            {
                $this->uploadBody = (string) $uploadResponse?->getBody();
            }
            public function __invoke(RequestInterface $request, array $options): \GuzzleHttp\Promise\PromiseInterface
            {
                $this->requests[] = $request;
                $this->options[] = $options;
                $path = $request->getUri()->getPath();
                if (str_ends_with($path, '/cluster/info')) {
                    $response = $this->discovery;
                } elseif (str_ends_with($path, '/external-payloads/v1')) {
                    if ($this->uploadResponse !== null) {
                        return Create::promiseFor(new Response($this->uploadResponse->getStatusCode(), $this->uploadResponse->getHeaders(), $this->uploadBody));
                    }
                    $response = ['schema' => 'durable-workflow.v2.runtime-external-payload-upload.v1', 'version' => 1,
                        'reference' => RuntimePayloadUploadTest::reference((string) $request->getBody())];
                } else {
                    $response = ['outcome' => 'completed'];
                }

                return Create::promiseFor(new Response(200, [], json_encode($response, JSON_THROW_ON_ERROR)));
            }
        };
        $transport = new Psr18Transport(new GuzzleClient(['handler' => $http]));
        $client = new Client('https://runtime.test/api/runtime/v1/namespaces/fixture', namespace: 'tenant-one',
            transport: $transport,
            controlToken: $workerOnly ? null : 'fixture-client', workerToken: 'fixture-worker');

        return [$client, $http, $transport];
    }

    private static function discovery(): array
    {
        return ['limits' => ['max_payload_bytes' => 4096], 'namespace' => ['external_payload_storage' => [
            'status' => 'available', 'threshold_bytes' => 128,
            'transport' => ['schema' => 'durable-workflow.v2.runtime-external-payload-transport.v1', 'version' => 1,
                'reference_schema' => RuntimePayloads::SCHEMA, 'mode' => 'authenticated_namespace_runtime',
                'upload' => ['method' => 'POST', 'path' => '/api/external-payloads/v1'],
                'fetch' => ['method' => 'GET', 'path_template' => '/api/external-payloads/v1/{referenceId}'],
                'limits' => ['max_payload_bytes' => 16384, 'request_timeout_seconds' => 45]],
        ]]];
    }

    public static function reference(string $blob): array
    {
        return ['schema' => RuntimePayloads::SCHEMA, 'reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
            'codec' => 'avro', 'size_bytes' => strlen($blob), 'sha256' => hash('sha256', $blob)];
    }

    private function body(RequestInterface $request): array
    {
        return json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
    }
}
