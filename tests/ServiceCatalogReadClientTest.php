<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RuntimePayloads;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class ServiceCatalogReadClientTest extends TestCase
{
    #[DataProvider('readMethods')]
    public function testReadsUseDiscoveredContractAndScopedControlPlane(string $method, string $path, bool $detail): void
    {
        $surface = ['data' => [['id' => 'call-1']], 'call_deadline_at' => '2026-10-11T19:00:01.123456Z',
            'failure_type' => 'ServiceCallTimedOut', 'input_payload_reference' => ['reference_id' => 'opaque-1']];
        $transport = new FakeTransport([$this->discovery(), $surface]);
        $client = new Client('https://server.example', token: 'operator-token', namespace: 'tenant-a', transport: $transport);
        $result = $detail ? $client->{$method}('call/1') : $client->{$method}(['page' => 2, 'per_page' => 10]);

        self::assertSame($surface, $result);
        self::assertCount(2, $transport->requests);
        self::assertSame('https://server.example/api/cluster/info', $transport->requests[0]['uri']);
        self::assertSame('https://server.example/api/service-catalog/'.$path.($detail ? '/call%2F1' : '?page=2&per_page=10'), $transport->requests[1]['uri']);
        foreach ($transport->requests as $request) {
            self::assertSame('GET', $request['method']);
            self::assertNull($request['body']);
            self::assertSame('tenant-a', $request['headers']['X-Namespace']);
            self::assertSame('2', $request['headers']['X-Durable-Workflow-Control-Plane-Version']);
            self::assertSame('Bearer operator-token', $request['headers']['Authorization']);
        }
    }

    public static function readMethods(): array
    {
        return [
            ['listServiceCatalogEndpoints', 'endpoints', false], ['describeServiceCatalogEndpoint', 'endpoints', true],
            ['listServiceCatalogServices', 'services', false], ['describeServiceCatalogService', 'services', true],
            ['listServiceCatalogOperations', 'operations', false], ['describeServiceCatalogOperation', 'operations', true],
            ['listServiceCatalogCalls', 'calls', false], ['describeServiceCatalogCall', 'calls', true],
        ];
    }

    public function testDescribeRejectsBlankIdentitiesBeforeDiscovery(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        foreach (self::readMethods() as [$method, , $detail]) {
            if (!$detail) {
                continue;
            }
            foreach (['', ' '] as $id) {
                try {
                    $client->{$method}($id);
                    self::fail('Blank identity must not turn a detail lookup into a collection read.');
                } catch (\InvalidArgumentException $exception) {
                    self::assertStringContainsString('non-empty ID', $exception->getMessage());
                    self::assertCount(0, $transport->requests);
                }
            }
        }
    }

    #[DataProvider('unsupportedContracts')]
    public function testUnsupportedBackendIsExplainedBeforeAttemptingARead(mixed $contract): void
    {
        $transport = new FakeTransport([['service_catalog_read_contract' => $contract]]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->listServiceCatalogCalls();
            self::fail('Missing or unsupported read capability must be diagnosed.');
        } catch (ServerException $exception) {
            self::assertSame(501, $exception->status);
            self::assertSame('backend_capability_unavailable', $exception->reason);
            self::assertSame('service_catalog_reads', $exception->details['capability']);
            self::assertSame(1, $exception->details['required_contract_version']);
            self::assertSame($contract, $exception->details['advertised_contract']);
            self::assertCount(1, $transport->requests);
        }
    }

    public static function unsupportedContracts(): array
    {
        return [[null], ['legacy'], [[]], [['schema' => 'other', 'version' => 1]],
            [['schema' => 'durable-workflow.v2.service-catalog-read.contract', 'version' => 2]],
            [['schema' => 'durable-workflow.v2.service-catalog-read.contract', 'version' => '1']]];
    }

    public function testActualReadRefusalsRetainTheServerReasonAndDiscoveryFailuresAreNotUnsupported(): void
    {
        $transport = new FakeTransport([$this->discovery(), new TransportException('Forbidden', 403, ['reason' => 'forbidden'])]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->describeServiceCatalogCall('call-1');
            self::fail('Worker role must remain refused by the backend.');
        } catch (ServerException $exception) {
            self::assertSame(403, $exception->status);
            self::assertSame('forbidden', $exception->reason);
        }

        $transport = new FakeTransport([new TransportException('Backend unavailable', 503, ['reason' => 'backend_unavailable'])]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->listServiceCatalogCalls();
            self::fail('Discovery failure must retain the backend failure.');
        } catch (ServerException $exception) {
            self::assertSame(503, $exception->status);
            self::assertSame('backend_unavailable', $exception->reason);
            self::assertCount(1, $transport->requests);
        }
    }

    private function discovery(): array
    {
        return ['service_catalog_read_contract' => ['schema' => 'durable-workflow.v2.service-catalog-read.contract', 'version' => 1]];
    }

    #[DataProvider('outputs')]
    public function testOwnedTerminalOutputIsDecodedWithoutReplacingTheOpaqueReference(mixed $value): void
    {
        $codec = new AvroPayloadCodec();
        $envelope = $codec->envelope($value);
        $transport = new FakeTransport([$this->discovery(), [
            'id' => 'call-1', 'payloads_in_observer_namespace' => true,
            'output_payload_reference' => $envelope,
        ]]);
        $result = (new Client('https://runtime.test', transport: $transport))->describeServiceCatalogCall('call-1');
        self::assertTrue($result['output_available']);
        self::assertSame($envelope, $result['output_payload_reference']);
        self::assertSame($codec->encode($value), $codec->encode($result['output']));
    }

    public static function outputs(): array
    {
        return [[null], [false], [0], [''], [['result', ['nested' => 'value']]]];
    }

    public function testExternalOutputUsesOnlyTheConfiguredRuntimeNamespaceAndOperatorCredential(): void
    {
        $codec = new AvroPayloadCodec();
        $value = ['result', str_repeat('x', 10000)];
        $blob = $codec->envelope($value)['blob'];
        $reference = ['schema' => RuntimePayloads::SCHEMA, 'reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
            'codec' => 'avro', 'size_bytes' => strlen($blob), 'sha256' => hash('sha256', $blob)];
        $envelope = ['codec' => 'avro', 'external_payload' => $reference];
        $requests = [];
        $http = new GuzzleClient(['handler' => function (RequestInterface $request, array $options) use (&$requests, $blob, $envelope) {
            $requests[] = $request;
            self::assertFalse($options['allow_redirects']);
            if (count($requests) === 1) {
                return Create::promiseFor(new Response(200, [], json_encode($this->discovery(), JSON_THROW_ON_ERROR)));
            }
            if (count($requests) === 2) {
                return Create::promiseFor(new Response(200, [], json_encode([
                    'id' => 'call-1', 'payloads_in_observer_namespace' => true, 'output_payload_reference' => $envelope,
                ], JSON_THROW_ON_ERROR)));
            }

            return Create::promiseFor(new Response(200, [
                'Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $blob));
        }]);
        $client = new Client('https://runtime.test/api/runtime/v1/namespaces/owner-id', namespace: 'owner',
            transport: new Psr18Transport($http), controlToken: 'operator', workerToken: 'worker');
        $result = $client->describeServiceCatalogCall('call-1');
        self::assertSame($codec->encode($value), $codec->encode($result['output']));
        self::assertSame($envelope, $result['output_payload_reference']);
        self::assertCount(3, $requests);
        self::assertSame('https://runtime.test/api/runtime/v1/namespaces/owner-id/api/external-payloads/v1/'.$reference['reference_id'], (string) $requests[2]->getUri());
        self::assertSame('owner', $requests[2]->getHeaderLine('X-Namespace'));
        self::assertSame('Bearer operator', $requests[2]->getHeaderLine('Authorization'));
    }

    public function testForeignOrUnverifiedOutputsAreNotFetchedAndOwnedDownloadsHonorTheByteLimit(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['private'])['blob'];
        $reference = ['schema' => RuntimePayloads::SCHEMA, 'reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
            'codec' => 'avro', 'size_bytes' => strlen($blob), 'sha256' => hash('sha256', $blob)];
        foreach ([false, null, true] as $authorized) {
            $transport = new FakeTransport([$this->discovery(), [
                'payloads_in_observer_namespace' => $authorized,
                'output_payload_reference' => ['codec' => 'avro', 'external_payload' => $reference],
            ]]);
            $client = new Client('https://runtime.test', transport: $transport, maxExternalPayloadBytes: 1);
            try {
                $result = $client->describeServiceCatalogCall('call-1');
                self::assertNotTrue($authorized);
                self::assertArrayNotHasKey('output', $result);
            } catch (ExternalPayloadException $exception) {
                self::assertTrue($authorized);
                self::assertSame('external_payload_oversized', $exception->reason);
            }
            self::assertCount(2, $transport->requests);
        }
    }
}
