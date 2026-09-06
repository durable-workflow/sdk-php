<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\PumpStream;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class RuntimeExternalPayloadTest extends TestCase
{
    public function testRecordedOpaqueEnvelopeResolvesTheExistingAvroGoldenValue(): void
    {
        $response = json_decode(file_get_contents(__DIR__.'/fixtures/runtime-external-payload.json'), true, flags: JSON_THROW_ON_ERROR);
        $golden = json_decode(file_get_contents(__DIR__.'/fixtures/codec-regressions/avro-value-v1-long-zero.json'), true, flags: JSON_THROW_ON_ERROR);
        [$client] = $this->fixture($response, $golden['framing']['wire_base64']);

        self::assertSame(0, $client->describeWorkflow('external-payload-regression')->output);
    }

    public function testPortableValueIdentityIsPreservedAfterDownload(): void
    {
        $codec = new AvroPayloadCodec();
        $value = AvroMapValue::fromPairs([
            ['long', 7], ['double', 7.0], ['binary', AvroBinaryValue::fromBytes("\xff\x00")],
            ['text', 'same'], ['empty', AvroMapValue::fromPairs([])],
            ['nested', [true, null, AvroMapValue::fromPairs([['0', 'numeric key']])]],
        ]);
        $blob = $codec->envelope($value)['blob'];
        [$client] = $this->fixture(['output_envelope' => ['codec' => 'avro', 'external_payload' => self::reference($blob)]], $blob);

        self::assertSame($blob, $codec->encode($client->describeWorkflow('test')->output));
    }

    public function testClientResolvesOpaquePayloadFromItsOwnRuntimeAndRole(): void
    {
        $codec = new AvroPayloadCodec();
        $blob = $codec->envelope(['large-value'])['blob'];
        $reference = self::reference($blob);
        $requests = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$requests, $reference, $blob) {
            $requests[] = $request;
            self::assertFalse($options['allow_redirects']);
            if (count($requests) === 1) {
                return Create::promiseFor(new Response(200, [], json_encode([
                    'workflow_id' => 'test', 'status' => 'completed',
                    'input_envelope' => ['codec' => 'avro', 'external_payload' => $reference],
                ], JSON_THROW_ON_ERROR)));
            }

            return Create::promiseFor(new Response(200, [
                'Content-Type' => 'application/octet-stream',
                'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $blob));
        }]);
        $client = new Client('https://runtime.test/api/runtime/v1/namespaces/test-id', namespace: 'test',
            transport: new Psr18Transport($http), controlToken: 'fixture-client', workerToken: 'fixture-worker');

        self::assertSame(['large-value'], $client->describeWorkflow('test')->input);
        self::assertCount(2, $requests);
        self::assertSame('https://runtime.test/api/runtime/v1/namespaces/test-id/api/external-payloads/v1/'.$reference['reference_id'], (string) $requests[1]->getUri());
        self::assertSame('Bearer fixture-client', $requests[1]->getHeaderLine('Authorization'));
        self::assertSame('test', $requests[1]->getHeaderLine('X-Namespace'));
        self::assertSame('2', $requests[1]->getHeaderLine('X-Durable-Workflow-Control-Plane-Version'));
        self::assertSame('application/octet-stream', $requests[1]->getHeaderLine('Accept'));
    }

    /** @return array{schema: string, reference_id: string, codec: string, size_bytes: int, sha256: string} */
    public static function reference(string $blob): array
    {
        return ['schema' => 'durable-workflow.v2.runtime-external-payload-reference.v1',
            'reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'codec' => 'avro',
            'size_bytes' => strlen($blob), 'sha256' => hash('sha256', $blob)];
    }

    public function testWorkerPollsHydrateArgumentsAndHistoryUsingOnlyWorkerCredentials(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['portable'])['blob'];
        $envelope = ['codec' => 'avro', 'external_payload' => self::reference($blob)];
        foreach (['pollWorkflowTaskResponse', 'pollActivityTaskResponse', 'pollQueryTaskResponse'] as $method) {
            [$client, $observed] = $this->fixture(['task' => [
                'arguments' => $envelope, 'workflow_arguments' => $envelope, 'query_arguments' => $envelope,
                'history_events' => [['payload' => ['activity' => ['result' => $envelope]]]],
                'history_export' => ['payloads' => ['arguments' => ['data' => $envelope]]],
            ]], $blob);
            $result = $client->$method('worker', 'queue', 0);
            self::assertSame(['portable'], $client->payloadCodec()->decodeEnvelope($result['task']['arguments']));
            self::assertSame(['codec' => 'avro', 'blob' => $blob], $result['task']['history_events'][0]['payload']['activity']['result']);
            self::assertSame(['codec' => 'avro', 'blob' => $blob], $result['task']['history_export']['payloads']['arguments']['data']);
            self::assertCount(2, $observed->requests, 'Repeated references in one response should fetch once.');
            self::assertSame('Bearer fixture-worker', $observed->requests[1]->getHeaderLine('Authorization'));
            self::assertSame('1.19', $observed->requests[1]->getHeaderLine('X-Durable-Workflow-Protocol-Version'));
            self::assertSame('', $observed->requests[1]->getHeaderLine('X-Durable-Workflow-Control-Plane-Version'));
        }
    }

    public function testHistoryAndExportResolveOnlyEnvelopePositionsNotUserProjections(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $envelope = ['codec' => 'avro', 'external_payload' => self::reference($blob)];
        [$client] = $this->fixture(['events' => [['payload' => [
            'arguments' => $envelope, 'result' => $envelope, 'output' => $envelope,
            'command' => ['payload' => $envelope], 'exception' => ['details' => $envelope],
        ]]], 'memo' => $envelope], $blob);
        $history = $client->workflowHistory('workflow', 'run');
        self::assertSame(['codec' => 'avro', 'blob' => $blob], $history['events'][0]['payload']['exception']['details']);
        self::assertSame($envelope, $history['memo']);

        [$client] = $this->fixture(['payloads' => ['arguments' => ['data' => $envelope]],
            'signals' => [['arguments' => $envelope]], 'timeline' => [['command' => ['payload' => $envelope]]]], $blob);
        $export = $client->exportWorkflowHistory('workflow', 'run');
        self::assertSame(['codec' => 'avro', 'blob' => $blob], $export['payloads']['arguments']['data']);
        self::assertSame(['codec' => 'avro', 'blob' => $blob], $export['signals'][0]['arguments']);

        [$client, $observed] = $this->fixture(['workflow_id' => 'test', 'input' => $envelope, 'output' => $envelope], $blob);
        self::assertSame($envelope, $client->describeWorkflow('test')->input);
        self::assertCount(1, $observed->requests);
    }

    public function testInvalidReferencesAndByteLimitsFailBeforeAnyFetch(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $reference = self::reference($blob);
        foreach ([['reference_id' => 'https://other.test/payload'], ['reference_id' => '../secret'],
            ['schema' => 'unknown'], ['codec' => 'json'], ['size_bytes' => '10'], ['size_bytes' => -1],
            ['sha256' => str_repeat('x', 64)], ['provider_key' => 'never-read'],
        ] as $change) {
            [$client, $observed] = $this->fixture(['input_envelope' => ['codec' => 'avro', 'external_payload' => array_replace($reference, $change)]], $blob);
            try {
                $client->describeWorkflow('test');
                self::fail('Invalid reference was accepted.');
            } catch (ExternalPayloadException $exception) {
                self::assertSame('external_payload_unsupported', $exception->reason);
                self::assertCount(1, $observed->requests);
            }
        }
        [$client, $observed] = $this->fixture(['input_envelope' => ['codec' => 'avro', 'external_payload' => $reference]], $blob, limit: 1);
        try {
            $client->withNamespace('other')->describeWorkflow('test');
            self::fail('Namespace copy discarded the configured byte limit.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_oversized', $exception->reason);
            self::assertCount(1, $observed->requests);
            self::assertSame('other', $observed->requests[0]->getHeaderLine('X-Namespace'));
        }
    }

    public function testDistinctReferencesShareOneResponseByteBudget(): void
    {
        $blob = (new AvroPayloadCodec())->envelope('value')['blob'];
        $first = self::reference($blob);
        $second = array_replace($first, ['reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAW']);
        [$client, $observed] = $this->fixture([
            'input_envelope' => ['codec' => 'avro', 'external_payload' => $first],
            'output_envelope' => ['codec' => 'avro', 'external_payload' => $second],
        ], $blob, limit: strlen($blob));

        try {
            $client->describeWorkflow('test');
            self::fail('Each download was incorrectly given a separate byte budget.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_oversized', $exception->reason);
            self::assertCount(2, $observed->requests);
        }
    }

    public function testDownloadsAreNotCachedAcrossNamespaceOrRoleBoundaries(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $envelope = ['codec' => 'avro', 'external_payload' => self::reference($blob)];
        $downloads = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use (&$downloads, $envelope, $blob) {
            if (!str_contains((string) $request->getUri(), '/external-payloads/')) {
                return Create::promiseFor(new Response(200, [], json_encode([
                    'input_envelope' => $envelope, 'task' => ['arguments' => $envelope],
                ], JSON_THROW_ON_ERROR)));
            }
            $downloads[] = [$request->getHeaderLine('X-Namespace'), $request->getHeaderLine('Authorization')];

            return Create::promiseFor(new Response(200, [
                'Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob), 'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $blob));
        }]);
        $client = new Client('https://runtime.test', namespace: 'first', transport: new Psr18Transport($http),
            controlToken: 'fixture-client', workerToken: 'fixture-worker');
        $client->describeWorkflow('test');
        $client->describeWorkflow('test');
        $client->withNamespace('second')->describeWorkflow('test');
        $client->pollWorkflowTaskResponse('worker', 'queue', 0);

        self::assertSame([
            ['first', 'Bearer fixture-client'], ['first', 'Bearer fixture-client'],
            ['second', 'Bearer fixture-client'], ['first', 'Bearer fixture-worker'],
        ], $downloads);
    }

    public function testOversizedStreamIsStoppedAfterDeclaredSizeAndClosed(): void
    {
        $read = 0;
        $stream = new PumpStream(static function (int $length) use (&$read): string {
            $read += $length;

            return str_repeat('x', $length);
        });
        $headers = ['Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
            'X-Durable-Workflow-Payload-Size' => '10', 'X-Durable-Workflow-Payload-SHA256' => str_repeat('0', 64)];
        $transport = new Psr18Transport(new GuzzleClient(['handler' => static fn () => Create::promiseFor(new Response(200, $headers, $stream))]));

        try {
            $transport->fetchPayload('https://runtime.test/api/external-payloads/v1/fixture', $headers, 10);
            self::fail('Unbounded stream was read.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_integrity_mismatch', $exception->reason);
            self::assertSame(11, $read);
            self::assertTrue($stream->eof());
        }
    }

    public function testCorruptTruncatedOversizedAndMismatchedResponsesFailBeforeAvroDecode(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $envelope = ['codec' => 'avro', 'external_payload' => self::reference($blob)];
        foreach (['corrupt', 'truncated', 'oversized', 'metadata'] as $case) {
            $bytes = match ($case) {
                'corrupt' => str_repeat('x', strlen($blob)),
                'truncated' => substr($blob, 0, -1),
                'oversized' => $blob.'extra',
                default => $blob,
            };
            [$client] = $this->fixture(['input_envelope' => $envelope], $blob, bytes: $bytes,
                responseHeaders: $case === 'metadata' ? ['X-Durable-Workflow-Payload-SHA256' => str_repeat('0', 64)] : []);
            try {
                $client->describeWorkflow('test');
                self::fail('Invalid payload response was accepted.');
            } catch (ExternalPayloadException $exception) {
                self::assertSame('external_payload_integrity_mismatch', $exception->reason);
            }
        }
    }

    public function testHttpFailuresAndRedirectsDoNotLeakCredentialsOrBecomeAvroErrors(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        foreach ([302 => 'external_payload_unavailable', 401 => 'external_payload_unauthorized', 403 => 'external_payload_unauthorized',
            404 => 'external_payload_not_found', 410 => 'external_payload_expired', 503 => 'external_payload_unavailable'] as $status => $reason) {
            [$client, $observed] = $this->fixture(['input_envelope' => ['codec' => 'avro', 'external_payload' => self::reference($blob)]],
                $blob, status: $status, responseHeaders: ['Location' => 'https://other.test/steal']);
            try {
                $client->describeWorkflow('test');
                self::fail('Failed payload request was accepted.');
            } catch (ExternalPayloadException $exception) {
                self::assertSame($status, $exception->status);
                self::assertSame($reason, $exception->reason);
                self::assertCount(2, $observed->requests);
            }
        }
    }

    public function testJsonOnlyCustomTransportFailsClearlyWithoutProviderFallback(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $transport = new FakeTransport([['input_envelope' => ['codec' => 'avro', 'external_payload' => self::reference($blob)]]]);
        try {
            (new Client('https://runtime.test', transport: $transport))->describeWorkflow('test');
            self::fail('A JSON-only transport cannot fetch binary payloads.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame('external_payload_unsupported', $exception->reason);
            self::assertStringContainsString('PayloadTransport', $exception->getMessage());
            self::assertCount(1, $transport->requests);
        }
    }

    /**
     * @param array<string, mixed> $response
     * @param array<string, string> $responseHeaders
     * @return array{Client, \stdClass}
     */
    private function fixture(array $response, string $blob, ?string $bytes = null, int $status = 200, array $responseHeaders = [], int $limit = 67108864): array
    {
        $observed = new \stdClass();
        $observed->requests = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use ($observed, $response, $blob, $bytes, $status, $responseHeaders) {
            $observed->requests[] = $request;
            self::assertFalse($options['allow_redirects']);
            if (count($observed->requests) === 1) {
                return Create::promiseFor(new Response(200, [], json_encode($response, JSON_THROW_ON_ERROR)));
            }
            self::assertTrue($options['stream']);

            return Create::promiseFor(new Response($status, array_replace([
                'Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob), 'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $responseHeaders), $bytes ?? $blob));
        }]);

        return [new Client('https://runtime.test', namespace: 'test', transport: new Psr18Transport($http),
            controlToken: 'fixture-client', workerToken: 'fixture-worker', maxExternalPayloadBytes: $limit), $observed];
    }
}
