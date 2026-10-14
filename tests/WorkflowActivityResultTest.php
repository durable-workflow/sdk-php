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
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class WorkflowActivityResultTest extends TestCase
{
    public function testItReadsOnlyTheSelectedActivityWithTheControlRole(): void
    {
        $envelope = (new AvroPayloadCodec())->envelope([9007199254740993, null, false, '雪']);
        $response = ['workflow_id' => 'workflow/1', 'run_id' => 'run/2', 'activity_id' => 'activity/3',
            'state' => 'available', 'available' => true, 'history_sequence' => 7, 'result_envelope' => $envelope];
        $transport = new FakeTransport([$response]);
        $client = new Client('https://runtime.test', transport: $transport, namespace: 'tenant-a',
            controlToken: 'operator-test', workerToken: 'worker-test');

        self::assertSame($response, $client->workflowActivityResult('workflow/1', 'run/2', 'activity/3'));
        self::assertSame([9007199254740993, null, false, '雪'], $client->payloadCodec()->decodeEnvelope($envelope));
        self::assertCount(1, $transport->requests);
        self::assertSame('GET', $transport->requests[0]['method']);
        self::assertSame('https://runtime.test/api/workflows/workflow%2F1/runs/run%2F2/activities/activity%2F3/result',
            $transport->requests[0]['uri']);
        self::assertSame('tenant-a', $transport->requests[0]['headers']['X-Namespace']);
        self::assertSame('Bearer operator-test', $transport->requests[0]['headers']['Authorization']);
        self::assertSame('2', $transport->requests[0]['headers']['X-Durable-Workflow-Control-Plane-Version']);
        self::assertArrayNotHasKey('X-Durable-Workflow-Protocol-Version', $transport->requests[0]['headers']);
    }

    public function testItPreservesUnavailableStatesWithoutInventingNullResults(): void
    {
        foreach (['pending', 'failed', 'cancelled', 'pruned', 'unsupported', 'unavailable'] as $state) {
            $response = ['state' => $state, 'available' => false, 'reason' => 'fixture_'.$state];
            $transport = new FakeTransport([$response]);
            $client = new Client('https://runtime.test', transport: $transport);
            self::assertSame($response, $client->workflowActivityResult('workflow', 'run', 'activity'));
            self::assertCount(1, $transport->requests);
        }
        $codec = new AvroPayloadCodec();
        $response = ['state' => 'available', 'available' => true, 'result_envelope' => $codec->envelope(null)];
        $client = new Client('https://runtime.test', transport: new FakeTransport([$response]));
        $result = $client->workflowActivityResult('workflow', 'run', 'activity');
        self::assertTrue($result['available']);
        self::assertNull($codec->decodeEnvelope($result['result_envelope']));
    }

    public function testServerRefusalsAreNotReplacedByHistoryOrAnotherRun(): void
    {
        $details = ['reason' => 'activity_not_found', 'message' => 'Activity not found in this run.'];
        $transport = new FakeTransport([new TransportException('Not found', 404, $details)]);
        $client = new Client('https://runtime.test', transport: $transport);
        try {
            $client->workflowActivityResult('workflow', 'run', 'activity');
            self::fail('The exact activity refusal must reach the caller.');
        } catch (ServerException $exception) {
            self::assertSame(404, $exception->status);
            self::assertSame('activity_not_found', $exception->reason);
            self::assertSame($details, $exception->details);
        }
        self::assertCount(1, $transport->requests);
    }

    public function testItFetchesOnlyTheSelectedAuthenticatedExternalEnvelope(): void
    {
        $codec = new AvroPayloadCodec();
        $blob = $codec->envelope([9007199254740993, null, '雪'])['blob'];
        $reference = RuntimeExternalPayloadTest::reference($blob);
        $requests = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$requests, $reference, $blob) {
            $requests[] = $request;
            self::assertFalse($options['allow_redirects']);
            if (count($requests) === 1) {
                return Create::promiseFor(new Response(200, [], json_encode([
                    'state' => 'available', 'available' => true,
                    'result_envelope' => ['codec' => 'avro', 'external_payload' => $reference],
                    'diagnostics' => ['codec' => 'avro', 'external_payload' => $reference],
                ], JSON_THROW_ON_ERROR)));
            }

            return Create::promiseFor(new Response(200, [
                'Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $blob));
        }]);
        $client = new Client('https://runtime.test', namespace: 'tenant-a', controlToken: 'operator-test',
            workerToken: 'worker-test', transport: new Psr18Transport($http));
        $response = $client->workflowActivityResult('workflow', 'run', 'activity');

        self::assertSame([9007199254740993, null, '雪'], $codec->decodeEnvelope($response['result_envelope']));
        self::assertSame(['codec' => 'avro', 'external_payload' => $reference], $response['diagnostics']);
        self::assertCount(2, $requests);
        self::assertSame('https://runtime.test/api/external-payloads/v1/'.$reference['reference_id'], (string) $requests[1]->getUri());
        self::assertSame('Bearer operator-test', $requests[1]->getHeaderLine('Authorization'));
        self::assertSame('tenant-a', $requests[1]->getHeaderLine('X-Namespace'));
        self::assertSame('2', $requests[1]->getHeaderLine('X-Durable-Workflow-Control-Plane-Version'));
        self::assertSame(hash('sha256', $blob), $requests[1]->getHeaderLine('X-Durable-Workflow-Payload-SHA256'));
    }

    public function testItRejectsExternalBytesWithTheWrongAuthenticatedChecksum(): void
    {
        $blob = (new AvroPayloadCodec())->envelope('expected')['blob'];
        $reference = RuntimeExternalPayloadTest::reference($blob);
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use ($reference) {
            if (str_contains((string) $request->getUri(), '/external-payloads/')) {
                return Create::promiseFor(new Response(200, [
                    'Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
                    'X-Durable-Workflow-Payload-Size' => (string) $reference['size_bytes'],
                    'X-Durable-Workflow-Payload-SHA256' => $reference['sha256'],
                ], str_repeat('x', $reference['size_bytes'])));
            }

            return Create::promiseFor(new Response(200, [], json_encode([
                'state' => 'available', 'available' => true,
                'result_envelope' => ['codec' => 'avro', 'external_payload' => $reference],
            ], JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://runtime.test', transport: new Psr18Transport($http));
        $this->expectException(ExternalPayloadException::class);
        $this->expectExceptionMessage('Runtime payload bytes differ');
        $client->workflowActivityResult('workflow', 'run', 'activity');
    }
}
