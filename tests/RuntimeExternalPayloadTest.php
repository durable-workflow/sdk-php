<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class RuntimeExternalPayloadTest extends TestCase
{
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
}
