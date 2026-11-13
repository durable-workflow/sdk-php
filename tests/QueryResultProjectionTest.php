<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Tests\Support\CodecRegressionFixture;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class QueryResultProjectionTest extends TestCase
{
    public function testBinaryCorpusCompletesThroughTheActualJsonTransport(): void
    {
        CodecRegressionFixture::executeFile('codec-regression-v1',
            __DIR__.'/fixtures/codec-regressions/query-nested-binary-result.json');
        self::addToAssertionCount(1);
    }

    #[DataProvider('plainResults')]
    public function testJsonDisplayProjectionIsPreserved(mixed $result): void
    {
        $http = self::http();
        $client = new Client('https://query.invalid', transport: new Psr18Transport($http));
        $client->completeQueryTask('query-json', 'worker-json', 2, $result);

        self::assertCount(2, $http->requests);
        $request = $http->requests[1];
        self::assertSame('/api/worker/query-tasks/query-json/complete', $request->getUri()->getPath());
        $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
        self::assertArrayHasKey('result', $body);
        self::assertSame($result, $body['result']);
        self::assertSame('worker-json', $body['lease_owner']);
        self::assertSame(2, $body['query_task_attempt']);
        self::assertSame((new AvroPayloadCodec())->envelope($result), $body['result_envelope']);
        if ($result === -0.0) {
            self::assertSame(pack('E', $result), pack('E', $body['result']));
        }
    }

    public static function plainResults(): iterable
    {
        yield 'null' => [null];
        yield 'whole float' => [1.0];
        yield 'signed zero' => [-0.0];
        yield 'nested JSON' => [['values' => [null, true, 'λ', 7.0]]];
    }

    public function testInvalidAvroResultStillRefusesBeforeAnyHttpRequest(): void
    {
        $http = self::http();
        $client = new Client('https://query.invalid', transport: new Psr18Transport($http));
        try {
            $client->completeQueryTask('query-invalid', 'worker-invalid', 1, "\xff");
            self::fail('An invalid UTF-8 string must be wrapped explicitly as Avro bytes.');
        } catch (CodecException) {
            self::assertSame([], $http->requests);
        }
    }

    private static function http(): ClientInterface
    {
        return new class implements ClientInterface {
            public array $requests = [];
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;
                $reply = str_ends_with($request->getUri()->getPath(), '/cluster/info')
                    ? ['limits' => ['max_payload_bytes' => 2097152],
                        'namespace' => ['external_payload_storage' => ['status' => 'unconfigured']]]
                    : ['outcome' => 'completed'];

                return new Response(200, [], json_encode($reply, JSON_THROW_ON_ERROR));
            }
        };
    }
}
