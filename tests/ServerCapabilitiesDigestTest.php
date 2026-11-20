<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\ServerCapabilitiesDigest;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class ServerCapabilitiesDigestTest extends TestCase
{
    private const DIGEST = 'sha256:0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0';
    private const CAPABILITIES = ['query_tasks' => true, 'activity_lease_renewal' => true,
        'supported_workflow_task_commands' => ['schedule_activity', 'complete_workflow']];

    /** @param list<array{0: int, 1: array<string, string>, 2: string}> $responses */
    private function http(array $responses): ClientInterface
    {
        return new class ($responses) implements ClientInterface {
            /** @var list<RequestInterface> */
            public array $requests = [];

            /** @param list<array{0: int, 1: array<string, string>, 2: string}> $responses */
            public function __construct(private array $responses)
            {
            }

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;
                [$status, $headers, $body] = array_shift($this->responses);

                return new Response($status, $headers + ['Content-Type' => 'application/json'], $body);
            }
        };
    }

    private static function tagged(): array
    {
        return self::CAPABILITIES + ['digest' => self::DIGEST];
    }

    public function testAWorkerKeepsTaggedCapabilitiesAndRestoresElidedCopies(): void
    {
        $http = $this->http([
            [200, [], json_encode(['task' => null, 'server_capabilities' => self::tagged()], JSON_THROW_ON_ERROR)],
            [200, [], json_encode(['outcome' => 'completed', 'activity_poll' => ['leased' => true,
                'answer' => ['task' => ['id' => 'a'], 'server_capabilities' => self::DIGEST]],
                'server_capabilities' => self::DIGEST], JSON_THROW_ON_ERROR)],
            [409, [], json_encode(['reason' => 'lease_expired', 'server_capabilities' => self::DIGEST], JSON_THROW_ON_ERROR)],
        ]);
        $transport = new Psr18Transport(client: $http);
        $worker = ['X-Durable-Workflow-Protocol-Version' => '1.20'];

        $first = $transport->send('POST', 'https://runtime.test/api/worker/heartbeat', $worker, []);
        self::assertSame(self::CAPABILITIES, $first['server_capabilities']);
        self::assertSame('none', $http->requests[0]->getHeaderLine(ServerCapabilitiesDigest::HEADER));

        $second = $transport->send('POST', 'https://runtime.test/api/worker/workflow-tasks/t/complete', $worker, []);
        self::assertSame(self::DIGEST, $http->requests[1]->getHeaderLine(ServerCapabilitiesDigest::HEADER));
        self::assertSame(self::CAPABILITIES, $second['server_capabilities']);
        self::assertSame(self::CAPABILITIES, $second['activity_poll']['answer']['server_capabilities']);
        self::assertSame(['id' => 'a'], $second['activity_poll']['answer']['task']);

        try {
            $transport->send('POST', 'https://runtime.test/api/worker/activity-tasks/a/complete', $worker, []);
            self::fail('A 409 remains a refusal.');
        } catch (TransportException $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame(self::CAPABILITIES, $exception->response['server_capabilities']);
        }
    }

    public function testControlRequestsAndFullCapabilitiesAreUnchanged(): void
    {
        $http = $this->http([
            [200, [], json_encode(['server_capabilities' => self::CAPABILITIES], JSON_THROW_ON_ERROR)],
            [200, [], json_encode(['server_capabilities' => self::CAPABILITIES], JSON_THROW_ON_ERROR)],
        ]);
        $transport = new Psr18Transport(client: $http);
        $worker = $transport->send('POST', 'https://runtime.test/api/worker/heartbeat', ['X-Durable-Workflow-Protocol-Version' => '1.20'], []);
        self::assertSame(self::CAPABILITIES, $worker['server_capabilities']);
        $control = $transport->send('GET', 'https://runtime.test/api/workflows/x', ['X-Durable-Workflow-Control-Plane-Version' => '2']);
        self::assertSame(self::CAPABILITIES, $control['server_capabilities']);
        self::assertSame('none', $http->requests[0]->getHeaderLine(ServerCapabilitiesDigest::HEADER));
        self::assertFalse($http->requests[1]->hasHeader(ServerCapabilitiesDigest::HEADER));
        // A custom PSR-18 client is not asked to decode compressed bodies.
        self::assertFalse($http->requests[0]->hasHeader('Accept-Encoding'));
    }

    public function testAnUnknownDigestIsAProtocolFailure(): void
    {
        $http = $this->http([[200, [], json_encode(['server_capabilities' => self::DIGEST], JSON_THROW_ON_ERROR)]]);
        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('The server omitted capabilities this worker does not hold.');
        (new Psr18Transport(client: $http))->send('POST', 'https://runtime.test/api/worker/heartbeat',
            ['X-Durable-Workflow-Protocol-Version' => '1.20'], []);
    }

    public function testAGzipBodyLeftEncodedByTheClientIsDecoded(): void
    {
        $body = json_encode(['ok' => true, 'blob' => str_repeat('x', 4096)], JSON_THROW_ON_ERROR);
        $http = $this->http([[200, ['Content-Encoding' => 'gzip'], (string) gzencode($body)]]);
        $decoded = (new Psr18Transport(client: $http))->send('GET', 'https://runtime.test/api/workflows/x',
            ['X-Durable-Workflow-Control-Plane-Version' => '2']);
        self::assertSame(json_decode($body, true), $decoded);
    }

    public function testTheOwnedGuzzleClientAsksForGzip(): void
    {
        if (!extension_loaded('curl') || !extension_loaded('zlib') || !function_exists('pcntl_fork')) {
            self::markTestSkipped('The owned client decodes gzip through cURL and zlib; the peer needs pcntl.');
        }
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertNotFalse($server, $error);
        $address = stream_socket_get_name($server, false);
        $pid = pcntl_fork();
        if ($pid === 0) {
            $connection = stream_socket_accept($server, 10);
            $request = '';
            while (!str_contains($request, "\r\n\r\n")) {
                $request .= fread($connection, 8192);
            }
            $accepted = str_contains(strtolower($request), "\r\naccept-encoding: gzip\r\n");
            $body = json_encode(['gzip' => $accepted, 'blob' => str_repeat('y', 4096)], JSON_THROW_ON_ERROR);
            $headers = $accepted ? "Content-Encoding: gzip\r\n" : '';
            $payload = $accepted ? (string) gzencode($body) : $body;
            fwrite($connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{$headers}Content-Length: ".strlen($payload)."\r\nConnection: close\r\n\r\n".$payload);
            fclose($connection);
            exit(0);
        }
        try {
            $decoded = (new Psr18Transport())->send('GET', 'http://'.$address.'/api/workflows/x', ['X-Durable-Workflow-Control-Plane-Version' => '2']);
            self::assertSame(['gzip' => true, 'blob' => str_repeat('y', 4096)], $decoded);
        } finally {
            pcntl_waitpid($pid, $status);
            fclose($server);
        }
    }
}
