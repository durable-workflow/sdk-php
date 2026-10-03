<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class BoundedWorkerTransportTest extends TestCase
{
    public function testCooperativeClientBoundsWorkerIoAndAllowsTheOfferedLongPoll(): void
    {
        $calls = [];
        $http = new GuzzleClient([
            'timeout' => 42, 'read_timeout' => 42, 'connect_timeout' => 42,
            'handler' => static function (RequestInterface $request, array $options) use (&$calls) {
                $calls[] = ['timeout' => $options['timeout'], 'read_timeout' => $options['read_timeout'],
                    'connect_timeout' => $options['connect_timeout'], 'uri' => (string) $request->getUri(),
                    'namespace' => $request->getHeaderLine('X-Namespace'),
                    'authorization' => $request->getHeaderLine('Authorization')];

                return Create::promiseFor(new Response(200, [], '{"status":"idle","task":null}'));
            },
        ]);
        $original = new Client('https://server.example', namespace: 'ns-1',
            transport: new Psr18Transport($http), controlToken: 'control-test', workerToken: 'worker-test',
            workerProtocolVersion: '1.20');
        $bounded = $original->withBoundedWorkerRequests();
        $original->heartbeatWorkflowTask('task-1', 'owner-1', 1);
        $bounded->heartbeatWorkflowTask('task-1', 'owner-1', 1);
        $bounded->pollWorkflowTaskResponse('worker-1', 'queue', 60);
        $bounded->clusterInfo();

        self::assertSame([42.0, 5.0, 65.0, 42.0], array_map(static fn (array $call): float => (float) $call['timeout'], $calls));
        self::assertSame(5, $calls[1]['read_timeout']);
        self::assertSame(5, $calls[1]['connect_timeout']);
        self::assertSame('ns-1', $calls[1]['namespace']);
        self::assertSame('Bearer worker-test', $calls[1]['authorization']);
        self::assertSame('Bearer control-test', $calls[3]['authorization']);
        self::assertStringEndsWith('/worker/workflow-tasks/task-1/heartbeat', $calls[1]['uri']);
    }

    public function testUnboundedAdapterIsRefusedBeforeWorkerRegistration(): void
    {
        $transport = new FakeTransport();
        try {
            new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
                'queue', enableCooperativeCancellation: true);
            self::fail('An unbounded adapter was accepted for cooperative work.');
        } catch (InvalidArgumentException $error) {
            self::assertStringContainsString('bounded requests', $error->getMessage());
        }
        self::assertSame([], $transport->requests);
    }

    public function testOpaquePsrClientRemainsUsableForOrdinaryRequests(): void
    {
        $http = new class implements ClientInterface {
            public int $calls = 0;
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                ++$this->calls;

                return new Response(200, [], '{"status":"ok"}');
            }
        };
        $transport = new Psr18Transport($http);
        self::assertFalse($transport->supportsBoundedRequests());
        try {
            $transport->sendBounded('GET', 'https://server.example', [], null, 1);
            self::fail('An adapter with no timeout control claimed a bounded request.');
        } catch (InvalidArgumentException) {
            self::assertSame(0, $http->calls);
        }
        self::assertSame(['status' => 'ok'], $transport->send('GET', 'https://server.example', []));
        self::assertSame(1, $http->calls);
    }

    public function testRealPartialResponseBodyCannotPinTheOwningWorker(): void
    {
        if (!extension_loaded('curl') || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::markTestSkipped('Real cURL and process control are required.');
        }
        $listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertIsResource($listener);
        $address = stream_socket_get_name($listener, false);
        self::assertIsString($address);
        $pid = pcntl_fork();
        self::assertGreaterThanOrEqual(0, $pid);
        if ($pid === 0) {
            $connection = stream_socket_accept($listener, 3);
            if ($connection !== false) {
                while (($line = fgets($connection)) !== false && $line !== "\r\n") {}
                fwrite($connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{");
                fflush($connection);
                sleep(10);
                fclose($connection);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        fclose($listener);
        $startedAt = microtime(true);
        try {
            (new Psr18Transport())->sendBounded('POST', 'http://'.$address, [], ['lease_owner' => 'owner-1'], 1);
            self::fail('The incomplete response escaped its bound.');
        } catch (TransportException) {
            self::assertGreaterThanOrEqual(0.8, microtime(true) - $startedAt);
            self::assertLessThan(2.5, microtime(true) - $startedAt);
        } finally {
            posix_kill($pid, SIGKILL);
            self::assertSame($pid, pcntl_waitpid($pid, $status));
        }
    }
}
