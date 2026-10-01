<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\PayloadTransport;
use DurableWorkflow\Transport\PayloadUploadTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Transport\RuntimePayloads;
use DurableWorkflow\Transport\RuntimePayloadUploads;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamInterface;

final class BoundedPayloadTransportTest extends TestCase
{
    public function testWorkerPayloadDiscoveryUploadAndDownloadAreBoundedWithoutChangingOrdinaryRequests(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(str_repeat('x', 4096))['blob'];
        $reference = RuntimePayloadUploadTest::reference($blob);
        $calls = [];
        $http = new GuzzleClient(['timeout' => 42, 'stream' => true,
            'handler' => static function (RequestInterface $request, array $options) use (&$calls, $reference, $blob) {
                $calls[] = [$request, $options];
                $path = $request->getUri()->getPath();
                if (str_ends_with($path, '/cluster/info')) {
                    $body = ['limits' => ['max_payload_bytes' => 4096], 'namespace' => ['external_payload_storage' => [
                        'status' => 'available', 'threshold_bytes' => 128,
                        'transport' => ['schema' => 'durable-workflow.v2.runtime-external-payload-transport.v1', 'version' => 1,
                            'reference_schema' => $reference['schema'], 'mode' => 'authenticated_namespace_runtime',
                            'upload' => ['method' => 'POST', 'path' => '/api/external-payloads/v1'],
                            'fetch' => ['method' => 'GET', 'path_template' => '/api/external-payloads/v1/{referenceId}'],
                            'limits' => ['max_payload_bytes' => 16384, 'request_timeout_seconds' => 45]],
                    ]]];
                } elseif (str_ends_with($path, '/external-payloads/v1')) {
                    self::assertSame($blob, (string) $request->getBody());
                    $body = ['schema' => 'durable-workflow.v2.runtime-external-payload-upload.v1',
                        'transport_version' => 1, 'reference' => $reference];
                } elseif (str_ends_with($path, '/poll')) {
                    $body = ['task' => ['arguments' => ['codec' => 'avro', 'external_payload' => $reference]]];
                } elseif (str_ends_with($path, '/'.$reference['reference_id'])) {
                    return Create::promiseFor(new Response(200, self::metadata($blob), $blob));
                } else {
                    $body = ['outcome' => 'completed'];
                }

                return Create::promiseFor(new Response(200, [], json_encode($body, JSON_THROW_ON_ERROR)));
            }]);
        $original = new Client('https://runtime.test', namespace: 'tenant-one', transport: new Psr18Transport($http),
            controlToken: 'fixture-control', workerToken: 'fixture-worker', workerProtocolVersion: '1.20');
        $bounded = $original->withBoundedWorkerRequests();
        $bounded->completeActivityTask('task-1', 'attempt-1', 'owner-1', str_repeat('x', 4096));
        $poll = $bounded->pollWorkflowTaskResponse('worker-1', 'queue', 60);
        self::assertSame(['codec' => 'avro', 'blob' => $blob], $poll['task']['arguments']);
        $original->completeActivityTask('task-1', 'attempt-1', 'owner-1', str_repeat('x', 4096));
        $bounded->clusterInfo();

        self::assertSame([5, 5, 5, 65, 5, 45, 42, 42], array_column(array_column($calls, 1), 'timeout'));
        foreach (array_slice($calls, 0, 5) as [$request, $options]) {
            self::assertSame('Bearer fixture-worker', $request->getHeaderLine('Authorization'));
            self::assertSame('tenant-one', $request->getHeaderLine('X-Namespace'));
            self::assertFalse($options['stream']);
            self::assertFalse($options['allow_redirects']);
        }
        self::assertInstanceOf(StreamInterface::class, $calls[1][1]['sink']);
        self::assertInstanceOf(StreamInterface::class, $calls[4][1]['sink']);
        self::assertTrue($calls[5][1]['stream']);
        self::assertTrue($calls[6][1]['stream']);
        self::assertSame('Bearer fixture-control', $calls[7][0]->getHeaderLine('Authorization'));
        self::assertSame(42, $http->getConfig('timeout'));
        self::assertTrue($http->getConfig('stream'));
    }

    public function testLegacyBinaryAdapterCannotAdvertiseCooperativeSupport(): void
    {
        $transport = new class implements BoundedTransport, PayloadTransport, PayloadUploadTransport {
            public int $calls = 0;
            public function supportsBoundedRequests(): bool { return true; }
            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array { ++$this->calls; return []; }
            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array { ++$this->calls; return []; }
            public function fetchPayload(string $uri, array $headers, int $maxBytes): string { ++$this->calls; return ''; }
            public function uploadPayload(string $uri, array $headers, string $blob, int $timeoutSeconds): array { ++$this->calls; return []; }
        };
        try {
            (new Client('https://runtime.test', transport: $transport))->withBoundedWorkerRequests();
            self::fail('Legacy binary I/O was accepted for a cooperative worker.');
        } catch (\InvalidArgumentException $exception) {
            self::assertStringContainsString('bounded payload', $exception->getMessage());
        }
        self::assertSame(0, $transport->calls);
    }

    #[DataProvider('uploadStages')]
    public function testDiscoveryAndUploadCannotRestartAnExpiredCompletionBudget(bool $stallDiscovery): void
    {
        $blob = (new AvroPayloadCodec())->envelope(str_repeat('x', 4096))['blob'];
        $requests = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$requests, $stallDiscovery, $blob) {
            ++$requests;
            $discovery = str_ends_with($request->getUri()->getPath(), '/cluster/info');
            self::assertSame(1, $options['timeout']);
            if ($discovery === $stallDiscovery) { usleep(1100000); }
            $body = $discovery ? self::discovery() : [
                'schema' => 'durable-workflow.v2.runtime-external-payload-upload.v1', 'transport_version' => 1,
                'reference' => RuntimePayloadUploadTest::reference($blob),
            ];

            return Create::promiseFor(new Response(200, [], json_encode($body, JSON_THROW_ON_ERROR)));
        }]);
        try {
            (new RuntimePayloadUploads(new Psr18Transport($http), 'https://runtime.test'))->request(
                ['result' => ['codec' => 'avro', 'blob' => $blob]], 'POST', '/worker/activity-tasks/task-1/complete', true, [], new RequestBudget(1));
            self::fail('A late reply restarted or escaped the request budget.');
        } catch (TransportException|ExternalPayloadException $exception) {
            $cause = $exception instanceof ExternalPayloadException ? $exception->getPrevious() : $exception;
            self::assertInstanceOf(TransportException::class, $cause);
            self::assertStringContainsString('time budget expired', $cause->getMessage());
        }
        self::assertSame($stallDiscovery ? 1 : 2, $requests);
    }

    public static function uploadStages(): iterable
    {
        yield 'discovery' => [true];
        yield 'upload' => [false];
    }

    public function testHydratingSeveralObjectsSharesOneBudgetAndNeverReturnsAPartialTask(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $reference = RuntimePayloadUploadTest::reference($blob);
        $other = array_replace($reference, ['reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAW']);
        $requests = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$requests, $blob) {
            ++$requests;
            self::assertSame(1, $options['timeout']);
            usleep(1100000);

            return Create::promiseFor(new Response(200, self::metadata($blob), $blob));
        }]);
        try {
            (new RuntimePayloads(new Psr18Transport($http), 'https://runtime.test', [], 4096, new RequestBudget(1)))->response(
                ['task' => ['arguments' => ['codec' => 'avro', 'external_payload' => $reference],
                    'workflow_arguments' => ['codec' => 'avro', 'external_payload' => $other]]], '/worker/workflow-tasks/poll', true);
            self::fail('An expired hydration returned a task.');
        } catch (TransportException $exception) {
            self::assertStringContainsString('time budget expired', $exception->getMessage());
        }
        self::assertSame(1, $requests);
    }

    private static function discovery(): array
    {
        return ['namespace' => ['external_payload_storage' => ['status' => 'available', 'threshold_bytes' => 128,
            'transport' => ['schema' => 'durable-workflow.v2.runtime-external-payload-transport.v1', 'version' => 1,
                'reference_schema' => RuntimePayloads::SCHEMA, 'mode' => 'authenticated_namespace_runtime',
                'upload' => ['method' => 'POST', 'path' => '/api/external-payloads/v1'],
                'fetch' => ['method' => 'GET', 'path_template' => '/api/external-payloads/v1/{referenceId}'],
                'limits' => ['max_payload_bytes' => 16384, 'request_timeout_seconds' => 45]],
        ]]];
    }

    #[DataProvider('incompleteResponses')]
    public function testRealIncompletePayloadTransfersExpireWithinTheWholeRequestBound(string $mode): void
    {
        $blob = str_repeat('x', 32);
        $this->withTcpServer(static function ($connection) use ($mode, $blob): void {
            $headers = $mode === 'upload' ? ['Content-Type' => 'application/json'] : self::metadata($blob);
            $reply = "HTTP/1.1 200 OK\r\nContent-Length: 32\r\n";
            foreach ($headers as $name => $value) { $reply .= $name.': '.$value."\r\n"; }
            fwrite($connection, $reply."\r\n".($mode === 'upload' ? '{' : 'x'));
            fflush($connection);
            if ($mode === 'trickle') {
                for ($i = 0; $i < 24; ++$i) { usleep(150000); @fwrite($connection, 'x'); }
            } else {
                sleep(10);
            }
        }, static function (string $uri) use ($mode, $blob): void {
            $startedAt = microtime(true);
            try {
                $transport = new Psr18Transport();
                if ($mode === 'upload') {
                    $transport->uploadPayloadBounded($uri, [], $blob, 1);
                } else {
                    $transport->fetchPayloadBounded($uri, self::metadata($blob), strlen($blob), 1);
                }
                self::fail('An incomplete payload transfer escaped its deadline.');
            } catch (TransportException) {
                self::assertGreaterThanOrEqual(0.8, microtime(true) - $startedAt);
                self::assertLessThan(2.5, microtime(true) - $startedAt);
            }
        });
    }

    public static function incompleteResponses(): iterable
    {
        yield 'stalled download' => ['download'];
        yield 'trickling download' => ['trickle'];
        yield 'stalled upload response' => ['upload'];
    }

    #[DataProvider('binaryOperations')]
    public function testRealOversizedPayloadResponsesStopAtTheSinkBound(bool $upload): void
    {
        $blob = str_repeat('x', 32);
        $this->withTcpServer(static function ($connection) use ($blob): void {
            $reply = "HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\n";
            foreach (self::metadata($blob) as $name => $value) { $reply .= $name.': '.$value."\r\n"; }
            @fwrite($connection, $reply."\r\n".str_repeat('x', 1048576));
        }, static function (string $uri) use ($upload, $blob): void {
            try {
                $transport = new Psr18Transport();
                if ($upload) { $transport->uploadPayloadBounded($uri, [], $blob, 1); }
                else { $transport->fetchPayloadBounded($uri, self::metadata($blob), strlen($blob), 1); }
                self::fail('The oversized response was accepted.');
            } catch (ExternalPayloadException $exception) {
                self::assertSame('external_payload_integrity_mismatch', $exception->reason);
                self::assertStringContainsString('read bound', $exception->getMessage());
            }
        });
    }

    public static function binaryOperations(): iterable
    {
        yield 'download' => [false];
        yield 'upload' => [true];
    }

    #[DataProvider('binaryOperations')]
    public function testRealCompletePayloadResponsesRemainReadableAndValid(bool $upload): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value', 42])['blob'];
        $reference = RuntimePayloadUploadTest::reference($blob);
        $this->withTcpServer(static function ($connection) use ($upload, $blob, $reference): void {
            $body = $upload ? json_encode(['reference' => $reference], JSON_THROW_ON_ERROR) : $blob;
            $headers = $upload ? ['Content-Type' => 'application/json'] : self::metadata($blob);
            $reply = "HTTP/1.1 200 OK\r\nContent-Length: ".strlen($body)."\r\n";
            foreach ($headers as $name => $value) { $reply .= $name.': '.$value."\r\n"; }
            fwrite($connection, $reply."\r\n".$body);
        }, static function (string $uri) use ($upload, $blob, $reference): void {
            $transport = new Psr18Transport();
            if ($upload) { self::assertSame(['reference' => $reference], $transport->uploadPayloadBounded($uri, [], $blob, 1)); }
            else { self::assertSame($blob, $transport->fetchPayloadBounded($uri, self::metadata($blob), strlen($blob), 1)); }
        });
    }

    private static function metadata(string $blob): array
    {
        return ['Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
            'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
            'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob)];
    }

    private function withTcpServer(callable $serve, callable $request): void
    {
        if (!extension_loaded('curl') || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::markTestSkipped('Real cURL and process control are required.');
        }
        $listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertIsResource($listener);
        $address = stream_socket_get_name($listener, false);
        self::assertIsString($address);
        $serverPid = pcntl_fork();
        self::assertGreaterThanOrEqual(0, $serverPid);
        if ($serverPid === 0) {
            $connection = stream_socket_accept($listener, 3);
            if ($connection !== false) {
                $length = 0;
                while (($line = fgets($connection)) !== false && $line !== "\r\n") {
                    if (preg_match('/^Content-Length: (\d+)/i', $line, $match)) { $length = (int) $match[1]; }
                }
                while ($length > 0 && !feof($connection)) { $length -= strlen(fread($connection, $length)); }
                $serve($connection);
                fclose($connection);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        fclose($listener);
        try {
            $request('http://'.$address);
        } finally {
            posix_kill($serverPid, SIGKILL);
            self::assertSame($serverPid, pcntl_waitpid($serverPid, $status));
        }
    }
}
