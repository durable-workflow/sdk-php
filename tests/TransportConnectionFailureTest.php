<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\TransferStats;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class TransportConnectionFailureTest extends TestCase
{
    #[DataProvider('transferFailures')]
    public function testStructuredTransferClassification(mixed $error, bool $retryable): void
    {
        $observations = 0;
        $http = new GuzzleClient([
            'on_stats' => static function () use (&$observations): void {
                ++$observations;
            },
            'handler' => static function (RequestInterface $request, array $options) use ($error) {
                self::assertFalse($options['allow_redirects']);
                self::assertFalse($options['http_errors']);
                self::assertTrue($options['synchronous']);
                $options['on_stats'](new TransferStats($request, null, 0.0, $error));

                return Create::rejectionFor(new ConnectException('Network failure.', $request));
            },
        ]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));

        try {
            $client->heartbeatWorker('worker-1');
            self::fail('The transport should surface the failure without retrying a request.');
        } catch (ServerException $exception) {
            self::assertSame($retryable, $exception->isTransientConnectionFailure());
            self::assertSame(0, $exception->status);
        }
        self::assertSame(1, $observations);
    }

    /** @return iterable<string, array{mixed, bool}> */
    public static function transferFailures(): iterable
    {
        foreach ([5, 6, 7, 28, 52, 55, 56] as $errno) {
            yield "transient cURL {$errno}" => [$errno, true];
        }
        foreach ([1, 2, 3, 35, 43, 58, 59, 60, 77, 83, 90, 91, 98, 0] as $errno) {
            yield "permanent cURL {$errno}" => [$errno, false];
        }
        yield 'opaque handler' => [null, false];
        yield 'stream handler boolean' => [true, false];
        yield 'string is not an errno' => ['7', false];
        yield 'local stream failure' => [new \RuntimeException('Stream failed.'), false];
    }

    public function testFailureStatisticsDoNotLeakIntoTheNextRequest(): void
    {
        $calls = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$calls) {
            if (++$calls === 1) {
                $options['on_stats'](new TransferStats($request, null, 0.0, 7));

                return Create::rejectionFor(new ConnectException('Connection refused.', $request));
            }

            return Create::promiseFor(new Response(200, [], 'invalid JSON'));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));

        foreach ([true, false] as $expected) {
            try {
                $client->heartbeatWorker('worker-1');
                self::fail('The request should fail.');
            } catch (ServerException $exception) {
                self::assertSame($expected, $exception->isTransientConnectionFailure());
            }
        }
    }

    public function testOpaquePsrNetworkFailureIsNotSilentlyRetried(): void
    {
        $http = new class implements ClientInterface {
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                throw new ConnectException('cURL error 7 is just prose, not classification.', $request);
            }
        };

        try {
            (new Client('https://server.example', transport: new Psr18Transport($http)))->heartbeatWorker('worker-1');
            self::fail('The unclassified network failure should be surfaced.');
        } catch (ServerException $exception) {
            self::assertFalse($exception->isTransientConnectionFailure());
        }
    }

    public function testRealConnectionRefusalIsClassifiedByTheDefaultTransport(): void
    {
        if (!extension_loaded('curl')) {
            self::markTestSkipped('The default cURL transport requires ext-curl.');
        }
        // Reserve a local port without listening, so no other process can claim it.
        $socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error, STREAM_SERVER_BIND);
        self::assertIsResource($socket);
        try {
            $address = stream_socket_get_name($socket, false);
            self::assertIsString($address);
            (new Client('http://'.$address))->heartbeatWorker('worker-1');
            self::fail('The reserved non-listening port should refuse the connection.');
        } catch (ServerException $exception) {
            self::assertTrue($exception->isTransientConnectionFailure());
        } finally {
            fclose($socket);
        }
    }
}
