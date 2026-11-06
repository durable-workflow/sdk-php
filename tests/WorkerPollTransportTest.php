<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\TransferStats;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class WorkerPollTransportTest extends TestCase
{
    private const MEDIA_TYPE = 'application/vnd.durable-workflow.poll.v1+json';

    #[DataProvider('pollKinds')]
    public function testEveryPollKindNegotiatesAndUnwrapsTheLogicalBody(string $kind): void
    {
        $calls = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$calls) {
            $calls[] = [$request, $options];

            return Create::promiseFor(new Response(200, ['Content-Type' => strtoupper(self::MEDIA_TYPE).'; charset=utf-8'],
                "\n \n".self::frame(200, '{"task":null,"poll_status":"empty","decimal":1.0,"large":9223372036854775808}')));
        }]);
        $result = (new Psr18Transport($http))->send('POST', 'https://server.example/namespaces/example/api/worker/'.$kind.'-tasks/poll',
            ['Accept' => 'application/json'], ['timeout_seconds' => 4]);
        self::assertCount(1, $calls);
        self::assertSame(self::MEDIA_TYPE.', application/json;q=0.9', $calls[0][0]->getHeaderLine('Accept'));
        self::assertSame(9, $calls[0][1]['timeout']);
        self::assertFalse($calls[0][1]['stream']);
        self::assertFalse($calls[0][1]['allow_redirects']);
        self::assertSame(json_decode('{"task":null,"poll_status":"empty","decimal":1.0,"large":9223372036854775808}', true), $result);
    }

    public static function pollKinds(): array
    {
        return [['workflow'], ['activity'], ['query'], ['update-validation']];
    }

    #[DataProvider('pollKinds')]
    public function testOfferAndAcceptanceKeepProgressNegotiationAndTheirAbsoluteBudgets(string $kind): void
    {
        $calls = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$calls) {
            $calls[] = [$request, $options];
            return Create::promiseFor(new Response(200, ['Content-Type' => self::MEDIA_TYPE], self::frame(200)));
        }]);
        $transport = new Psr18Transport($http);
        $uri = 'https://server.example/api/worker/'.$kind.'-tasks/offers';
        $transport->send('POST', $uri, [], ['timeout_seconds' => 4, 'poll_request_id' => 'same-poll']);
        $transport->send('POST', $uri, [], ['timeout_seconds' => 0, 'poll_request_id' => 'same-poll', 'task_offer_token' => 'same-packet']);
        self::assertSame(9, $calls[0][1]['timeout']);
        self::assertSame(5, $calls[1][1]['timeout']);
        foreach ($calls as [$request, $options]) {
            self::assertSame(self::MEDIA_TYPE.', application/json;q=0.9', $request->getHeaderLine('Accept'));
            self::assertFalse($options['stream']);
        }
        self::assertSame('same-packet', json_decode((string) $calls[1][0]->getBody(), true)['task_offer_token']);
    }

    public function testAShorterInjectedGuzzleDeadlineIsPreserved(): void
    {
        $observed = null;
        $http = new GuzzleClient(['timeout' => 0.25, 'handler' => static function (RequestInterface $request, array $options) use (&$observed) {
            $observed = $options['timeout'];

            return Create::promiseFor(new Response(200, [], '{"task":null}'));
        }]);
        (new Psr18Transport($http))->send('POST', 'https://server.example/api/worker/workflow-tasks/poll', [], ['timeout_seconds' => 30]);
        self::assertSame(0.25, $observed);
    }

    #[DataProvider('lateStatuses')]
    public function testLateErrorsKeepStatusDetailsAndRetryHeaders(int $status): void
    {
        $http = new GuzzleClient(['handler' => static fn () => Create::promiseFor(
            new Response(200, ['Content-Type' => self::MEDIA_TYPE], self::frame($status,
                '{"task":null,"reason":"fixture_reason","retry_after_seconds":2}', '{"Retry-After":"2","X-Durable-Workflow-Protocol-Version":"1.20"}')),
        )]);
        try {
            (new Client('https://server.example', transport: new Psr18Transport($http)))
                ->pollActivityTaskResponse('worker', 'queue', 4, 'same-poll');
            self::fail('A late HTTP error became a successful task response.');
        } catch (ServerException $error) {
            self::assertSame($status, $error->status);
            self::assertSame('fixture_reason', $error->reason);
            self::assertSame(2, $error->details['retry_after_seconds']);
            self::assertInstanceOf(TransportException::class, $error->getPrevious());
            self::assertSame(['retry-after' => '2', 'x-durable-workflow-protocol-version' => '1.20'], $error->getPrevious()->headers);
        }
    }

    public static function lateStatuses(): array
    {
        return [[409], [422], [429], [500], [503]];
    }

    #[DataProvider('invalidFrames')]
    public function testMalformedOrContradictoryFramesNeverBecomeSuccessfulPolls(string $frame, int $outer): void
    {
        $http = new GuzzleClient(['handler' => static fn () => Create::promiseFor(new Response($outer, ['Content-Type' => self::MEDIA_TYPE], $frame))]);
        $this->expectException(TransportException::class);
        (new Psr18Transport($http))->send('POST', 'https://server.example/api/worker/workflow-tasks/poll', [], ['timeout_seconds' => 4]);
    }

    public static function invalidFrames(): array
    {
        return [
            'unknown schema' => [str_replace('transport.v1', 'transport.v2', self::frame(200)), 200],
            'boolean status' => [str_replace('"status":200', '"status":true', self::frame(200)), 200],
            'invalid status' => [self::frame(600), 200],
            'body list' => [self::frame(200, '[]'), 200],
            'headers list' => [self::frame(200, '{}', '[]'), 200],
            'header injection' => [self::frame(200, '{}', '{"retry-after":"2\\r\\nX-Other: no"}'), 200],
            'unknown header' => [self::frame(200, '{}', '{"set-cookie":"fixture"}'), 200],
            'duplicate normalized header' => [self::frame(200, '{}', '{"Retry-After":"2","retry-after":"3"}'), 200],
            'contradictory gateway status' => [self::frame(200), 504],
            'truncated frame' => [substr(self::frame(200), 0, -1), 200],
        ];
    }

    public function testLegacyJsonAndOpaqueClientPoliciesStayUsable(): void
    {
        $http = new class implements ClientInterface {
            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                self::assertLegacy($request);

                return new Response(200, ['Content-Type' => 'application/json'], '{"task":null,"poll_status":"empty"}');
            }

            private static function assertLegacy(RequestInterface $request): void
            {
                WorkerPollTransportTest::assertSame('application/json', $request->getHeaderLine('Accept'));
            }
        };
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        self::assertSame(['task' => null, 'poll_status' => 'empty'], $client->pollWorkflowTaskResponse('worker', 'queue', 4));

        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) {
            self::assertSame('application/json', $request->getHeaderLine('Accept'));

            return Create::promiseFor(new Response(200, [], '{"renewed":true}'));
        }]);
        self::assertSame(['renewed' => true], (new Psr18Transport($http))->send('POST', 'https://server.example/api/worker/workflow-tasks/task/heartbeat', ['Accept' => 'application/json']));
    }

    public function testAConnectionRetryKeepsTheSamePollIdentityAndWarnsBeforeTheRetrySucceeds(): void
    {
        $ids = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$ids) {
            $ids[] = json_decode((string) $request->getBody(), true)['poll_request_id'];
            if (count($ids) === 1) {
                $options['on_stats'](new TransferStats($request, null, 0, 7));

                return Create::rejectionFor(new ConnectException('Fixture connection failure.', $request));
            }

            return Create::promiseFor(new Response(200, ['Content-Type' => 'application/json'], '{"task":null,"poll_status":"stopped"}'));
        }]);
        $handler = new TestHandler();
        $worker = new Worker(new Client('https://server.example', transport: new Psr18Transport($http)), 'queue', logger: new Logger('fixture', [$handler]));
        self::assertFalse($worker->tick(30));
        self::assertCount(2, $ids);
        self::assertSame($ids[0], $ids[1]);
        $warnings = array_values(array_filter($handler->getRecords(), static fn ($record) => $record['message'] === 'worker.poll_ended_early'));
        self::assertCount(1, $warnings);
        self::assertSame(Logger::WARNING, $warnings[0]['level']);
        self::assertSame(30, $warnings[0]['context']['configured_wait_seconds']);
        self::assertLessThan(30, $warnings[0]['context']['observed_seconds']);
        self::assertArrayNotHasKey('exception', $warnings[0]['context']);
    }

    public function testProgressBytesCannotExtendTheRealCurlDeadline(): void
    {
        if (!extension_loaded('curl') || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::markTestSkipped('Real cURL and process control are required.');
        }
        $listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertIsResource($listener);
        $address = stream_socket_get_name($listener, false);
        $pid = pcntl_fork();
        self::assertGreaterThanOrEqual(0, $pid);
        if ($pid === 0) {
            $connection = stream_socket_accept($listener, 3);
            if ($connection !== false) {
                while (($line = fgets($connection)) !== false && $line !== "\r\n") {}
                fwrite($connection, "HTTP/1.1 200 OK\r\nContent-Type: ".self::MEDIA_TYPE."\r\nConnection: close\r\n\r\n");
                for ($i = 0; $i < 30; ++$i) {
                    @fwrite($connection, "\n");
                    @fflush($connection);
                    usleep(100000);
                }
                @fwrite($connection, self::frame(200));
                fclose($connection);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        fclose($listener);
        $started = hrtime(true);
        try {
            (new Psr18Transport())->sendBounded('POST', 'http://'.$address.'/api/worker/query-tasks/poll', [], ['timeout_seconds' => 0], 1);
            self::fail('Progress extended the absolute request deadline.');
        } catch (TransportException) {
            $observed = (hrtime(true) - $started) / 1_000_000_000;
            self::assertGreaterThanOrEqual(0.8, $observed);
            self::assertLessThan(2.5, $observed);
        } finally {
            posix_kill($pid, SIGKILL);
            self::assertSame($pid, pcntl_waitpid($pid, $status));
        }
    }

    #[DataProvider('cutoffStatuses')]
    public function testOnlyEarlyGatewayFailuresAreDiagnosedAsCutoffs(int $status, int $wait, bool $warn): void
    {
        $http = new GuzzleClient(['handler' => static fn () => Create::promiseFor(new Response($status, [], '{"task":null,"reason":"fixture"}'))]);
        $warnings = [];
        $client = (new Client('https://server.example', transport: new Psr18Transport($http)))
            ->withWorkerPollCutoffObserver(static function (array $context) use (&$warnings): void { $warnings[] = $context; });
        try {
            $client->pollWorkflowTaskResponse('worker', 'queue', $wait);
            self::fail('Fixture HTTP error became success.');
        } catch (ServerException $error) {
            self::assertSame($status, $error->status);
        }
        self::assertCount($warn ? 1 : 0, $warnings);
    }

    public static function cutoffStatuses(): array
    {
        return [[502, 4, true], [504, 4, true], [429, 4, false], [503, 4, false], [500, 4, false], [504, 0, false]];
    }

    private static function frame(int $status, string $body = '{}', string $headers = '{}'): string
    {
        return '{"schema":"durable-workflow.poll-transport.v1","status":'.$status.',"headers":'.$headers.',"body":'.$body.'}';
    }
}
