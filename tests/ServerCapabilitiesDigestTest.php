<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\ResponseTooLarge;
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

    /**
     * Rolling deploy: a completion is in flight under D1 while another worker
     * request (as completion pipelining overlaps them) learns D2. The server
     * answers the completion with the digest it was sent, D1, after committing
     * it; the transport must restore the D1 object, not refuse the answer.
     */
    public function testOverlappingRequestsAcrossTwoDigestsEachRestoreTheirOwnCapabilities(): void
    {
        $d2 = 'sha256:'.str_repeat('2', 64);
        $capabilities2 = ['query_tasks' => true, 'activity_lease_renewal' => true, 'new_after_deploy' => true];
        $http = new class (self::DIGEST, self::CAPABILITIES, $d2, $capabilities2) implements ClientInterface {
            public ?Psr18Transport $transport = null;
            /** @var list<array{string, string}> */
            public array $sent = [];
            public ?array $overlapped = null;

            public function __construct(private string $d1, private array $c1, private string $d2, private array $c2)
            {
            }

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $path = $request->getUri()->getPath();
                $held = $request->getHeaderLine(ServerCapabilitiesDigest::HEADER);
                $this->sent[] = [$path, $held];
                $body = match ($path) {
                    // Learn D1.
                    '/api/worker/heartbeat' => ['server_capabilities' => $this->c1 + ['digest' => $this->d1]],
                    // The deploy lands; this answer moves the transport to D2.
                    '/api/worker/activity-tasks/poll' => ['task' => null, 'server_capabilities' => $this->c2 + ['digest' => $this->d2]],
                    // Committed under D1: the server elides with the digest this request sent.
                    default => ['outcome' => 'completed', 'recorded' => true, 'server_capabilities' => $held],
                };
                if ($path === '/api/worker/workflow-tasks/t/complete') {
                    // Another request runs while the completion is in flight.
                    $this->overlapped = $this->transport->send('POST', 'https://runtime.test/api/worker/activity-tasks/poll',
                        ['X-Durable-Workflow-Protocol-Version' => '1.20'], ['timeout_seconds' => 0]);
                }

                return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
            }
        };
        $transport = new Psr18Transport(client: $http);
        $http->transport = $transport;
        $worker = ['X-Durable-Workflow-Protocol-Version' => '1.20'];

        $transport->send('POST', 'https://runtime.test/api/worker/heartbeat', $worker, []);
        $completion = $transport->send('POST', 'https://runtime.test/api/worker/workflow-tasks/t/complete', $worker, []);

        self::assertSame(self::CAPABILITIES, $completion['server_capabilities'], 'Resolved against the pair it was sent with.');
        self::assertTrue($completion['recorded']);
        self::assertSame($capabilities2, $http->overlapped['server_capabilities']);
        self::assertSame([['/api/worker/heartbeat', 'none'], ['/api/worker/workflow-tasks/t/complete', self::DIGEST],
            ['/api/worker/activity-tasks/poll', self::DIGEST]], $http->sent);

        // Requests sent after the overlap carry the newest digest.
        $http->overlapped = null;
        $after = $transport->send('POST', 'https://runtime.test/api/worker/workflow-tasks/u/complete', $worker, []);
        self::assertSame($d2, $http->sent[3][1]);
        self::assertSame($capabilities2, $after['server_capabilities']);
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

    public function testAnUnknownDigestIsRecoveredByAskingAgainInFull(): void
    {
        $http = $this->http([
            [200, [], json_encode(['task' => null, 'server_capabilities' => 'sha256:'.str_repeat('f', 64)], JSON_THROW_ON_ERROR)],
            [200, [], json_encode(['task' => null, 'server_capabilities' => self::CAPABILITIES], JSON_THROW_ON_ERROR)],
            [200, [], json_encode(['task' => null, 'server_capabilities' => self::tagged()], JSON_THROW_ON_ERROR)],
        ]);
        $transport = new Psr18Transport(client: $http);
        $worker = ['X-Durable-Workflow-Protocol-Version' => '1.20'];

        $answer = $transport->send('POST', 'https://runtime.test/api/worker/workflow-tasks/poll', $worker, ['timeout_seconds' => 0]);
        self::assertSame(self::CAPABILITIES, $answer['server_capabilities']);
        self::assertCount(2, $http->requests);
        self::assertSame('none', $http->requests[0]->getHeaderLine(ServerCapabilitiesDigest::HEADER));
        self::assertFalse($http->requests[1]->hasHeader(ServerCapabilitiesDigest::HEADER), 'Retried without the header.');
        self::assertSame((string) $http->requests[0]->getBody(), (string) $http->requests[1]->getBody());
        // The next request asks for tagged capabilities again and learns them.
        $transport->send('POST', 'https://runtime.test/api/worker/heartbeat', $worker, []);
        self::assertSame('none', $http->requests[2]->getHeaderLine(ServerCapabilitiesDigest::HEADER));
    }

    public function testAGarbledDigestOnAMutatingAnswerKeepsTheBodyAndReadsCapabilitiesInstead(): void
    {
        $http = $this->http([
            [200, [], json_encode(['outcome' => 'completed', 'recorded' => true, 'next' => 'kept',
                'server_capabilities' => 'sha256:'.str_repeat('e', 64)], JSON_THROW_ON_ERROR)],
            [200, [], json_encode(['worker_protocol' => ['version' => '1.20', 'server_capabilities' => self::CAPABILITIES]], JSON_THROW_ON_ERROR)],
        ]);
        $answer = (new Psr18Transport(client: $http))->send('POST', 'https://runtime.test/api/worker/workflow-tasks/t/complete',
            ['X-Durable-Workflow-Protocol-Version' => '1.20', 'Authorization' => 'Bearer w'], ['commands' => []]);

        self::assertSame('kept', $answer['next'], 'The answer as received is processed.');
        self::assertSame(self::CAPABILITIES, $answer['server_capabilities']);
        self::assertCount(2, $http->requests, 'The completion is never repeated.');
        self::assertSame('POST', $http->requests[0]->getMethod());
        self::assertSame('GET', $http->requests[1]->getMethod());
        self::assertSame('/api/cluster/info', $http->requests[1]->getUri()->getPath());
        self::assertSame('Bearer w', $http->requests[1]->getHeaderLine('Authorization'));
    }

    public function testAGarbledDigestOnAPollDoesNotStopAManagedWorker(): void
    {
        $polls = 0;
        $http = new class ($polls) implements ClientInterface {
            /** @var list<RequestInterface> */
            public array $requests = [];

            public function __construct(public int $polls)
            {
            }

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;
                $path = $request->getUri()->getPath();
                $garbled = 'sha256:'.str_repeat('c', 64);
                $body = match (true) {
                    str_ends_with($path, '/worker/register') => ['registered' => true],
                    str_ends_with($path, '/workflow-tasks/poll') => ['task' => null, 'poll_status' => 'empty',
                        'server_capabilities' => ++$this->polls === 1 && $request->hasHeader(ServerCapabilitiesDigest::HEADER)
                            ? $garbled : ['query_tasks' => true]],
                    str_ends_with($path, '/query-tasks/poll') => ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
                    default => ['task' => null, 'poll_status' => 'empty'],
                };

                return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
            }
        };
        $worker = new \DurableWorkflow\Worker(new \DurableWorkflow\Client('https://runtime.test',
            transport: new Psr18Transport(client: $http), workerToken: 'w'), 'orders', workerId: 'worker-1');

        $worker->run(0);

        $paths = array_map(static fn (RequestInterface $request): string => $request->getUri()->getPath(), $http->requests);
        self::assertSame(2, count(array_keys($paths, '/api/worker/workflow-tasks/poll')), 'The garbled poll was asked again in full.');
        self::assertContains('/api/worker/query-tasks/poll', $paths, 'The Worker kept running.');
    }

    /**
     * The reviewer's scenario: a workflow-task completion whose answer carries
     * the next leased activity arrives with every capabilities copy garbled.
     * The completion is never repeated (a repeat would lose the carried task);
     * the answer is kept, capabilities are read from cluster info, the carried
     * activity runs once and completes once, and the Worker keeps running.
     */
    public function testAGarbledCompletionAnswerCarryingTheNextActivityIsKeptNotResent(): void
    {
        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
        $caps = ['workflow_task_completion_activity_poll' => true];
        $codec = new \DurableWorkflow\Codec\AvroPayloadCodec();
        $garbled = 'sha256:'.str_repeat('9', 64);
        $answer = static fn (array $task, mixed $capabilities): array => ['task' => $task + ['lease_owner' => 'worker-1',
            'payload_codec' => 'avro', 'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $capabilities,
            'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
        $http = new class ($answer, $caps, $codec, $garbled) implements ClientInterface {
            /** @var list<RequestInterface> */
            public array $requests = [];
            public int $workflowPolls = 0;

            public function __construct(private \Closure $answer, private array $caps, private object $codec, private string $garbled)
            {
            }

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;
                $path = $request->getUri()->getPath();
                $answer = $this->answer;
                $body = match (true) {
                    str_ends_with($path, '/worker/register') => ['registered' => true],
                    str_ends_with($path, '/cluster/info') => ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => $this->caps]],
                    str_ends_with($path, '/workflow-tasks/poll') && ++$this->workflowPolls === 1 => $answer(['task_id' => 'workflow-task',
                        'workflow_task_attempt' => 1, 'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'carry',
                        'history_events' => []], $this->caps),
                    str_ends_with($path, '/workflow-tasks/workflow-task/complete') => ['completed' => true,
                        'activity_poll' => ['leased' => true, 'answer' => $answer(['task_id' => 'activity-1',
                            'activity_attempt_id' => 'attempt-1', 'activity_type' => 'carried.activity',
                            'arguments' => $this->codec->envelope([7])], $this->garbled)],
                        'server_capabilities' => $this->garbled],
                    str_ends_with($path, '/activity-tasks/activity-1/complete') => ['recorded' => true, 'outcome' => 'completed'],
                    str_ends_with($path, '/query-tasks/poll') && $this->workflowPolls >= 2 => ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
                    default => ['task' => null, 'poll_status' => 'empty'],
                };

                return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
            }
        };
        $runs = 0;
        $worker = new \DurableWorkflow\Worker(new \DurableWorkflow\Client('https://runtime.test',
            transport: new Psr18Transport(client: $http), workerToken: 'w'), 'orders', workerId: 'worker-1');
        $worker->registerWorkflow('carry', static fn (\DurableWorkflow\Worker\WorkflowContext $context): string => 'done');
        $worker->registerActivity('carried.activity', static function (\DurableWorkflow\Worker\ActivityContext $context, int $value) use (&$runs): int {
            ++$runs;

            return $value;
        });

        $worker->run(0);

        $paths = array_map(static fn (RequestInterface $request): string => $request->getUri()->getPath(), $http->requests);
        self::assertSame(1, count(array_keys($paths, '/api/worker/workflow-tasks/workflow-task/complete')), 'Never repeated.');
        self::assertContains('/api/cluster/info', $paths, 'Capabilities were read, not re-requested by repeating.');
        self::assertSame(1, $runs, 'The carried activity ran exactly once.');
        self::assertSame(1, count(array_keys($paths, '/api/worker/activity-tasks/activity-1/complete')), 'And completed once.');
    }

    public function testAnUnknownDigestInAnErrorBodyNeverReplacesTheHttpError(): void
    {
        $http = $this->http([[409, [], json_encode(['reason' => 'stale_attempt', 'server_capabilities' => 'sha256:'.str_repeat('d', 64)], JSON_THROW_ON_ERROR)]]);
        try {
            (new Psr18Transport(client: $http))->send('POST', 'https://runtime.test/api/worker/activity-tasks/a/complete',
                ['X-Durable-Workflow-Protocol-Version' => '1.20'], []);
            self::fail('The 409 must surface.');
        } catch (TransportException $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('stale_attempt', $exception->response['reason']);
            self::assertArrayNotHasKey('server_capabilities', $exception->response);
        }
        self::assertCount(1, $http->requests, 'An error answer is not retried.');
    }

    public function testAGzipBodyThatDecodesPastTheLimitIsRefused(): void
    {
        $body = (string) gzencode(json_encode(['blob' => str_repeat('z', 64 * 1024)], JSON_THROW_ON_ERROR));
        self::assertLessThan(1024, strlen($body));
        $http = $this->http([[200, ['Content-Encoding' => 'gzip'], $body]]);
        try {
            (new Psr18Transport(client: $http, maxDecodedResponseBytes: 16 * 1024))->send('GET', 'https://runtime.test/api/workflows/x',
                ['X-Durable-Workflow-Control-Plane-Version' => '2']);
            self::fail('A bomb must be refused.');
        } catch (ResponseTooLarge $exception) {
            self::assertSame(16 * 1024, $exception->limitBytes);
            self::assertSame('response_too_large', $exception->response['reason']);
        }
        // Within the default limit it decodes normally.
        $http = $this->http([[200, ['Content-Encoding' => 'gzip'], $body]]);
        self::assertSame(64 * 1024, strlen((new Psr18Transport(client: $http))->send('GET', 'https://runtime.test/api/workflows/x',
            ['X-Durable-Workflow-Control-Plane-Version' => '2'])['blob']));
    }

    public function testTheDefaultDecodeLimitFitsPhpsMemoryLimit(): void
    {
        self::assertSame(32 * 1024 * 1024, Psr18Transport::defaultMaxDecodedResponseBytes('128M'));
        self::assertSame(16 * 1024 * 1024, Psr18Transport::defaultMaxDecodedResponseBytes('64M'));
        self::assertSame(32 * 1024 * 1024, Psr18Transport::defaultMaxDecodedResponseBytes('-1'));
        self::assertSame(32 * 1024 * 1024, Psr18Transport::defaultMaxDecodedResponseBytes('2G'));
        self::assertSame(1024 * 1024, Psr18Transport::defaultMaxDecodedResponseBytes('2M'));
    }

    public function testAnOversizedResponseReachesAClientCallerAsResponseTooLarge(): void
    {
        $body = (string) gzencode(json_encode(['blob' => str_repeat('z', 64 * 1024)], JSON_THROW_ON_ERROR));
        $client = new \DurableWorkflow\Client('https://runtime.test', transport: new Psr18Transport(
            client: $this->http([[200, ['Content-Encoding' => 'gzip'], $body]]), maxDecodedResponseBytes: 16 * 1024));
        $this->expectException(ResponseTooLarge::class);
        $client->describeWorkflow('x');
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
