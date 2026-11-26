<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\TransientFailure;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\TransferStats;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * Cluster info is a read-only discovery call. A Worker outlives its outage:
 * the read is retried with the shared jittered backoff, a lasting outage is
 * reported as an error, startup waits, and no work is lost.
 */
final class WorkerClusterInfoOutageTest extends TestCase
{
    /** @return iterable<string, array{string}> */
    public static function outages(): iterable
    {
        yield 'a 500 with no reason' => ['500'];
        yield 'a refused connection' => ['refused'];
        yield 'a bodiless 502 from a load balancer' => ['502'];
    }

    /**
     * A Server whose cluster info fails `$failures` times with `$outage`.
     *
     * @param array<string, mixed> $capabilities
     */
    private static function server(string $outage, int $failures, array $capabilities = []): object
    {
        return new class ($outage, $failures, $capabilities) implements ClientInterface {
            /** @var list<string> */
            public array $paths = [];
            public int $clusterInfoReads = 0;
            public int $workflowPolls = 0;
            public bool $completed = false;
            public float $now = 0.0;

            /** @param array<string, mixed> $capabilities */
            public function __construct(private string $outage, private int $failures, private array $capabilities)
            {
            }

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $path = $request->getUri()->getPath();
                $this->paths[] = $path;
                if ($path === '/api/cluster/info' && ++$this->clusterInfoReads <= $this->failures) {
                    $this->now += 5.0;
                    if ($this->outage === 'refused') {
                        throw new ConnectException('Connection refused', $request);
                    }

                    return match ($this->outage) {
                        '502' => new Response(502, ['Content-Type' => 'text/html'], '<html>Bad gateway</html>'),
                        '503' => new Response(503, ['Content-Type' => 'application/json', 'Retry-After' => '9'],
                            '{"message":"Service Unavailable"}'),
                        default => new Response(500, ['Content-Type' => 'application/json'], '{"message":"Server Error"}'),
                    };
                }
                $body = match (true) {
                    $path === '/api/cluster/info' => ['version' => '3.17.0', 'limits' => ['max_payload_bytes' => 2097152],
                        'worker_protocol' => ['version' => '1.20', 'server_capabilities' => $this->capabilities]],
                    str_ends_with($path, '/worker/register') => ['registered' => true],
                    // Until it is completed, the task is offered again (as after a lease expiry).
                    str_ends_with($path, '/workflow-tasks/poll') && !$this->completed => ['poll_status' => 'leased',
                        'protocol_version' => '1.20', 'task' => ['task_id' => 'workflow-task',
                            'workflow_task_attempt' => ++$this->workflowPolls,
                            'lease_owner' => 'worker-1', 'workflow_id' => 'workflow', 'run_id' => 'run',
                            'workflow_type' => 'orders.flow', 'payload_codec' => 'avro', 'history_events' => [],
                            'lease_expires_at' => gmdate('Y-m-d\\TH:i:s.000000\\Z', time() + 30)],
                        'server_time' => gmdate('Y-m-d\\TH:i:s.000000\\Z')],
                    str_ends_with($path, '/workflow-tasks/workflow-task/complete') => ['completed' => $this->completed = true],
                    str_ends_with($path, '/query-tasks/poll') && ($this->completed || count($this->paths) > 600) => ['task' => null,
                        'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
                    default => ['task' => null, 'poll_status' => 'empty'],
                };

                return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
            }

            /** A Guzzle handler: a refused connection is reported as cURL reports it (errno 7). */
            public function client(): GuzzleClient
            {
                return new GuzzleClient(['handler' => function (RequestInterface $request, array $options): PromiseInterface {
                    try {
                        $response = $this->sendRequest($request);
                    } catch (ConnectException $refused) {
                        ($options['on_stats'] ?? static fn () => null)(new TransferStats($request, null, 0.0, 7));

                        return Create::rejectionFor($refused);
                    }
                    ($options['on_stats'] ?? static fn () => null)(new TransferStats($request, $response));

                    return Create::promiseFor($response);
                }]);
            }
        };
    }

    /** @param \ArrayObject<int, mixed> $events */
    private static function worker(object $server, \ArrayObject $events, bool $cooperativeCancellation = false,
        ?\ArrayObject $retryDelays = null): Worker
    {
        $transport = new Psr18Transport(client: $server->client());
        $worker = new Worker(
            new Client('https://runtime.test', transport: $transport, workerToken: 'w', workerProtocolVersion: '1.20'),
            'orders',
            workerId: 'worker-1',
            enableCooperativeCancellation: $cooperativeCancellation,
            clock: static fn (): float => $server->now,
            sleeper: static function (int $microseconds) use ($server): void {
                $server->now += $microseconds / 1_000_000;
            },
            diagnosticListener: static function (string $event, array $context, string $level = 'info') use ($events, $retryDelays): void {
                $events[] = [$event, $context['operation'] ?? ''];
                if ($event === 'worker.retrying' && $retryDelays !== null) {
                    $retryDelays[] = [$context['operation'] ?? '', $context['delay_seconds'] ?? null];
                }
            },
        );
        $worker->registerWorkflow('orders.flow', static fn (WorkflowContext $context): string => 'done');

        return $worker;
    }

    #[DataProvider('outages')]
    public function testAWorkflowTaskCompletionWaitsOutAClusterInfoOutageAndIsSentOnce(string $outage): void
    {
        $server = self::server($outage, 30);
        self::worker($server, $log = new \ArrayObject())->run(0);
        $events = $log->getArrayCopy();

        $completions = array_keys($server->paths, '/api/worker/workflow-tasks/workflow-task/complete');
        self::assertCount(1, $completions, 'The completion is sent once, after discovery recovers.');
        self::assertSame(31, $server->clusterInfoReads);
        self::assertContains(['worker.retrying', 'cluster_info'], $events);
        self::assertContains('worker.outage_persisting', array_column($events, 0), 'Outlasting a minute is an error.');
        self::assertNotContains('worker.fatal_failure', array_column($events, 0));
        self::assertSame([], preg_grep('~/fail$~', $server->paths), 'An outage is never reported as a task failure.');
    }

    #[DataProvider('outages')]
    public function testAWorkerStartedDuringAClusterInfoOutageWaitsForIt(string $outage): void
    {
        $server = self::server($outage, 20, ['cooperative_cancellation' => true]);
        $server->completed = true;
        self::worker($server, $log = new \ArrayObject(), cooperativeCancellation: true)->run(0);
        $events = $log->getArrayCopy();

        $register = array_search('/api/worker/register', $server->paths, true);
        self::assertIsInt($register, 'The Worker registered once discovery recovered.');
        self::assertSame(21, count(array_keys(array_slice($server->paths, 0, $register), '/api/cluster/info')));
        self::assertContains(['worker.outage_persisting', 'cluster_info'], $events);
        self::assertNotContains('worker.fatal_failure', array_column($events, 0));
    }

    public function testAClusterInfoOutageWaitsTheRetryAfterHeader(): void
    {
        $server = self::server('503', 2, ['cooperative_cancellation' => true]);
        $server->completed = true;
        self::worker($server, $log = new \ArrayObject(), cooperativeCancellation: true, retryDelays: $delays = new \ArrayObject())->run(0);

        $register = array_search('/api/worker/register', $server->paths, true);
        self::assertIsInt($register, 'The Worker registered once discovery recovered.');
        $discovery = array_values(array_filter($delays->getArrayCopy(), static fn (array $retry): bool => $retry[0] === 'cluster_info'));
        self::assertSame([['cluster_info', 9.0], ['cluster_info', 9.0]], $discovery,
            'Each 503 waits its Retry-After header, above the jittered schedule.');
        self::assertNotContains('worker.fatal_failure', array_column($log->getArrayCopy(), 0));
    }

    public function testAReadOnlyDiscoveryOutageIsTransientButARefusalIsNot(): void
    {
        self::assertTrue(TransientFailure::isReadOutage(new ServerException('x', 500)));
        self::assertTrue(TransientFailure::isReadOutage(new TransportException('x', status: 500)));
        self::assertTrue(TransientFailure::isReadOutage(new ServerException('x', 429)));
        self::assertTrue(TransientFailure::isReadOutage(new TransportException('Connection refused', transientConnectionFailure: true)));
        self::assertFalse(TransientFailure::isReadOutage(new ServerException('x', 500, null, ['retryable' => false])));
        self::assertFalse(TransientFailure::isReadOutage(new ServerException('x', 401)));
        self::assertFalse(TransientFailure::isReadOutage(new ServerException('x', 403)));
        self::assertFalse(TransientFailure::isReadOutage(new ServerException('x', 404)));
    }
}
