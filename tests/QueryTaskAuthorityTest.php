<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\QueryTaskAuthority;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowClaimRevoked;
use GuzzleHttp\Client as Http;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class QueryTaskAuthorityTest extends TestCase
{
    public function testExpiredOriginalDoesNotCallTheHandlerOrPublishAnAcknowledgement(): void
    {
        foreach (['expired-lease', 'expired-caller'] as $case) {
            $transport = new FakeTransport([[]]);
            $worker = new Worker(new Client('http://runtime', transport: $transport), 'queue', workerId: 'worker-1');
            $calls = 0;
            $worker->registerQuery('query.workflow', 'value', static function (QueryContext $context) use (&$calls): int {
                ++$calls;
                return 7;
            });
            $task = self::task(-2);
            if ($case === 'expired-caller') {
                $task['lease_expires_at'] = self::time(60);
            }
            (new \ReflectionMethod($worker, 'executeQueryTask'))->invoke($worker, $task);
            self::assertSame(0, $calls, $case);
            self::assertSame([], $transport->requests, $case);
        }
    }

    public function testLegacyDeliveryWithoutDisclosedTimingRetainsItsExplicitlyWeakerBehavior(): void
    {
        $transport = new FakeTransport([[]]);
        $worker = new Worker(new Client('http://runtime', transport: $transport), 'queue', workerId: 'worker-1');
        $worker->registerQuery('query.workflow', 'value', static fn (QueryContext $context): int => 7);
        $task = self::task();
        foreach (['lease_expires_at', 'query_admitted_at', 'query_deadline_at', 'server_time'] as $key) {
            unset($task[$key]);
        }
        (new \ReflectionMethod($worker, 'executeQueryTask'))->invoke($worker, $task);
        self::assertCount(1, $transport->requests);
        self::assertStringEndsWith('/query-1/complete', $transport->requests[0]['uri']);
        self::assertSame(7, $transport->requests[0]['body']['result']);
    }

    public function testNonrenewingObservationRequiresTheSameClaimAndOriginalTimes(): void
    {
        $task = self::task();
        foreach ([['query_task_attempt' => true], ['query_task_id' => 'other-query'],
            ['lease_owner' => 'other-worker'], ['query_deadline_at' => self::time(120)],
            ['lease_expires_at' => self::time(120)], ['can_continue' => false],
            ['query_task_status' => 'completed'], ['server_time' => 'not-a-time']] as $changed) {
            $reply = array_replace($task, ['can_continue' => true, 'query_task_status' => 'leased', 'reason' => null], $changed);
            $reads = 0;
            $client = self::client(static function (RequestInterface $request, array $options) use ($reply, &$reads): array {
                ++$reads;
                self::assertSame('GET', $request->getMethod());
                self::assertSame('/api/worker/query-tasks/query-1/status', $request->getUri()->getPath());
                parse_str($request->getUri()->getQuery(), $query);
                self::assertSame(['lease_owner' => 'worker-1', 'query_task_attempt' => '1'], $query);
                self::assertLessThanOrEqual(5, $options['timeout']);
                return $reply;
            });
            $authority = new QueryTaskAuthority($client, $task, 'worker-1', 'queue', true);
            try {
                $authority->confirm();
                self::fail('A changed or closed query claim was accepted.');
            } catch (WorkflowClaimRevoked $error) {
                self::assertContains($error->reason, ['query_authority_unavailable', 'query_claim_unavailable']);
            }
            self::assertSame(1, $reads);
        }
    }

    public function testFreshServerTimeCanOnlyShortenTheOriginalMonotonicBudget(): void
    {
        $task = self::task(60);
        $reads = 0;
        $client = self::client(static function () use ($task, &$reads): array {
            $serverTime = (new DateTimeImmutable($task['server_time']))->modify(++$reads === 1 ? '+40 seconds' : '-40 seconds');
            return array_replace($task, ['can_continue' => true, 'query_task_status' => 'leased', 'reason' => null,
                'server_time' => $serverTime->format('Y-m-d\TH:i:s.u\Z')]);
        });
        $authority = new QueryTaskAuthority($client, $task, 'worker-1', 'queue', true);
        $authority->confirm();
        $shortened = $authority->remaining();
        self::assertLessThanOrEqual(20.1, $shortened);
        $authority->confirm();
        self::assertLessThanOrEqual($shortened, $authority->remaining());
        self::assertSame(2, $reads);
    }

    public function testLoadedClaimMustRetainItsNamespaceIdentityAndTiming(): void
    {
        $task = self::task();
        $authority = new QueryTaskAuthority(new Client('http://runtime'), $task, 'worker-1', 'queue', false);
        self::assertTrue($authority->matches($task));
        foreach (['namespace' => 'other', 'run_id' => 'other-run', 'workflow_id' => 'other-workflow',
            'task_queue' => 'other-queue', 'query_task_attempt' => true, 'lease_expires_at' => self::time(120)] as $key => $value) {
            self::assertFalse($authority->matches(array_replace($task, [$key => $value])), $key);
        }
    }

    public function testProtocol119DoesNotCallTheStatusEndpoint(): void
    {
        $transport = new FakeTransport();
        $client = new Client('http://runtime', transport: $transport, workerProtocolVersion: '1.19');
        try {
            $client->queryTaskStatus('query-1', 'worker-1', 1);
            self::fail('Protocol 1.19 issued a status request.');
        } catch (\InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public function testOrdinaryUnboundedTransportKeepsLocalTimingWithoutClaimingLiveObservation(): void
    {
        $task = self::task();
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($task): array {
            return match (true) {
                str_ends_with($uri, '/register') => ['server_capabilities' => ['query_task_authority' => true]],
                str_ends_with($uri, '/query-tasks/poll') => ['task' => $task],
                default => [],
            };
        });
        $worker = new Worker(new Client('http://runtime', transport: $transport), 'queue', workerId: 'worker-1');
        $calls = 0;
        $worker->registerQuery('query.workflow', 'value', static function (QueryContext $context) use ($worker, &$calls): int {
            ++$calls;
            $worker->requestShutdown();
            return 7;
        });
        $worker->run(0);
        self::assertSame(1, $calls);
        self::assertCount(1, array_filter($transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/query-1/complete')));
        self::assertSame([], array_filter($transport->requests,
            static fn (array $request): bool => str_contains($request['uri'], '/status')));
    }

    public function testRequiredAuthorityRefusesUnsupportedDiscoveryBeforeRegistration(): void
    {
        $requests = [];
        $client = self::client(static function (RequestInterface $request) use (&$requests): array {
            $requests[] = [$request->getMethod(), $request->getUri()->getPath()];
            return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => ['query_task_authority' => false]]];
        });
        $worker = new Worker($client, 'queue', requireQueryTaskAuthority: true);
        try {
            $worker->run(0);
            self::fail('Unsupported runtime registered a strong-authority Worker.');
        } catch (WorkflowClaimAborted) {
            self::assertSame([['GET', '/api/cluster/info']], $requests);
        }
    }

    public function testRequiredAuthorityRefusesUnsupervisedPayloadTransportBeforeAnyRequest(): void
    {
        $transport = new class implements \DurableWorkflow\Transport\Transport,
            \DurableWorkflow\Transport\BoundedTransport, \DurableWorkflow\Transport\BoundedPayloadTransport {
            public int $requests = 0;
            public function supportsBoundedRequests(): bool { return true; }
            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            { ++$this->requests; return []; }
            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
            { ++$this->requests; return []; }
            public function fetchPayload(string $uri, array $headers, int $maxBytes): string
            { ++$this->requests; return ''; }
            public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string
            { ++$this->requests; return ''; }
        };
        $worker = new Worker(new Client('http://runtime', transport: $transport), 'queue', requireQueryTaskAuthority: true);
        try {
            $worker->run(0);
            self::fail('A payload transport without authority supervision registered.');
        } catch (WorkflowClaimAborted) {
            self::assertSame(0, $transport->requests);
        }
    }

    /** @return array<string, mixed> */
    private static function task(int $offset = 30): array
    {
        $deadline = self::time($offset);
        return ['payload_codec' => 'avro', 'query_task_id' => 'query-1', 'query_task_attempt' => 1, 'lease_owner' => 'worker-1',
            'workflow_id' => 'workflow-1', 'run_id' => 'run-1', 'task_queue' => 'queue',
            'workflow_type' => 'query.workflow', 'query_name' => 'value', 'history_events' => [],
            'query_arguments' => (new AvroPayloadCodec())->envelope([]),
            'query_admitted_at' => self::time(-60), 'query_deadline_at' => $deadline,
            'lease_expires_at' => $deadline, 'server_time' => self::time(0)];
    }

    private static function time(int $offset): string
    {
        return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->modify(sprintf('%+d seconds', $offset))
            ->format('Y-m-d\TH:i:s.u\Z');
    }

    private static function client(\Closure $respond): Client
    {
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use ($respond): FulfilledPromise {
            return new FulfilledPromise(new Response(200, [], json_encode($respond($request, $options), JSON_THROW_ON_ERROR)));
        }]);

        return new Client('http://runtime', transport: new Psr18Transport($http));
    }
}
