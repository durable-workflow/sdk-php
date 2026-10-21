<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestBudget;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class ActivityLeaseRenewalClientTest extends TestCase
{
    public function testRenewalUsesOnlyWorkerCredentialsAndOriginalClaimWithoutUserProgress(): void
    {
        $requests = [];
        $ack = ['lease_renewed' => true, 'heartbeat_recorded' => false, 'original' => 'retained'];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$requests, $ack) {
            $requests[] = ['request' => $request, 'timeout' => $options['timeout']];

            return Create::promiseFor(new Response(200, [], json_encode(
                count($requests) === 1 ? self::discovery(true) : $ack, JSON_THROW_ON_ERROR,
            )));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http),
            namespace: 'tenant-a', controlToken: 'control', workerToken: 'worker');

        self::assertSame($ack, $client->renewActivityTaskLease('task/0', 'attempt/17', 'owner'));
        self::assertCount(2, $requests);
        self::assertSame('https://server.example/api/cluster/info', (string) $requests[0]['request']->getUri());
        self::assertSame('https://server.example/api/worker/activity-tasks/task%2F0/renew-lease', (string) $requests[1]['request']->getUri());
        self::assertSame(['activity_attempt_id' => 'attempt/17', 'lease_owner' => 'owner'],
            json_decode((string) $requests[1]['request']->getBody(), true, 512, JSON_THROW_ON_ERROR));
        foreach ($requests as $call) {
            self::assertSame('Bearer worker', $call['request']->getHeaderLine('Authorization'));
            self::assertSame('tenant-a', $call['request']->getHeaderLine('X-Namespace'));
            self::assertSame('1.20', $call['request']->getHeaderLine('X-Durable-Workflow-Protocol-Version'));
            self::assertFalse($call['request']->hasHeader('X-Durable-Workflow-Control-Plane-Version'));
            self::assertGreaterThan(0, $call['timeout']);
            self::assertLessThanOrEqual(5, $call['timeout']);
        }
    }

    #[DataProvider('incapableRuntimeProvider')]
    public function testOnlyAffirmativeCapabilityAllowsRenewal(array $discovery): void
    {
        $calls = 0;
        $http = new GuzzleClient(['handler' => static function () use (&$calls, $discovery) {
            ++$calls;

            return Create::promiseFor(new Response(200, [], json_encode($discovery, JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner');
            self::fail('An unavailable or malformed capability allowed activity renewal.');
        } catch (LogicException $error) {
            self::assertStringContainsString('activity_lease_renewal', $error->getMessage());
        }
        self::assertSame(1, $calls, 'Capability refusal must not submit a renewal.');
    }

    public static function incapableRuntimeProvider(): array
    {
        return [[self::discovery(false)], [self::discovery(null)], [self::discovery('true')],
            [self::discovery(1)], [self::discovery([])], [[]], [['worker_protocol' => 'malformed']],
            [['worker_protocol' => ['server_capabilities' => true]]]];
    }

    public function testExistingProtocolWithAffirmativeCapabilityAddsNoNewProtocolFloor(): void
    {
        $http = new GuzzleClient(['handler' => static fn (): mixed => Create::promiseFor(
            new Response(200, [], json_encode([...self::discovery(true), 'lease_renewed' => true], JSON_THROW_ON_ERROR)),
        )]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http), workerProtocolVersion: '1.19');

        self::assertTrue($client->renewActivityTaskLease('0', '017', '17')['lease_renewed']);
    }

    #[DataProvider('invalidIdentityProvider')]
    public function testInvalidIdentityRefusesBeforeDiscovery(string $task, string $attempt, string $owner): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->renewActivityTaskLease($task, $attempt, $owner);
            self::fail('An invalid claim allowed discovery.');
        } catch (InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public static function invalidIdentityProvider(): array
    {
        return [['', 'attempt', 'owner'], ['task', ' ', 'owner'], ['task', 'attempt', ''],
            [str_repeat('x', 256), 'attempt', 'owner'], ['task', str_repeat('x', 256), 'owner'],
            ['task', 'attempt', str_repeat('x', 256)]];
    }

    public function testUnboundedTransportCannotClaimToMaintainALease(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner');
            self::fail('A transport without bounded I/O allowed renewal.');
        } catch (InvalidArgumentException $error) {
            self::assertStringContainsString('bounded', $error->getMessage());
        }
        self::assertSame([], $transport->requests);
    }

    public function testExpiredOriginalBudgetCannotDiscoverOrRenew(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        $budget = new RequestBudget(5, hrtime(true) / 1e9 - 1);
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner', $budget);
            self::fail('An expired original authority allowed I/O.');
        } catch (TransportException $error) {
            self::assertFalse($error->transientConnectionFailure);
        }
        self::assertSame([], $transport->requests);
    }

    public function testWideCallerBudgetIsRestrictedToFiveSeconds(): void
    {
        $timeouts = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request, array $options) use (&$timeouts) {
            $timeouts[] = $options['timeout'];

            return Create::promiseFor(new Response(200, [], json_encode(self::discovery(true), JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        $client->renewActivityTaskLease('task', 'attempt', 'owner', new RequestBudget(65));

        self::assertCount(2, $timeouts);
        foreach ($timeouts as $timeout) {
            self::assertGreaterThan(0, $timeout);
            self::assertLessThanOrEqual(5, $timeout);
        }
    }

    public function testDiscoveryCannotGetAFreshRequestBudgetAfterConsumingTheOriginalWindow(): void
    {
        $calls = 0;
        $http = new GuzzleClient(['handler' => static function () use (&$calls) {
            ++$calls;
            usleep(1100000);

            return Create::promiseFor(new Response(200, [], json_encode(self::discovery(true), JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        $budget = new RequestBudget(1);
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner', $budget);
            self::fail('Discovery refreshed the expired caller window.');
        } catch (ServerException $error) {
            self::assertStringContainsString('request time budget expired', $error->getMessage());
        }
        self::assertSame(1, $calls);
    }

    public function testSuccessfulResponseAfterTheOriginalWindowDoesNotReturnAGrant(): void
    {
        $calls = 0;
        $http = new GuzzleClient(['handler' => static function () use (&$calls) {
            if (++$calls === 2) {
                usleep(1100000);
            }

            return Create::promiseFor(new Response(200, [], json_encode(
                $calls === 1 ? self::discovery(true) : ['lease_renewed' => true], JSON_THROW_ON_ERROR,
            )));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        $budget = new RequestBudget(1);
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner', $budget);
            self::fail('A late successful response acquired fresh execution authority.');
        } catch (ServerException $error) {
            self::assertStringContainsString('request time budget expired', $error->getMessage());
        }
        self::assertSame(2, $calls);
    }

    public function testExpiredLeaseRefusalIsRetainedWithoutRetryingAnotherClaim(): void
    {
        $calls = 0;
        $http = new GuzzleClient(['handler' => static function () use (&$calls) {
            ++$calls;

            return Create::promiseFor($calls === 1
                ? new Response(200, [], json_encode(self::discovery(true), JSON_THROW_ON_ERROR))
                : new Response(409, [], '{"reason":"lease_expired","message":"Original lease expired"}'));
        }]);
        $client = new Client('https://server.example', transport: new Psr18Transport($http));
        try {
            $client->renewActivityTaskLease('task', 'attempt', 'owner');
            self::fail('An expired lease acquired execution authority.');
        } catch (ServerException $error) {
            self::assertSame(409, $error->status);
            self::assertSame('lease_expired', $error->reason);
        }
        self::assertSame(2, $calls);
    }

    private static function discovery(mixed $capability): array
    {
        return ['worker_protocol' => ['server_capabilities' => ['activity_lease_renewal' => $capability]]];
    }
}
