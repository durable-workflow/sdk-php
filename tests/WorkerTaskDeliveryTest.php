<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\PollTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\TaskDelivery;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerTaskDeliveryTest extends TestCase
{
    #[DataProvider('kinds')]
    public function testOnlyPositiveAcceptanceReturnsAnExecutableTask(string $kind): void
    {
        $transport = new FakeTransport([self::registration(), self::offer($kind), self::claimed($kind)]);
        $observed = [];
        $client = (new Client('https://server.example', transport: $transport))->withWorkerPayloadObserver(
            static function (string $path, ?array $body, array $response) use (&$observed): ?\Closure {
                if ($response['task'] ?? null) {
                    $observed[] = $path;
                }
                return null;
            });
        $client->registerWorker('worker', 'orders', [], []);
        self::assertSame(self::claimed($kind), self::poll($client, $kind));
        self::assertCount(3, $transport->requests);
        self::assertSame(['/worker/'.$kind.'-tasks/poll'], $observed);
        $initial = $transport->requests[1];
        $accept = $transport->requests[2];
        self::assertStringEndsWith('/worker/'.$kind.'-tasks/offers', $initial['uri']);
        self::assertSame($initial['uri'], $accept['uri']);
        self::assertSame(TaskDelivery::VERSION, $initial['headers'][TaskDelivery::HEADER]);
        self::assertSame($initial['headers'], $accept['headers']);
        self::assertSame(4, $initial['body']['timeout_seconds']);
        self::assertSame(array_replace($initial['body'], ['timeout_seconds' => 0, 'task_offer_token' => 'opaque-first-packet']), $accept['body']);
        self::assertSame(str_repeat('a', 32), $initial['body']['registration_token']);
        self::assertSame(getmypid(), $transport->requests[0]['body']['process_metrics']['process_id']);
        self::assertArrayNotHasKey(TaskDelivery::HEADER, $transport->requests[0]['headers']);
    }

    #[DataProvider('uncertainOutcomes')]
    public function testUncertainRepliesRetryOnlyTheFirstSelectedPacket(TransportException|array|null $outcome): void
    {
        $transport = new FakeTransport([self::registration(), self::offer('workflow'), $outcome, self::claimed('workflow')]);
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        try {
            self::poll($client, 'workflow');
            self::fail('The uncertain acceptance must not authorize execution or start another poll.');
        } catch (TaskDeliveryUncertain $error) {
            self::assertTrue(PollResponse::isTransientFailure($error));
            self::assertSame('fixed-poll', $error->details['poll_request_id']);
            self::assertArrayNotHasKey('task_offer_token', $error->details);
        }
        // A clone and changed wait/page inputs cannot replace an unresolved packet.
        $clone = $client->withWorkerPollCutoffObserver(static function (): void {});
        self::assertSame(self::claimed('workflow'), $clone->pollWorkflowTaskResponse('worker', 'orders', 19, 'fixed-poll', 30));
        self::assertCount(4, $transport->requests);
        self::assertSame($transport->requests[2], $transport->requests[3]);
        self::assertSame('opaque-first-packet', $transport->requests[3]['body']['task_offer_token']);
    }

    public static function uncertainOutcomes(): iterable
    {
        yield 'lost response' => [new TransportException('Connection reset.', transientConnectionFailure: true)];
        yield 'gateway timeout' => [new TransportException('Gateway timeout.', 504)];
        yield 'older node' => [new TransportException('Not found.', 404)];
        yield 'structured backend failure' => [new TransportException('Backend unavailable.', 503,
            ['task' => null, 'reason' => 'backend_unavailable', 'retryable' => true])];
        yield 'lock pressure' => [new TransportException('Lock pressure.', 503,
            ['task' => null, 'reason' => 'backend_lock_pressure', 'poll_status' => 'backend_lock_pressure', 'retry_after_seconds' => 1])];
        yield 'packet unavailable on another node' => [new TransportException('Unavailable.', 409,
            ['task' => null, 'reason' => 'task_offer_unavailable'])];
        yield 'empty is not proof of no claim' => [['task' => null, 'poll_status' => 'empty']];
        yield 'malformed response' => [null];
        yield 'wrong task' => [['task' => ['task_id' => 'other', 'lease_owner' => 'worker']]];
        yield 'wrong owner' => [['task' => ['task_id' => 'task', 'lease_owner' => 'other']]];
    }

    public function testAnOlderNodeCannotDowngradeTheNegotiatedEndpoint(): void
    {
        $transport = new FakeTransport([self::registration(), new TransportException('Not found.', 404), ['task' => null]]);
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        try {
            self::poll($client, 'activity');
            self::fail('Negotiated delivery cannot fall back to a legacy immediate claim.');
        } catch (TaskDeliveryUncertain) {
            self::assertSame(['task' => null], self::poll($client, 'activity'));
        }
        self::assertSame($transport->requests[1], $transport->requests[2]);
        self::assertStringEndsWith('/activity-tasks/offers', $transport->requests[2]['uri']);
    }

    public function testAConfirmedExpiryReleasesTheSelectedPacket(): void
    {
        $transport = new FakeTransport([self::registration(), self::offer('workflow'),
            new TransportException('Expired.', 409, ['task' => null, 'poll_status' => 'task_offer_expired', 'reason' => 'task_offer_expired']),
            self::offer('workflow', 'second-packet'), self::claimed('workflow')]);
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        self::assertSame('empty', self::poll($client, 'workflow')['poll_status']);
        self::assertSame(self::claimed('workflow'), self::poll($client, 'workflow'));
        self::assertArrayNotHasKey('task_offer_token', $transport->requests[3]['body']);
        self::assertSame('second-packet', $transport->requests[4]['body']['task_offer_token']);
    }

    public function testRegistrationCannotDowngradeAnUnresolvedAcceptance(): void
    {
        $transport = new FakeTransport([self::registration(), self::offer('workflow'),
            new TransportException('Gateway timeout.', 504), ['registered' => true], self::claimed('workflow')]);
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        try { self::poll($client, 'workflow'); self::fail('Expected an uncertain acceptance.'); }
        catch (TaskDeliveryUncertain) {}
        try { $client->registerWorker('worker', 'orders', [], []); self::fail('Expected a rejected downgrade.'); }
        catch (ServerException $error) { self::assertSame('invalid_task_delivery_capability', $error->reason); }
        self::assertSame(self::claimed('workflow'), self::poll($client, 'workflow'));
        self::assertSame($transport->requests[2], $transport->requests[4]);
    }

    public function testReplacementRegistrationDiscardsOnlyTheOldIncarnationsPacket(): void
    {
        $replacement = self::registration();
        $replacement['registration_token'] = str_repeat('b', 32);
        $transport = new FakeTransport([self::registration(), self::offer('activity'),
            new TransportException('Gateway timeout.', 504), $replacement, self::offer('activity', 'replacement-packet'), self::claimed('activity')]);
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        try { self::poll($client, 'activity'); self::fail('Expected an uncertain acceptance.'); }
        catch (TaskDeliveryUncertain) {}
        $client->registerWorker('worker', 'orders', [], []);
        self::assertSame(self::claimed('activity'), self::poll($client, 'activity'));
        self::assertSame(str_repeat('b', 32), $transport->requests[4]['body']['registration_token']);
        self::assertArrayNotHasKey('task_offer_token', $transport->requests[4]['body']);
        self::assertSame('replacement-packet', $transport->requests[5]['body']['task_offer_token']);
    }

    #[DataProvider('legacyRegistrations')]
    public function testLegacyDeliveryIsChosenOnlyAtRegistration(array $registration, string $protocol, ?array $capabilities): void
    {
        $transport = new FakeTransport([$registration, ['task' => null]]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: $protocol);
        if ($capabilities === null) {
            $client->registerWorker('worker', 'orders', [], []);
        } else {
            $client->registerWorker('worker', 'orders', [], [], $capabilities);
        }
        self::poll($client, 'workflow');
        self::assertStringEndsWith('/workflow-tasks/poll', $transport->requests[1]['uri']);
        self::assertArrayNotHasKey(TaskDelivery::HEADER, $transport->requests[1]['headers']);
        if ($protocol === '1.19') {
            self::assertNotContains(TaskDelivery::CAPABILITY, $transport->requests[0]['body']['capabilities']);
        }
    }

    public static function legacyRegistrations(): iterable
    {
        yield 'no server feature' => [['registered' => true], '1.20', null];
        yield 'explicit worker opt-out' => [self::registration(), '1.20', ['query_tasks']];
        yield 'older protocol' => [self::registration(), '1.19', null];
    }

    public function testAContradictoryOfferCannotReachPayloadOrExecutionObservers(): void
    {
        $contradictory = self::claimed('workflow') + ['task_offer' => self::offer('workflow')['task_offer']];
        $transport = new FakeTransport([self::registration(), $contradictory]);
        $claims = 0;
        $client = (new Client('https://server.example', transport: $transport))->withWorkerPayloadObserver(
            static function (string $path, ?array $body, array $response) use (&$claims): ?\Closure {
                if ($response['task'] ?? null) { ++$claims; }
                return null;
            });
        $client->registerWorker('worker', 'orders', [], []);
        try {
            self::poll($client, 'workflow');
            self::fail('A contradictory offer cannot authorize a task.');
        } catch (ServerException $error) {
            self::assertSame('invalid_task_delivery_response', $error->reason);
        }
        self::assertSame(0, $claims);
        self::assertCount(2, $transport->requests);
    }

    public function testTheManagedWorkerRetainsItsLogicalPollAfterAnUncertainAcceptance(): void
    {
        $now = 0.0;
        $retries = [];
        $transport = new FakeTransport([self::registration(), self::offer('workflow'),
            new TransportException('Not found.', 404),
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']]);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            transientPollRetryObserver: static function (string $kind, int $attempt, float $delay, ServerException $error) use (&$retries): void {
                $retries[] = [$kind, $attempt, $error::class];
            });
        $worker->run(4);
        self::assertSame([['workflow', 1, TaskDeliveryUncertain::class]], $retries);
        self::assertSame($transport->requests[2], $transport->requests[3]);
        self::assertContains(TaskDelivery::CAPABILITY, $transport->requests[0]['body']['capabilities']);
        self::assertCount(4, $transport->requests);
    }

    public function testOfferTransportUsesTheSameProgressAndLogicalPathForAllKinds(): void
    {
        foreach (['workflow', 'activity', 'query', 'update-validation'] as $kind) {
            $path = '/worker/'.$kind.'-tasks/offers';
            self::assertSame($kind, PollTransport::taskKind('POST', $path));
            self::assertSame('/worker/'.$kind.'-tasks/poll', PollTransport::logicalPath($path));
            self::assertNull(PollTransport::taskKind('GET', $path));
        }
    }

    public static function kinds(): array { return [['workflow'], ['activity'], ['query']]; }

    private static function poll(Client $client, string $kind): array
    {
        return match ($kind) {
            'workflow' => $client->pollWorkflowTaskResponse('worker', 'orders', 4, 'fixed-poll'),
            'activity' => $client->pollActivityTaskResponse('worker', 'orders', 4, 'fixed-poll'),
            'query' => $client->pollQueryTaskResponse('worker', 'orders', 4, 'fixed-poll'),
        };
    }

    private static function offer(string $kind, string $token = 'opaque-first-packet'): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => $kind, 'task_id' => 'task',
            'token' => $token, 'expires_at_ms' => 1000,
        ]];
    }

    private static function claimed(string $kind): array
    {
        return ['task' => [($kind === 'query' ? 'query_task_id' : 'task_id') => 'task', 'lease_owner' => 'worker', 'task_kind' => $kind]];
    }

    private static function registration(): array
    {
        return ['registered' => true, 'registration_token' => str_repeat('a', 32), 'heartbeat_interval_seconds' => 10,
            'server_capabilities' => ['task_delivery' => [
                'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
                'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER,
                'version' => TaskDelivery::VERSION, 'endpoint_suffix' => '/offers',
                'acceptance_timeout_seconds' => 2, 'unaccepted_offer_reserves_task' => false,
            ], 'worker_deregistration_fencing' => ['schema' => 'durable-workflow.v2.worker-deregistration.v1',
                'supported' => true, 'endpoint' => '/worker/registrations/{workerId}/deregister', 'receipt_retention_seconds' => 600]]];
    }
}
