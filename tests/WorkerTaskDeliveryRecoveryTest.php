<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\TaskDeliveryRefusal;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\TaskDelivery;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use ReflectionProperty;

/** Recovery paths that must release, fall back or retry instead of wedging the worker. */
final class WorkerTaskDeliveryRecoveryTest extends TestCase
{
    /** Native Server #1291 rolls these acceptances back; a peer may already hold the task. */
    #[DataProvider('nativeRefusals')]
    public function testRolledBackAcceptanceReleasesThePacketOverTheRealTransport(string $reason, string $endpoint, string $kind): void
    {
        $requests = [];
        $acceptances = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use (&$requests, &$acceptances, $reason, $kind) {
            $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
            $requests[] = ['uri' => (string) $request->getUri(), 'body' => $body];
            if (str_ends_with((string) $request->getUri(), '/worker/register')) {
                return Create::promiseFor(self::json(200, self::registration()));
            }
            if (!isset($body['task_offer_token'])) {
                return Create::promiseFor(self::json(200, self::offer($kind)));
            }
            return Create::promiseFor(++$acceptances === 1
                ? self::json(409, ['task' => null, 'poll_status' => $reason, 'reason' => $reason])
                : self::json(200, self::claimed($kind)));
        }]);
        $observed = 0;
        $client = (new Client('https://server.example', transport: new Psr18Transport(client: $http)))->withWorkerPayloadObserver(
            static function (string $path, ?array $body, array $response) use (&$observed): ?\Closure {
                if ($response['task'] ?? null) { ++$observed; }
                return null;
            });
        $client->registerWorker('worker', 'orders', [], []);

        self::assertSame(['task' => null, 'poll_status' => 'empty', 'reason' => $reason], self::poll($client, $endpoint, $kind, 'original'));
        self::assertSame(0, $observed, 'A rolled-back acceptance must not run the task.');
        self::assertCount(3, $requests);

        // The packet is released: even the same identity starts a new offer read.
        self::assertSame(self::claimed($kind), self::poll($client, $endpoint, $kind, 'original'));
        self::assertArrayNotHasKey('task_offer_token', $requests[3]['body']);
        self::assertStringEndsWith('/'.$endpoint.'-tasks/offers', $requests[3]['uri']);
        self::assertSame(1, $observed);
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function nativeRefusals(): iterable
    {
        foreach (['task_offer_unclaimable', 'task_offer_lease_ended', 'task_offer_conflict'] as $reason) {
            foreach ([['workflow', 'workflow'], ['workflow', 'update_validation'], ['activity', 'activity'], ['query', 'query'], ['update-validation', 'update_validation']] as [$endpoint, $kind]) {
                yield $reason.' / '.$endpoint.' / '.$kind => [$reason, $endpoint, $kind];
            }
        }
    }

    #[DataProvider('nativeReasons')]
    public function testManagedWorkerStartsANewLogicalPollAfterARolledBackAcceptance(string $reason): void
    {
        $now = 0.0;
        $retries = [];
        $offers = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($reason, &$offers): array {
            if (str_ends_with($uri, '/worker/register')) { return self::workerRegistration(); }
            if (isset($body['task_offer_token'])) { throw self::wireRefusal($reason, $reason); }
            // The second logical poll ends the run through the ordinary terminal contract.
            return ++$offers === 1 ? self::offer('workflow') : self::exactBody('stale_worker_registration', 'worker_registration_lost_authority');
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            transientPollRetryObserver: static function (string $kind, int $attempt, float $delay, ServerException $error) use (&$retries): void {
                $retries[] = $error::class;
            }, stopOnStaleRegistration: true);
        $worker->run(4);

        self::assertSame([], $retries, 'A definitive refusal is not an uncertain outcome.');
        $polls = array_values(array_filter($transport->requests, static fn (array $request): bool => str_ends_with($request['uri'], '/offers')));
        self::assertCount(3, $polls);
        self::assertArrayHasKey('task_offer_token', $polls[1]['body']);
        self::assertArrayNotHasKey('task_offer_token', $polls[2]['body']);
        self::assertNotSame($polls[0]['body']['poll_request_id'], $polls[2]['body']['poll_request_id']);
    }

    /** @return iterable<string, array{string}> */
    public static function nativeReasons(): iterable
    {
        foreach (['task_offer_unclaimable', 'task_offer_lease_ended', 'task_offer_conflict'] as $reason) {
            yield $reason => [$reason];
        }
    }

    /** The producer's enveloped variant is not the exact definitive shape; the bounded window releases it. */
    public function testEnvelopedRefusalRemainsUncertainUntilTheBoundedWindow(): void
    {
        $monotonic = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(); }
            if (!isset($body['task_offer_token'])) { return self::offer('workflow'); }
            $reply = ['task' => null, 'poll_status' => 'task_offer_unclaimable', 'reason' => 'task_offer_unclaimable',
                'protocol_version' => '1.20', 'server_capabilities' => []];
            throw new TransportException('Task acceptance is not confirmed.', 409, $reply,
                taskDeliveryRefusal: TaskDeliveryRefusal::fromJson(409, json_encode($reply, JSON_THROW_ON_ERROR)));
        });
        $client = self::clientWithClock($transport, $monotonic, 0);
        $client->registerWorker('worker', 'orders', [], []);
        try { self::poll($client, 'workflow', 'workflow', 'original'); self::fail('An enveloped refusal is not definitive.'); }
        catch (TaskDeliveryUncertain) {}
        $monotonic = 2_000 + TaskDelivery::UNCONFIRMED_GRACE_MS;
        self::assertSame(['task' => null, 'poll_status' => 'empty', 'reason' => TaskDelivery::UNCONFIRMED_REASON],
            self::poll($client, 'workflow', 'workflow', 'original'));
    }

    /**
     * Uncertain acceptances are retried until the packet deadline plus grace, then released. The
     * negotiated timeout caps a worker clock behind the Server; the grace covers one ahead of it.
     */
    #[DataProvider('packetDeadlines')]
    public function testUncertainAcceptanceIsReleasedAfterThePacketDeadlineAndGrace(int $expiresAtMs, int $releaseAfterMs): void
    {
        $monotonic = 1_000_000;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($expiresAtMs): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(); }
            if (!isset($body['task_offer_token'])) { return self::offer('workflow', $expiresAtMs); }
            throw new TransportException('Gateway timeout.', 504);
        });
        $client = self::clientWithClock($transport, $monotonic, 50_000);
        $client->registerWorker('worker', 'orders', [], []);

        try { self::poll($client, 'workflow', 'workflow', 'original'); self::fail('The first lost reply is uncertain.'); }
        catch (TaskDeliveryUncertain) {}
        $monotonic = 1_000_000 + $releaseAfterMs - 1;
        try { self::poll($client, 'workflow', 'workflow', 'original'); self::fail('Still inside the bounded window.'); }
        catch (TaskDeliveryUncertain) {}
        $monotonic = 1_000_000 + $releaseAfterMs;
        self::assertSame(['task' => null, 'poll_status' => 'empty', 'reason' => TaskDelivery::UNCONFIRMED_REASON],
            self::poll($client, 'workflow', 'workflow', 'original'));

        $accepts = array_values(array_filter($transport->requests, static fn (array $request): bool => isset($request['body']['task_offer_token'])));
        self::assertCount(3, $accepts, 'The release follows one more attempt with the same packet.');
        self::assertSame($accepts[0], $accepts[2]);
        try { self::poll($client, 'workflow', 'workflow', 'original'); self::fail('The new packet is uncertain too.'); }
        catch (TaskDeliveryUncertain) {}
        $last = array_slice($transport->requests, -2);
        self::assertArrayNotHasKey('task_offer_token', $last[0]['body'], 'A released packet is never resent.');
    }

    /** @return iterable<string, array{int, int}> */
    public static function packetDeadlines(): iterable
    {
        yield 'deadline inside the negotiated timeout' => [51_500, 1_500 + TaskDelivery::UNCONFIRMED_GRACE_MS];
        yield 'worker clock behind: capped by the negotiated timeout' => [3_600_000, 2_000 + TaskDelivery::UNCONFIRMED_GRACE_MS];
        yield 'worker clock ahead: grace only' => [1_000, TaskDelivery::UNCONFIRMED_GRACE_MS];
    }

    public function testManagedWorkerLeavesAnUnconfirmedAcceptanceAfterTheBoundedWindow(): void
    {
        $now = 0.0;
        $retries = [];
        $offers = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$offers): array {
            if (str_ends_with($uri, '/worker/register')) { return self::workerRegistration(); }
            if (str_ends_with($uri, '/heartbeat')) { return ['worker_id' => 'worker', 'acknowledged' => true]; }
            if (isset($body['task_offer_token'])) { throw new TransportException('Gateway timeout.', 504); }
            return ++$offers === 1 ? self::offer('workflow') : self::exactBody('stale_worker_registration', 'worker_registration_lost_authority');
        });
        $client = new Client('https://server.example', transport: $transport);
        (new ReflectionProperty(Client::class, 'taskDelivery'))->setValue($client, new TaskDelivery(
            static function () use (&$now): int { return (int) round($now * 1000); }, static fn (): int => 0));
        $worker = new Worker($client, 'orders', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            transientPollRetryObserver: static function (string $kind, int $attempt, float $delay, ServerException $error) use (&$retries): void {
                $retries[] = $error::class;
                if ($attempt > 100) { throw new \RuntimeException('The unconfirmed acceptance was never released.'); }
            }, stopOnStaleRegistration: true);
        $worker->run(4);

        self::assertNotEmpty($retries);
        self::assertSame([TaskDeliveryUncertain::class], array_values(array_unique($retries)));
        self::assertGreaterThanOrEqual(TaskDelivery::UNCONFIRMED_GRACE_MS / 1000, $now);
        $polls = array_values(array_filter($transport->requests, static fn (array $request): bool =>
            str_ends_with($request['uri'], '/offers') && !isset($request['body']['task_offer_token'])));
        self::assertCount(2, $polls);
        self::assertNotSame($polls[0]['body']['poll_request_id'], $polls[1]['body']['poll_request_id']);
    }

    /** Server #1291: a replica without the packet's seal key cannot say whether a claim committed. */
    public function testReadKeyUnavailableKeepsThePacketForAReplayOverTheRealTransport(): void
    {
        $requests = [];
        $acceptances = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use (&$requests, &$acceptances) {
            $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
            $requests[] = ['uri' => (string) $request->getUri(), 'body' => $body];
            if (str_ends_with((string) $request->getUri(), '/worker/register')) {
                return Create::promiseFor(self::json(200, self::registration()));
            }
            if (!isset($body['task_offer_token'])) {
                return Create::promiseFor(self::json(200, self::offer('activity')));
            }
            return Create::promiseFor(++$acceptances <= 2
                ? self::json(503, ['reason' => 'task_offer_read_key_unavailable', 'message' => 'task_offer_read_key_unavailable'])
                : self::json(200, self::claimed('activity')));
        }]);
        $observed = 0;
        $client = (new Client('https://server.example', transport: new Psr18Transport(client: $http)))->withWorkerPayloadObserver(
            static function (string $path, ?array $body, array $response) use (&$observed): ?\Closure {
                if ($response['task'] ?? null) { ++$observed; }
                return null;
            });
        $client->registerWorker('worker', 'orders', [], []);
        foreach ([1, 2] as $attempt) {
            try {
                self::poll($client, 'activity', 'activity', 'original');
                self::fail('A read-key outage is not definitive.');
            } catch (TaskDeliveryUncertain $error) {
                self::assertSame('original', $error->details['poll_request_id']);
                self::assertSame(503, $error->status);
            }
        }
        self::assertSame(0, $observed);
        self::assertSame(self::claimed('activity'), self::poll(clone $client, 'activity', 'activity', 'original'));
        self::assertSame(1, $observed);
        self::assertCount(5, $requests);
        self::assertSame($requests[2], $requests[3]);
        self::assertSame($requests[2], $requests[4], 'Each replay keeps the selected packet and identity.');
    }

    public function testManagedWorkerReplaysAfterReadKeyUnavailable(): void
    {
        $now = 0.0;
        $retries = [];
        $acceptances = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$acceptances): array {
            if (str_ends_with($uri, '/worker/register')) { return self::workerRegistration(); }
            if (!isset($body['task_offer_token'])) { return self::offer('workflow'); }
            if (++$acceptances <= 2) {
                throw new TransportException('Task acceptance is not confirmed.', 503,
                    ['reason' => 'task_offer_read_key_unavailable', 'message' => 'task_offer_read_key_unavailable']);
            }
            throw self::wireRefusal('stale_worker_registration', 'worker_registration_lost_authority');
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            transientPollRetryObserver: static function (string $kind, int $attempt, float $delay, ServerException $error) use (&$retries): void {
                $retries[] = [$kind, $attempt, $error::class, $error->getPrevious() instanceof ServerException ? $error->getPrevious()->reason : null];
            }, stopOnStaleRegistration: true);
        $worker->run(4);

        self::assertSame([
            ['workflow', 1, TaskDeliveryUncertain::class, 'task_offer_read_key_unavailable'],
            ['workflow', 2, TaskDeliveryUncertain::class, 'task_offer_read_key_unavailable'],
        ], $retries);
        $offers = array_values(array_filter($transport->requests, static fn (array $request): bool => str_ends_with($request['uri'], '/offers')));
        self::assertCount(4, $offers, 'One offer read, then the same acceptance three times.');
        self::assertSame($offers[1], $offers[2]);
        self::assertSame($offers[1], $offers[3]);
    }

    public function testPersistentReadKeyOutageIsReleasedOnlyAfterTheBoundedWindow(): void
    {
        $monotonic = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(); }
            if (!isset($body['task_offer_token'])) { return self::offer('workflow', 2_000); }
            throw new TransportException('Task acceptance is not confirmed.', 503,
                ['reason' => 'task_offer_read_key_unavailable', 'message' => 'task_offer_read_key_unavailable']);
        });
        $client = self::clientWithClock($transport, $monotonic, 0);
        $client->registerWorker('worker', 'orders', [], []);
        foreach ([0, 2_000 + TaskDelivery::UNCONFIRMED_GRACE_MS - 1] as $at) {
            $monotonic = $at;
            try { self::poll($client, 'workflow', 'workflow', 'original'); self::fail('Still inside the bounded window.'); }
            catch (TaskDeliveryUncertain) {}
        }
        $monotonic = 2_000 + TaskDelivery::UNCONFIRMED_GRACE_MS;
        self::assertSame(['task' => null, 'poll_status' => 'empty', 'reason' => TaskDelivery::UNCONFIRMED_REASON],
            self::poll($client, 'workflow', 'workflow', 'original'));
        $accepts = array_values(array_filter($transport->requests, static fn (array $request): bool => isset($request['body']['task_offer_token'])));
        self::assertCount(3, $accepts);
        self::assertSame($accepts[0], $accepts[2]);
    }

    /** Nothing was selected, so a node without the offer route is not an uncertain claim. */
    #[DataProvider('missingRoutes')]
    public function testMissingOfferRouteBeforeSelectionFallsBackToLegacyPolling(int $status): void
    {
        $registrations = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($status, &$registrations): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(str_repeat(++$registrations === 1 ? 'a' : 'b', 32)); }
            if (str_ends_with($uri, '/offers')) {
                if (!isset($body['task_offer_token']) && $registrations === 1) { throw new TransportException('Not found.', $status, ['message' => 'Not Found']); }
                return isset($body['task_offer_token']) ? self::claimed('workflow') : self::offer('workflow');
            }
            return ['task' => null, 'poll_status' => 'empty'];
        });
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);

        self::assertSame(['task' => null, 'poll_status' => 'empty'], self::poll($client, 'workflow', 'workflow', 'first'));
        self::assertSame(['task' => null, 'poll_status' => 'empty'], self::poll($client, 'workflow', 'workflow', 'second'));
        $uris = array_map(static fn (array $request): string => substr($request['uri'], strlen('https://server.example/api')), $transport->requests);
        self::assertSame(['/worker/register', '/worker/workflow-tasks/offers', '/worker/workflow-tasks/poll', '/worker/workflow-tasks/poll'], $uris);
        self::assertSame('first', $transport->requests[2]['body']['poll_request_id']);
        self::assertArrayNotHasKey('registration_token', $transport->requests[2]['body']);
        self::assertArrayNotHasKey(TaskDelivery::HEADER, $transport->requests[2]['headers']);

        // A new negotiation tries the offer route again.
        $client->registerWorker('worker', 'orders', [], []);
        self::assertSame(self::claimed('workflow'), self::poll($client, 'workflow', 'workflow', 'third'));
        self::assertStringEndsWith('/worker/workflow-tasks/offers', $transport->requests[5]['uri']);
    }

    /** @return iterable<string, array{int}> */
    public static function missingRoutes(): iterable
    {
        yield 'not found' => [404];
        yield 'method not allowed' => [405];
    }

    /** Native Server suspends new offers while its issuance window lapses. */
    public function testIssuanceUnavailableIsRetriedWithoutStoppingTheWorker(): void
    {
        $now = 0.0;
        $retries = [];
        $offers = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$offers): array {
            if (str_ends_with($uri, '/worker/register')) { return self::workerRegistration(); }
            if (++$offers <= 2) {
                throw new TransportException('Unavailable.', 503, ['reason' => TaskDelivery::ISSUANCE_UNAVAILABLE_REASON,
                    'message' => TaskDelivery::ISSUANCE_UNAVAILABLE_REASON]);
            }
            return self::exactBody('stale_worker_registration', 'worker_registration_lost_authority');
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            transientPollRetryObserver: static function (string $kind, int $attempt, float $delay, ServerException $error) use (&$retries): void {
                $retries[] = [$kind, $attempt, $error->reason, $delay > 0];
            }, stopOnStaleRegistration: true);
        $worker->run(4);

        self::assertSame([['workflow', 1, TaskDelivery::ISSUANCE_UNAVAILABLE_REASON, true],
            ['workflow', 2, TaskDelivery::ISSUANCE_UNAVAILABLE_REASON, true]], $retries);
        self::assertSame(3, $offers);
    }

    /** A lease carried by a completion is bound to the legacy poll identity on the Server. */
    public function testCompletionCarriedPollsRecoverThroughTheLegacyRoute(): void
    {
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(); }
            if (str_ends_with($uri, '/complete')) { throw new TransportException('Connection reset.', transientConnectionFailure: true); }
            if (str_ends_with($uri, '/offers')) { return ['task' => null, 'poll_status' => 'empty']; }
            return ['task' => ['task_id' => 'carried', 'lease_owner' => 'worker'], 'poll_status' => 'leased'];
        });
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        $workflowPoll = ['worker_id' => 'worker', 'task_queue' => 'orders', 'poll_request_id' => 'carried-workflow',
            'timeout_seconds' => 0, 'history_page_size' => 100, 'task_kinds' => ['workflow']];
        $activityPoll = ['worker_id' => 'worker', 'task_queue' => 'orders', 'poll_request_id' => 'carried-activity', 'timeout_seconds' => 0];
        try {
            $client->completeWorkflowTask('task', 'worker', 1, [['type' => 'complete_workflow']],
                activityPoll: $activityPoll, workflowPoll: $workflowPoll);
            self::fail('The completion reply was lost.');
        } catch (ServerException) {}
        try {
            $client->completeActivityTask('activity', 'attempt', 'worker', 'result', ['worker_id' => 'worker', 'task_queue' => 'orders',
                'poll_request_id' => 'carried-by-activity', 'timeout_seconds' => 0, 'task_kinds' => ['workflow']]);
            self::fail('The completion reply was lost.');
        } catch (ServerException) {}

        $carried = ['task' => ['task_id' => 'carried', 'lease_owner' => 'worker'], 'poll_status' => 'leased'];
        self::assertSame($carried, $client->pollWorkflowTaskResponse('worker', 'orders', 0, 'carried-workflow', 100));
        self::assertSame($carried, $client->pollActivityTaskResponse('worker', 'orders', 0, 'carried-activity'));
        self::assertSame($carried, $client->pollWorkflowTaskResponse('worker', 'orders', 0, 'carried-by-activity', 100));
        // An ordinary poll still uses offers.
        self::assertSame(['task' => null, 'poll_status' => 'empty'], $client->pollWorkflowTaskResponse('worker', 'orders', 0, 'fresh', 100));

        $polls = array_values(array_filter($transport->requests, static fn (array $request): bool => isset($request['body']['poll_request_id'])
            && !str_ends_with($request['uri'], '/complete')));
        $routes = array_map(static fn (array $request): array => [$request['body']['poll_request_id'],
            substr($request['uri'], strlen('https://server.example/api'))], $polls);
        self::assertSame([
            ['carried-workflow', '/worker/workflow-tasks/poll'],
            ['carried-activity', '/worker/activity-tasks/poll'],
            ['carried-by-activity', '/worker/workflow-tasks/poll'],
            ['fresh', '/worker/workflow-tasks/offers'],
        ], $routes);
        foreach (array_slice($polls, 0, 3) as $request) {
            self::assertArrayNotHasKey('registration_token', $request['body']);
            self::assertArrayNotHasKey(TaskDelivery::HEADER, $request['headers']);
        }
    }

    /** The managed worker drains a carried lease without an answer through its legacy identity. */
    public function testManagedWorkerDrainsACarriedLeaseThroughTheLegacyRoute(): void
    {
        $clock = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
        $caps = ['activity_completion_workflow_poll' => true];
        $workflow = ['task' => ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'task_kind' => 'workflow',
            'lease_owner' => 'worker', 'workflow_id' => 'workflow-instance', 'run_id' => 'run',
            'workflow_type' => 'carried.workflow', 'payload_codec' => 'avro', 'history_events' => [],
            'lease_expires_at' => $clock->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps,
            'server_time' => $clock->format('Y-m-d\TH:i:s.u\Z')];
        $carriedId = null;
        $commands = null;
        $activityOffers = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $workflow, $caps, &$carriedId, &$commands, &$activityOffers,
        ): ?array {
            if (str_ends_with($uri, '/worker/register')) { return self::workerRegistration(); }
            if (str_ends_with($uri, '/workflow-tasks/offers')) { return ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps]; }
            if (str_ends_with($uri, '/activity-tasks/offers')) {
                if (isset($body['task_offer_token'])) {
                    return ['task' => ['task_id' => 'selected-task', 'task_kind' => 'activity', 'activity_attempt_id' => 'attempt',
                        'lease_owner' => 'worker', 'activity_type' => 'carried.activity', 'payload_codec' => 'avro'],
                        'poll_status' => 'leased', 'server_capabilities' => $caps];
                }
                return ++$activityOffers === 1 ? self::offer('activity') : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/selected-task/complete')) {
                $carriedId = $body['workflow_poll']['poll_request_id'] ?? null;
                // The lease committed with the completion; its answer is not in this reply.
                return ['completed' => true, 'workflow_poll' => ['leased' => true]];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ($body['poll_request_id'] ?? null) === $carriedId ? $workflow : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow/complete')) { $commands = $body['commands']; return ['completed' => true]; }
            if (str_ends_with($uri, '/workflow-tasks/workflow/heartbeat')) {
                return ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/query-tasks/offers')) { return ['task' => null, 'poll_status' => 'empty']; }
            throw new \RuntimeException('Unexpected request '.$uri);
        });
        $client = new Client('https://server.example', transport: $transport);
        // tick() does not register; negotiate offer mode on the shared Client state directly.
        $client->registerWorker('worker', 'orders', [], []);
        $worker = new Worker($client, 'orders', workerId: 'worker');
        $worker->registerWorkflow('carried.workflow', static fn (\DurableWorkflow\Worker\WorkflowContext $context): string => 'done');
        $worker->registerActivity('carried.activity', static fn (\DurableWorkflow\Worker\ActivityContext $context): int => 1);
        $worker->tick(0);
        $worker->tick(0);

        self::assertIsString($carriedId);
        self::assertIsArray($commands, 'The carried workflow task ran after its legacy replay.');
        $carriedPolls = array_values(array_filter($transport->requests, static fn (array $request): bool =>
            ($request['body']['poll_request_id'] ?? null) === $carriedId && !str_contains($request['uri'], '/complete')));
        self::assertCount(1, $carriedPolls);
        self::assertStringEndsWith('/worker/workflow-tasks/poll', $carriedPolls[0]['uri']);
        self::assertArrayNotHasKey('registration_token', $carriedPolls[0]['body']);
    }

    private static function clientWithClock(FakeTransport $transport, int &$monotonic, int $wall): Client
    {
        $client = new Client('https://server.example', transport: $transport);
        (new ReflectionProperty(Client::class, 'taskDelivery'))->setValue($client, new TaskDelivery(
            static function () use (&$monotonic): int { return $monotonic; }, static fn (): int => $wall));

        return $client;
    }

    /** @return array<string, mixed> */
    private static function poll(Client $client, string $endpoint, string $kind, string $poll): array
    {
        return (new \ReflectionMethod(Client::class, 'pollTaskResponse'))->invoke($client, '/worker/'.$endpoint.'-tasks/poll', [
            'worker_id' => 'worker', 'task_queue' => 'orders', 'timeout_seconds' => 4, 'poll_request_id' => $poll, 'task_kinds' => [$kind],
        ]);
    }

    private static function wireRefusal(string $pollStatus, string $reason): TransportException
    {
        $body = self::exactBody($pollStatus, $reason);

        return new TransportException('Task acceptance is not confirmed.', 409, $body,
            taskDeliveryRefusal: TaskDeliveryRefusal::fromJson(409, json_encode($body, JSON_THROW_ON_ERROR)));
    }

    /** @return array{task: null, poll_status: string, reason: string} */
    private static function exactBody(string $pollStatus, string $reason): array
    {
        return ['task' => null, 'poll_status' => $pollStatus, 'reason' => $reason];
    }

    /** @param array<string, mixed> $body */
    private static function json(int $status, array $body): Response
    {
        return new Response($status, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
    }

    /** @return array<string, mixed> */
    private static function registration(?string $token = null): array
    {
        return ['registered' => true, 'registration_token' => $token ?? str_repeat('a', 32), 'server_capabilities' => ['task_delivery' => [
            'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
            'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER, 'version' => TaskDelivery::VERSION,
            'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2, 'unaccepted_offer_reserves_task' => false,
        ]]];
    }

    /** @return array<string, mixed> */
    private static function workerRegistration(): array
    {
        $registration = self::registration();
        $registration['heartbeat_interval_seconds'] = 10;
        $registration['server_capabilities']['worker_deregistration_fencing'] = ['schema' => 'durable-workflow.v2.worker-deregistration.v1',
            'supported' => true, 'endpoint' => '/worker/registrations/{workerId}/deregister', 'receipt_retention_seconds' => 600];

        return $registration;
    }

    /** @return array<string, mixed> */
    private static function offer(string $kind, int $expiresAtMs = 1000): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => $kind, 'task_id' => 'selected-task', 'token' => 'synthetic-packet', 'expires_at_ms' => $expiresAtMs,
        ]];
    }

    /** @return array<string, mixed> */
    private static function claimed(string $kind): array
    {
        $field = match ($kind) { 'query' => 'query_task_id', 'update_validation' => 'update_validation_task_id', default => 'task_id' };
        return ['task' => [$field => 'selected-task', 'task_kind' => $kind, 'lease_owner' => 'worker']];
    }
}
