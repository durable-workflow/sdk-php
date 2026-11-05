<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\TaskDelivery;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Selected acceptance outcomes require a closed, internally consistent reply. */
final class WorkerTaskDeliveryRefusalTest extends TestCase
{
    /** @param array<string, mixed> $response */
    #[DataProvider('malformedRefusals')]
    public function testMalformed409RetainsThePacketAndNegotiatedRegistration(array $response): void
    {
        self::assertUncertainReply(self::wireFailure($response));
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function malformedRefusals(): iterable
    {
        $pairs = self::existingPairs();
        $statuses = array_values(array_unique([...array_column($pairs, 0), 'draining', 'stopped',
            'worker_heartbeat_stale', 'worker_registration_lost_authority', 'empty', 'offered', 'foreign_status']));
        $reasons = array_values(array_unique([...array_column($pairs, 1), 'worker_draining', 'worker_stopped',
            'stale_worker_registration', 'worker_heartbeat_stale', 'backend_unavailable', 'unrelated_reason']));
        foreach ($statuses as $status) {
            foreach ($reasons as $reason) {
                if (!in_array([$status, $reason], $pairs, true)) {
                    yield $status.' / '.$reason => [self::body($status, $reason)];
                }
            }
        }
        foreach ($pairs as [$status, $reason]) {
            foreach (self::contradictoryFields() as $name => [$key, $value]) {
                yield $status.' / '.$name => [[...self::body($status, $reason), $key => $value]];
            }
            foreach (['task', 'poll_status', 'reason'] as $key) {
                $body = self::body($status, $reason);
                unset($body[$key]);
                yield $status.' / missing '.$key => [$body];
            }
        }
    }

    /** @return array<string, array{string, mixed}> */
    private static function contradictoryFields(): array
    {
        return [
            'non-null task' => ['task', ['task_id' => 'unexpected']],
            'false task' => ['task', false],
            'offer null is still present' => ['task_offer', null],
            'offer object' => ['task_offer', ['task_id' => 'other-task']],
            'acceptance token' => ['task_offer_token', 'contradictory-packet'],
            'accepted marker' => ['accepted', true],
            'lease owner' => ['lease_owner', 'worker'],
            'attempt marker' => ['attempt', 1],
            'task ID outside task' => ['task_id', 'selected-task'],
            'kind outside task' => ['task_kind', 'workflow'],
            'claim retry marker' => ['retry_same_poll_request_id', true],
            'retryable wrong type' => ['retryable', 'true'],
            'retryable true' => ['retryable', true],
            'retryable false' => ['retryable', false],
            'retry delay' => ['retry_after_seconds', 1],
            'matching worker' => ['worker_id', 'worker'],
            'matching queue' => ['task_queue', 'orders'],
            'plain error text' => ['error', 'Synthetic refusal.'],
            'plain message text' => ['message', 'Synthetic refusal.'],
            'null build' => ['registered_build_id', null],
            'unknown extension' => ['unknown_terminal_extension', true],
            'foreign worker' => ['worker_id', 'other-worker'],
            'foreign queue' => ['task_queue', 'other-queue'],
            'worker wrong type' => ['worker_id', 1],
            'queue wrong type' => ['task_queue', null],
            'error wrong type' => ['error', []],
            'message wrong type' => ['message', []],
            'build wrong type' => ['registered_build_id', []],
            'status wrong type' => ['worker_status', []],
            'drain intent wrong type' => ['drain_intent', []],
            'null poll status' => ['poll_status', null],
            'null reason' => ['reason', null],
        ];
    }

    /** @param array<string, mixed> $response */
    #[DataProvider('unqualifiedSuccessReplies')]
    public function testAnUnqualified200CannotResolveASelectedAcceptance(array $response): void
    {
        self::assertUncertainReply($response);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function unqualifiedSuccessReplies(): iterable
    {
        yield 'mixed successful terminal' => [self::body('draining', 'unrelated_reason')];
        yield 'successful expiry is not the required 409' => [self::body('task_offer_expired', 'task_offer_expired')];
        yield 'successful authority loss is not the required 409' => [self::body('stale_worker_registration', 'worker_registration_lost_authority')];
        yield 'successful stopped hint' => [self::body('stopped', 'worker_stopped')];
        yield 'successful draining hint' => [self::body('draining', 'worker_draining')];
        yield 'bare legacy stale is not definitive acceptance' => [['task' => null, 'poll_status' => 'stale_worker_registration']];
    }

    #[DataProvider('wrongHttpStatuses')]
    public function testAnotherHttpStatusCannotResolveTheSelectedPacket(int $status, string $pollStatus, string $reason): void
    {
        $body = self::body($pollStatus, $reason);
        self::assertUncertainReply($status === 200 ? $body : new TransportException('Synthetic refusal.', $status, $body));
    }

    /** @return iterable<string, array{int, string, string}> */
    public static function wrongHttpStatuses(): iterable
    {
        foreach (self::existingPairs() as [$status, $reason]) {
            foreach ([0, 200, 202, 400, 401, 403, 404, 405, 410, 429, 500, 503] as $http) {
                yield $status.' / '.$http => [$http, $status, $reason];
            }
        }
    }

    public function testExceptionAndBodyReasonsMustAgree(): void
    {
        self::assertUncertainReply(new ServerException('Synthetic refusal.', 409, 'unrelated_reason',
            self::body('task_offer_expired', 'task_offer_expired'), self::wireFailure(self::body('task_offer_expired', 'task_offer_expired'))));
    }

    #[DataProvider('positiveRefusals')]
    public function testTheExactDefinitiveRefusalPermitsOnlyItsDefinedTransition(string $status, string $reason): void
    {
        $body = self::body($status, $reason);
        $transport = new FakeTransport([self::registration(), self::offer(), self::wireFailure($body),
            self::registration(), self::offer('new-packet'), self::claimed()]);
        $client = self::client($transport);
        $expected = $body;
        if ($reason === 'task_offer_expired') { $expected['poll_status'] = 'empty'; }
        self::assertSame($expected, self::poll($client));
        $client->registerWorker('worker', 'orders', [], []);
        self::assertSame(self::claimed(), self::poll($client, 'new-poll'));
        self::assertCount(6, $transport->requests);
        self::assertArrayNotHasKey('task_offer_token', $transport->requests[4]['body']);
        self::assertSame('new-poll', $transport->requests[4]['body']['poll_request_id']);
        self::assertSame('new-packet', $transport->requests[5]['body']['task_offer_token']);
    }

    /** @return list<array{string, string}> */
    public static function positiveRefusals(): array { return self::existingPairs(); }

    public function testExpiryPreservesRegistrationAndAnotherUnresolvedPacket(): void
    {
        [$client, $transport] = self::pendingClient('poll-a', self::wireFailure(self::body('task_offer_expired', 'task_offer_expired')));
        self::expectPending($client, 'poll-a');
        self::expectPending($client, 'poll-b');
        self::assertSame('empty', self::poll($client, 'poll-a')['poll_status']);
        self::assertSame(self::claimed('poll-b'), self::poll(clone $client, 'poll-b'));
        self::assertSame(self::claimed('poll-c'), self::poll($client, 'poll-c'));
        self::assertCount(1, self::packetsFor($transport, 'poll-a', false));
        self::assertCount(1, self::packetsFor($transport, 'poll-b', false));
        self::assertCount(1, self::packetsFor($transport, 'poll-c', false));
        self::assertIdenticalRetries($transport, 'poll-b', 2);
    }

    public function testAnUncertainLifecycleReplyCannotEraseAnotherPendingPacket(): void
    {
        [$client, $transport] = self::pendingClient('poll-a', new TransportException('Draining.', 409,
            self::body('draining', 'worker_draining')));
        self::expectPending($client, 'poll-a');
        self::expectPending($client, 'poll-b');
        self::expectPending($client, 'poll-a');
        self::assertSame(self::claimed('poll-b'), self::poll(clone $client, 'poll-b'));
        self::assertSame(self::claimed('poll-a'), self::poll($client, 'poll-a'));
        self::assertCount(1, self::packetsFor($transport, 'poll-a', false));
        self::assertCount(1, self::packetsFor($transport, 'poll-b', false));
        self::assertIdenticalRetries($transport, 'poll-a', 3);
        self::assertIdenticalRetries($transport, 'poll-b', 2);
    }

    public function testDiscoveryDrainingDoesNotEraseAnAlreadySelectedPacket(): void
    {
        $stop = self::body('draining', 'worker_draining');
        [$client, $transport] = self::pendingClient('discovery', new TransportException('Draining.', 409, $stop), true);
        self::expectPending($client, 'poll-a');
        self::assertSame($stop, self::poll($client, 'discovery'));
        self::assertSame(self::claimed('poll-a'), self::poll(clone $client, 'poll-a'));
        self::assertCount(1, self::packetsFor($transport, 'poll-a', false));
        self::assertIdenticalRetries($transport, 'poll-a', 2);
    }

    public function testProvedAuthorityLossDoesNotForgetAnotherWorker(): void
    {
        $lost = self::body('stale_worker_registration', 'worker_registration_lost_authority');
        [$client, $transport] = self::pendingClient('poll-a', self::wireFailure($lost));
        $client->registerWorker('worker-b', 'orders', [], []);
        self::expectPending($client, 'poll-a');
        self::expectPending($client, 'poll-b', 'worker-b');
        self::assertSame($lost, self::poll($client, 'poll-a'));
        self::assertSame(self::claimed('poll-b', 'worker-b'), self::poll(clone $client, 'poll-b', 'worker-b'));
        self::assertCount(1, self::packetsFor($transport, 'poll-b', false));
        self::assertIdenticalRetries($transport, 'poll-b', 2);
    }

    public function testTheLegacyBareStalePollContractIsPreserved(): void
    {
        $response = ['task' => null, 'poll_status' => 'stale_worker_registration'];
        $transport = new FakeTransport([['registered' => true], $response]);
        $client = self::client($transport);
        self::assertSame($response, self::poll($client));
        self::assertTrue(PollResponse::isTerminal($response));
        self::assertStringEndsWith('/workflow-tasks/poll', $transport->requests[1]['uri']);
    }

    /** @param TransportException|ServerException|array<string, mixed> $reply */
    private static function assertUncertainReply(TransportException|ServerException|array $reply): void
    {
        $steps = [self::registration(), self::offer(), $reply, self::claimed()];
        $transport = new FakeTransport(handler: static function () use (&$steps): ?array {
            $next = array_shift($steps);
            if ($next instanceof TransportException || $next instanceof ServerException) { throw $next; }
            return $next;
        });
        $client = self::client($transport);
        $caught = null;
        try { self::poll($client); }
        catch (ServerException $error) { $caught = $error; }
        self::assertInstanceOf(TaskDeliveryUncertain::class, $caught);
        self::assertTrue(PollResponse::isTransientFailure($caught), 'An uncertain reply cannot terminate the managed worker.');
        self::assertSame(self::claimed(), self::poll(clone $client));
        self::assertCount(4, $transport->requests);
        self::assertSame($transport->requests[2], $transport->requests[3]);
        self::assertStringEndsWith('/workflow-tasks/offers', $transport->requests[3]['uri']);
    }

    /** @param array<string, mixed> $body */
    private static function wireFailure(array $body): TransportException
    {
        return new TransportException('Synthetic refusal.', 409, $body,
            taskDeliveryRefusal: \DurableWorkflow\Transport\TaskDeliveryRefusal::fromJson(409, json_encode($body, JSON_THROW_ON_ERROR)));
    }

    public function testADecodedOnlyRefusalCannotResolveThePacket(): void
    {
        self::assertUncertainReply(new TransportException('No original-byte evidence.', 409,
            self::body('task_offer_expired', 'task_offer_expired')));
    }

    public function testAnOldAuthorityRefusalCannotEraseAReplacementIncarnation(): void
    {
        $registrations = 0;
        $attempts = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$registrations, &$attempts): array {
            if (str_ends_with($uri, '/worker/register')) {
                $response = self::registration();
                $response['registration_token'] = str_repeat(++$registrations === 1 ? 'a' : 'b', 32);
                return $response;
            }
            $poll = $body['poll_request_id'];
            if (!isset($body['task_offer_token'])) { return self::offer('packet-'.$poll, $poll); }
            if ($poll === 'old') {
                \Fiber::suspend();
                throw self::wireFailure(self::body('stale_worker_registration', 'worker_registration_lost_authority'));
            }
            if (!isset($attempts[$poll])) {
                $attempts[$poll] = true;
                throw new TransportException('Lost response.', 504);
            }
            return self::claimed($poll);
        });
        $client = self::client($transport);
        $old = new \Fiber(static fn (): array => self::poll($client, 'old'));
        $old->start();
        self::assertTrue($old->isSuspended());
        $replacement = clone $client;
        $replacement->registerWorker('worker', 'orders', [], []);
        self::expectPending($replacement, 'new');
        $old->resume();
        self::assertSame('worker_registration_lost_authority', $old->getReturn()['reason']);
        self::assertSame(self::claimed('new'), self::poll($replacement, 'new'));
        self::assertCount(1, self::packetsFor($transport, 'new', false));
        self::assertIdenticalRetries($transport, 'new', 2);
        self::assertSame(str_repeat('b', 32), self::packetsFor($transport, 'new', true)[1]['body']['registration_token']);
    }

    /** @return list<array{string, string}> */
    private static function existingPairs(): array
    {
        // Exact two rows in the reviewed Server 31b281ac companion.
        return [
            ['task_offer_expired', 'task_offer_expired'],
            ['stale_worker_registration', 'worker_registration_lost_authority'],
        ];
    }

    /** @return array<string, mixed> */
    private static function body(string $status, string $reason): array
    {
        return ['task' => null, 'poll_status' => $status, 'reason' => $reason];
    }

    private static function client(FakeTransport $transport): Client
    {
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        return $client;
    }

    /** @return array<string, mixed> */
    private static function poll(Client $client, string $pollId = 'refusal-poll', string $workerId = 'worker'): array
    {
        return $client->pollWorkflowTaskResponse($workerId, 'orders', 4, $pollId);
    }

    private static function expectPending(Client $client, string $pollId, string $workerId = 'worker'): void
    {
        $caught = null;
        try { self::poll($client, $pollId, $workerId); }
        catch (ServerException $error) { $caught = $error; }
        self::assertInstanceOf(TaskDeliveryUncertain::class, $caught);
    }

    /** @return array{Client, FakeTransport} */
    private static function pendingClient(string $refusalPoll, TransportException $refusal, bool $discovery = false): array
    {
        $attempts = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $refusalPoll, $refusal, $discovery, &$attempts,
        ): array {
            if (str_ends_with($uri, '/worker/register')) {
                $response = self::registration();
                $response['registration_token'] = str_repeat($body['worker_id'] === 'worker' ? 'a' : 'b', 32);
                return $response;
            }
            $poll = $body['poll_request_id'];
            if (!isset($body['task_offer_token'])) {
                if ($discovery && $poll === $refusalPoll) { throw $refusal; }
                return self::offer('packet-'.$poll, $poll);
            }
            $attempts[$poll] = ($attempts[$poll] ?? 0) + 1;
            if (in_array($poll, ['poll-a', 'poll-b'], true) && $attempts[$poll] === 1) {
                throw new TransportException('Lost response.', 504);
            }
            if (!$discovery && $poll === $refusalPoll && $attempts[$poll] === 2) { throw $refusal; }
            return self::claimed($poll, $body['worker_id']);
        });

        return [self::client($transport), $transport];
    }

    /** @return list<array<string, mixed>> */
    private static function packetsFor(FakeTransport $transport, string $pollId, bool $accept): array
    {
        return array_values(array_filter($transport->requests, static fn (array $request): bool =>
            ($request['body']['poll_request_id'] ?? null) === $pollId
            && isset($request['body']['task_offer_token']) === $accept));
    }

    private static function assertIdenticalRetries(FakeTransport $transport, string $pollId, int $count): void
    {
        $requests = self::packetsFor($transport, $pollId, true);
        self::assertCount($count, $requests);
        $first = json_encode($requests[0], JSON_THROW_ON_ERROR);
        foreach ($requests as $request) {
            self::assertSame($first, json_encode($request, JSON_THROW_ON_ERROR));
        }
    }

    /** @return array<string, mixed> */
    private static function registration(): array
    {
        return ['registered' => true, 'registration_token' => str_repeat('a', 32), 'server_capabilities' => ['task_delivery' => [
            'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
            'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER,
            'version' => TaskDelivery::VERSION, 'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2,
            'unaccepted_offer_reserves_task' => false,
        ]]];
    }

    /** @return array<string, mixed> */
    private static function offer(string $token = 'selected-packet', string $taskId = 'selected-task'): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => 'workflow', 'task_id' => $taskId,
            'token' => $token, 'expires_at_ms' => 1000,
        ]];
    }

    /** @return array<string, mixed> */
    private static function claimed(string $taskId = 'selected-task', string $workerId = 'worker'): array
    {
        return ['task' => ['task_id' => $taskId, 'lease_owner' => $workerId, 'task_kind' => 'workflow']];
    }
}
