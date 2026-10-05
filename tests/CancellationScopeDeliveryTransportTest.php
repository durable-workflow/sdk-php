<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeDeliveryTransportTest extends TestCase
{
    public function test_preparation_and_delivery_share_original_boundary_claim_and_budget(): void
    {
        $transport = self::transport();
        $client = self::client($transport)->withBoundedWorkerRequests();
        $budget = new RequestBudget(5, hrtime(true) / 1e9 + 1.9);
        foreach (['prepare', 'deliver'] as $phase) {
            self::assertSame(['unverified_receipt' => true],
                $client->cancellationScopeOperation('task/one', 'original-owner', 4, $phase, self::boundary(), $budget));
        }
        self::assertCount(2, $transport->requests);
        foreach ($transport->requests as $index => $request) {
            self::assertSame('https://server.example/api/worker/workflow-tasks/task%2Fone/cancellation-scopes/'.['prepare', 'deliver'][$index], $request['uri']);
            self::assertSame(['lease_owner' => 'original-owner', 'workflow_task_attempt' => 4, ...self::boundary()], $request['body']);
            self::assertSame('Bearer worker', $request['headers']['Authorization']);
            self::assertSame('tenant', $request['headers']['X-Namespace']);
            self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
            self::assertSame(1, $request['timeout']);
        }
    }

    #[DataProvider('phases')]
    public function test_expired_original_budget_sends_neither_preparation_nor_delivery(string $phase): void
    {
        $transport = self::transport();
        try {
            self::client($transport)->withBoundedWorkerRequests()->cancellationScopeOperation('task', 'owner', 1,
                $phase, self::boundary(), new RequestBudget(5, hrtime(true) / 1e9 - 1));
            self::fail('An expired original authority budget must refuse before I/O.');
        } catch (ServerException $error) {
            self::assertInstanceOf(TransportException::class, $error->getPrevious());
            self::assertSame([], $transport->requests);
        }
    }

    #[DataProvider('phases')]
    public function test_original_budget_and_bounded_worker_transport_are_required(string $phase): void
    {
        $transport = self::transport();
        foreach ([false, true] as $bounded) {
            $client = self::client($transport);
            if ($bounded) { $client = $client->withBoundedWorkerRequests(); }
            try {
                $client->cancellationScopeOperation('task', 'owner', 1, $phase, self::boundary(),
                    $bounded ? null : new RequestBudget(5));
                self::fail('Scope delivery cannot allocate fresh or unbounded authority.');
            } catch (LogicException) {
                self::assertSame([], $transport->requests);
            }
        }
    }

    #[DataProvider('phases')]
    public function test_default_protocol_refuses_the_new_controls_without_io(string $phase): void
    {
        $transport = self::transport();
        try {
            (new Client('https://server.example', transport: $transport))->withBoundedWorkerRequests()
                ->cancellationScopeOperation('task', 'owner', 1, $phase, self::boundary(), new RequestBudget(5));
            self::fail('Published defaults must not enable scope delivery.');
        } catch (LogicException) {
            self::assertSame([], $transport->requests);
        }
    }

    #[DataProvider('invalidBoundaries')]
    public function test_invalid_or_added_authority_is_refused_before_transport(array $changes): void
    {
        $transport = self::transport();
        foreach (['prepare', 'deliver'] as $phase) {
            try {
                self::client($transport)->withBoundedWorkerRequests()->cancellationScopeOperation('task', 'owner', 1,
                    $phase, array_replace(self::boundary(), $changes), new RequestBudget(5));
                self::fail('Invalid original scope authority must not reach the Server.');
            } catch (InvalidArgumentException) {
                self::assertSame([], $transport->requests);
            }
        }
    }

    #[DataProvider('phases')]
    public function test_uncertain_reply_is_not_retried_or_reported_as_delivery(string $phase): void
    {
        $transport = self::transport(true);
        try {
            self::client($transport)->withBoundedWorkerRequests()->cancellationScopeOperation('task', 'owner', 1,
                $phase, self::boundary(), new RequestBudget(5));
            self::fail('An uncertain receipt requires canonical reconciliation.');
        } catch (ServerException $error) {
            self::assertInstanceOf(TransportException::class, $error->getPrevious());
            self::assertCount(1, $transport->requests);
            self::assertSame('owner', $transport->requests[0]['body']['lease_owner']);
            self::assertSame(1, $transport->requests[0]['body']['workflow_task_attempt']);
        }
    }

    public static function phases(): array { return [['prepare'], ['deliver']]; }

    public static function invalidBoundaries(): array
    {
        return [
            'root is not an operation scope' => [['scope_id' => 'root']],
            'missing scope' => [['scope_id' => '']],
            'missing request' => [['request_id' => null]],
            'invalid request encoding' => [['request_id' => "\xff"]],
            'oversized scope' => [['scope_id' => str_repeat('s', 256)]],
            'zero boundary' => [['sequence' => 0]],
            'changed single call span' => [['sequence_span' => 2]],
            'unrelated operation range' => [['operation_sequence' => 2]],
            'supplied deadline' => [['cleanup_deadline_at' => '2026-10-05T02:30:00Z']],
            'supplied authority' => [['authority_deadline_at' => '2026-10-05T02:30:00Z']],
            'substituted preparation' => [['preparation_history_event_id' => 'borrowed']],
            'replacement owner' => [['lease_owner' => 'replacement']],
            'replacement claim' => [['workflow_task_attempt' => 2]],
        ];
    }

    private static function boundary(): array
    {
        return ['scope_id' => 'original-scope', 'request_id' => 'original-request', 'sequence' => 3,
            'call_kind' => 'activity', 'sequence_span' => 1, 'operation_sequence' => null, 'operation_sequence_span' => 1];
    }

    private static function client(BoundedTransport $transport): Client
    {
        return new Client('https://server.example', transport: $transport, namespace: 'tenant',
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20');
    }

    private static function transport(bool $loseReply = false): CancellationScopeDeliveryTransportProbe
    {
        return new CancellationScopeDeliveryTransportProbe($loseReply);
    }
}

final class CancellationScopeDeliveryTransportProbe implements BoundedTransport
{
    /** @var list<array{uri: string, headers: array<string, string>, body: array<string, mixed>|null, timeout: int}> */
    public array $requests = [];
    public function __construct(private readonly bool $loseReply) {}
    public function supportsBoundedRequests(): bool { return true; }
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        throw new LogicException('Scope delivery requires bounded I/O.');
    }
    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $this->requests[] = ['uri' => $uri, 'headers' => $headers, 'body' => $body, 'timeout' => $timeoutSeconds];
        if ($this->loseReply) { throw new TransportException('Accepted reply intentionally lost.', transientConnectionFailure: true); }
        return ['unverified_receipt' => true];
    }
}
