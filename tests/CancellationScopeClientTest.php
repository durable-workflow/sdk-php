<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Exception\ServerException;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeClientTest extends TestCase
{
    #[DataProvider('scopeOperations')]
    public function test_scope_operations_keep_original_worker_authority(string $operation, array $body): void
    {
        $transport = new FakeTransport([['accepted' => true]]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'tenant',
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20');
        self::assertSame(['accepted' => true], $client->cancellationScopeOperation('task/one', 'original-owner', 4, $operation, $body));
        $request = $transport->requests[0];
        self::assertSame('https://server.example/api/worker/workflow-tasks/task%2Fone/cancellation-scopes/'.$operation, $request['uri']);
        self::assertSame(['lease_owner' => 'original-owner', 'workflow_task_attempt' => 4, ...$body], $request['body']);
        self::assertSame('Bearer worker', $request['headers']['Authorization']);
        self::assertSame('tenant', $request['headers']['X-Namespace']);
        self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
    }

    public static function scopeOperations(): array
    {
        return [
            'opening' => ['open', ['sequence' => 3, 'parent_scope_id' => 'root', 'shield_parent' => true]],
            'prefix' => ['checkpoint', ['checkpoint_id' => 'prefix-one', 'start_sequence' => 1, 'commands' => []]],
        ];
    }

    public function test_default_protocol_refuses_scope_operations_without_sending(): void
    {
        $transport = new FakeTransport([]);
        try {
            (new Client('https://server.example', transport: $transport))->cancellationScopeOperation('task', 'owner', 1, 'open');
            self::fail('Unqualified protocol must refuse scope opening.');
        } catch (LogicException) {
            self::assertSame([], $transport->requests);
        }
    }

    #[DataProvider('invalidAuthority')]
    public function test_scope_operations_cannot_replace_claim_authority(string $task, string $owner, int $attempt, string $operation, array $body): void
    {
        $transport = new FakeTransport([]);
        try {
            (new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'))
                ->cancellationScopeOperation($task, $owner, $attempt, $operation, $body);
            self::fail('Invalid scope authority must fail before transport.');
        } catch (InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public static function invalidAuthority(): array
    {
        return [
            [' ', 'owner', 1, 'open', []], ['task', ' ', 1, 'open', []], ['task', 'owner', 0, 'open', []],
            ['task', 'owner', 1, 'prepare', []], ['task', 'owner', 1, 'open', ['lease_owner' => 'replacement']],
            ['task', 'owner', 1, 'checkpoint', ['workflow_task_attempt' => 2]],
        ];
    }

    public function test_scope_admission_keeps_the_original_bounded_request_budget(): void
    {
        $transport = new class implements BoundedTransport {
            public ?int $observed = null;
            public function supportsBoundedRequests(): bool
            {
                return true;
            }
            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                throw new LogicException('Scope authority must use the bounded transport.');
            }
            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
            {
                $this->observed = $timeoutSeconds;
                return ['opened' => true];
            }
        };
        $client = (new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
        $budget = new RequestBudget(5, hrtime(true) / 1e9 + 1.9);
        $client->cancellationScopeOperation('task', 'original-owner', 4, 'open', ['sequence' => 1], $budget);
        self::assertSame(1, $transport->observed);
        $transport->observed = null;
        try {
            $client->cancellationScopeOperation('task', 'original-owner', 4, 'open', ['sequence' => 1], new RequestBudget(5, hrtime(true) / 1e9 - 1));
            self::fail('An expired authority budget must fail before transport.');
        } catch (ServerException) {
            self::assertNull($transport->observed);
        }
    }
}
