<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeOpeningReceiptTest extends TestCase
{
    public function test_original_claim_pages_prove_the_opening_before_returning_its_identity(): void
    {
        $history = self::history();
        $transport = self::transport([
            self::receipt(),
            self::page([$history[0]], 'opaque-next'),
            self::page(array_slice($history, 1)),
        ]);
        $scope = self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
        self::assertSame('scope-one', $scope->scopeId);
        self::assertSame('scope-event-one', $scope->historyEventId);
        self::assertSame(1, $scope->sequence);
        self::assertSame('root', $scope->parentScopeId);
        self::assertFalse($scope->shieldParent);
        self::assertSame($history, $scope->history);
        self::assertCount(3, $transport->fake->requests);
        foreach ($transport->fake->requests as $request) {
            self::assertSame('original-owner', $request['body']['lease_owner']);
            self::assertSame(4, $request['body']['workflow_task_attempt']);
            self::assertSame('Bearer worker', $request['headers']['Authorization']);
            self::assertSame('tenant', $request['headers']['X-Namespace']);
            self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
        }
        self::assertStringEndsWith('/task%2Fone/cancellation-scopes/open', $transport->fake->requests[0]['uri']);
        self::assertSame('opaque-start', $transport->fake->requests[1]['body']['next_history_page_token']);
        self::assertSame('opaque-next', $transport->fake->requests[2]['body']['next_history_page_token']);
    }

    public function test_lost_opening_response_reconciles_the_same_authored_boundary(): void
    {
        $receipt = array_replace(self::receipt(), ['duplicate' => true]);
        $transport = self::transport([new TransportException('Accepted response lost.', transientConnectionFailure: true), $receipt, self::page(self::history())]);
        $scope = self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
        self::assertSame('scope-one', $scope->scopeId);
        self::assertTrue($scope->duplicate);
        self::assertSame($transport->fake->requests[0], $transport->fake->requests[1]);
        self::assertCount(3, $transport->fake->requests);
    }

    public function test_original_deadline_is_shared_by_opening_reconciliation_and_every_history_page(): void
    {
        $transport = self::transport([new TransportException('Lost.', transientConnectionFailure: true), self::receipt(), self::page(self::history())]);
        $budget = new RequestBudget(5, hrtime(true) / 1e9 + 1.9);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1, budget: $budget);
        self::assertSame([1, 1, 1], $transport->timeouts);
    }

    public function test_budget_expiry_after_accepted_response_cannot_authorize_a_scope_body(): void
    {
        $transport = self::transport([], static function (): array {
            usleep(150000);
            return self::receipt();
        });
        $entered = false;
        try {
            self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1,
                budget: new RequestBudget(5, hrtime(true) / 1e9 + 1.1));
            $entered = true;
            self::fail('Expired claim budget must refuse a successful HTTP response.');
        } catch (WorkflowClaimAborted) {
            self::assertFalse($entered);
            self::assertCount(1, $transport->fake->requests);
        }
    }

    public function test_nested_shield_preserves_its_canonical_parent_and_original_identity(): void
    {
        $history = self::history();
        $payload = ['schema' => 'durable-workflow.cancellation-scope/v1', 'workflow_run_id' => 'run-one',
            'sequence' => 2, 'scope_id' => 'scope-two', 'parent_scope_id' => 'scope-one', 'shield_parent' => true];
        $history[] = ['id' => 'scope-event-two', 'sequence' => 4, 'event_type' => 'CancellationScopeOpened', 'namespace' => 'tenant', 'payload' => $payload];
        $receipt = array_replace(self::receipt(), ['scope_id' => 'scope-two', 'history_event_id' => 'scope-event-two',
            'sequence' => 2, 'parent_scope_id' => 'scope-one', 'shield_parent' => true, 'duplicate' => true]);
        $transport = self::transport([$receipt, self::page($history)]);
        $scope = self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 2, 'scope-one', true);
        self::assertSame('scope-two', $scope->scopeId);
        self::assertSame('scope-one', $scope->parentScopeId);
        self::assertTrue($scope->shieldParent);
        self::assertTrue($scope->duplicate);
    }

    #[DataProvider('invalidReceipts')]
    public function test_unproved_opening_cannot_return_authority(array $changes, ?string $remove = null): void
    {
        $receipt = array_replace(self::receipt(), $changes);
        if ($remove !== null) {
            unset($receipt[$remove]);
        }
        $transport = self::transport([$receipt, self::page(self::history())]);
        try {
            self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
            self::fail('Unproved scope must not reach a caller.');
        } catch (WorkflowClaimAborted) {
            self::assertCount(1, $transport->fake->requests, 'Invalid acknowledgements must not authorize history fetches.');
        }
    }

    public static function invalidReceipts(): array
    {
        return [
            'not opened' => [['opened' => false]], 'new task authority' => [['task_id' => 'other-task']],
            'different run' => [['workflow_run_id' => 'other-run']], 'new owner' => [['lease_owner' => 'replacement']],
            'new attempt' => [['workflow_task_attempt' => 5]], 'changed sequence' => [['sequence' => 2]],
            'changed parent' => [['parent_scope_id' => 'other-scope']], 'changed shield' => [['shield_parent' => true]],
            'released claim' => [['claim_released' => true]], 'new task' => [['created_task_ids' => ['extra-task']]],
            'refused reason' => [['reason' => 'refused']], 'root identity' => [['scope_id' => 'root']],
            'unknown duplication' => [['duplicate' => null]], 'missing event identity' => [[], 'history_event_id'],
            'missing canonical cursor' => [[], 'history_refresh_page_token'],
        ];
    }

    #[DataProvider('invalidHistories')]
    public function test_claim_history_must_prove_the_same_scope_and_tree(string $fault): void
    {
        $history = self::history();
        switch ($fault) {
            case 'missing scope': array_pop($history); break;
            case 'missing start': array_shift($history); array_shift($history); break;
            case 'foreign namespace': $history[2]['namespace'] = 'other'; break;
            case 'different run': $history[2]['payload']['workflow_run_id'] = 'other-run'; break;
            case 'different scope': $history[2]['payload']['scope_id'] = 'other-scope'; break;
            case 'different event': $history[2]['id'] = 'other-event'; break;
            case 'different sequence': $history[2]['payload']['sequence'] = 2; break;
            case 'different schema': $history[2]['payload']['schema'] = 'other'; break;
            case 'different shield': $history[2]['payload']['shield_parent'] = true; break;
            case 'missing parent': $history[2]['payload']['parent_scope_id'] = 'unrecorded'; break;
            case 'self parent': $history[2]['payload']['parent_scope_id'] = 'scope-one'; break;
            case 'duplicate event': $history[] = $history[2]; break;
            case 'duplicate scope':
                $history[] = array_replace($history[2], ['id' => 'later', 'sequence' => 4]); break;
            case 'out of order': $history = [$history[0], $history[2], $history[1]]; break;
        }
        $transport = self::transport([self::receipt(), self::page($history)]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    public static function invalidHistories(): array
    {
        return array_map(static fn (string $fault): array => [$fault], [
            'missing scope', 'missing start', 'foreign namespace', 'different run', 'different scope', 'different event',
            'different sequence', 'different schema', 'different shield', 'missing parent', 'self parent',
            'duplicate event', 'duplicate scope', 'out of order',
        ]);
    }

    #[DataProvider('invalidPages')]
    public function test_every_page_keeps_the_original_claim_and_opaque_cursor(array $changes): void
    {
        $transport = self::transport([self::receipt(), array_replace(self::page(self::history()), $changes)]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    public static function invalidPages(): array
    {
        return [
            [['task_id' => 'other-task']], [['workflow_task_attempt' => 5]], [['history_events' => ['bad' => []]]],
            [['next_history_page_token' => 1]], [['next_history_page_token' => 'opaque-start']],
        ];
    }

    public function test_a_positive_early_page_does_not_hide_a_later_malformed_page(): void
    {
        $transport = self::transport([self::receipt(), self::page(self::history(), 'opaque-next'),
            self::page([['id' => 'later', 'sequence' => 4, 'event_type' => 'CancellationScopeOpened', 'payload' => []]])]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    public function test_history_cannot_finish_without_an_explicit_terminal_cursor(): void
    {
        $page = self::page(self::history());
        unset($page['next_history_page_token']);
        $transport = self::transport([self::receipt(), $page]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    #[DataProvider('explicitRefusals')]
    public function test_an_explicit_refusal_is_not_reconciled_as_a_lost_acknowledgement(int $status, string $reason): void
    {
        $transport = self::transport([new TransportException('Refused.', $status, ['reason' => $reason]), self::receipt()]);
        try {
            self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
            self::fail('Refused claim cannot open a scope.');
        } catch (WorkflowClaimAborted $error) {
            self::assertSame($reason, $error->getPrevious()->reason);
            self::assertCount(1, $transport->fake->requests);
        }
    }

    public static function explicitRefusals(): array
    {
        return [[403, 'forbidden'], [409, 'workflow_task_attempt_mismatch'], [503, 'backend_unavailable']];
    }

    public function test_ownership_lost_after_opening_prevents_returning_the_scope(): void
    {
        $transport = self::transport([self::receipt(), new TransportException('Attempt reclaimed.', 409,
            ['reason' => 'workflow_task_attempt_mismatch'])]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    #[DataProvider('malformedLaterEvents')]
    public function test_every_later_event_must_keep_a_canonical_shape(array $event): void
    {
        $history = self::history();
        $history[] = ['id' => 'later', 'sequence' => 4, 'namespace' => 'tenant', ...$event];
        $transport = self::transport([self::receipt(), self::page($history)]);
        $this->expectException(WorkflowClaimAborted::class);
        self::client($transport)->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
    }

    public static function malformedLaterEvents(): array
    {
        return [[['payload' => []]], [['event_type' => 'SignalReceived', 'payload' => 'broken']]];
    }

    public function test_default_protocol_and_unbounded_transport_cannot_open_a_canonical_scope(): void
    {
        foreach ([new Client('https://server.example', transport: self::transport([])),
            new Client('https://server.example', transport: self::transport([]), workerProtocolVersion: '1.20')] as $client) {
            try {
                $client->openCancellationScopeOnClaim('task/one', 'run-one', 'original-owner', 4, 1);
                self::fail('Canonical admission must have candidate protocol and bounded authority.');
            } catch (LogicException) {
                self::assertTrue(true);
            }
        }
    }

    private static function client(BoundedTransport $transport): Client
    {
        return (new Client('https://server.example', namespace: 'tenant', transport: $transport,
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
    }

    private static function transport(array $responses, ?\Closure $handler = null): BoundedTransport
    {
        return new class($responses, $handler) implements BoundedTransport {
            public FakeTransport $fake;
            public array $timeouts = [];
            public function __construct(array $responses, ?\Closure $handler)
            {
                $this->fake = new FakeTransport($responses, $handler);
            }
            public function supportsBoundedRequests(): bool { return true; }
            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                throw new LogicException('Canonical admission must use bounded transport.');
            }
            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
            {
                $this->timeouts[] = $timeoutSeconds;
                return $this->fake->send($method, $uri, $headers, $body);
            }
        };
    }

    private static function receipt(): array
    {
        return ['opened' => true, 'duplicate' => false, 'claim_released' => false, 'task_id' => 'task/one',
            'workflow_run_id' => 'run-one', 'lease_owner' => 'original-owner', 'workflow_task_attempt' => 4,
            'history_event_id' => 'scope-event-one', 'scope_id' => 'scope-one', 'parent_scope_id' => 'root',
            'shield_parent' => false, 'sequence' => 1, 'created_task_ids' => [], 'reason' => null,
            'history_refresh_page_token' => 'opaque-start'];
    }

    private static function history(): array
    {
        return [
            ['id' => 'start-accepted', 'sequence' => 1, 'event_type' => 'StartAccepted', 'namespace' => 'tenant', 'payload' => []],
            ['id' => 'workflow-started', 'sequence' => 2, 'event_type' => 'WorkflowStarted', 'namespace' => 'tenant', 'payload' => []],
            ['id' => 'scope-event-one', 'sequence' => 3, 'event_type' => 'CancellationScopeOpened', 'namespace' => 'tenant',
                'payload' => ['schema' => 'durable-workflow.cancellation-scope/v1', 'workflow_run_id' => 'run-one',
                    'sequence' => 1, 'scope_id' => 'scope-one', 'parent_scope_id' => 'root', 'shield_parent' => false]],
        ];
    }

    private static function page(array $events, ?string $next = null): array
    {
        return ['task_id' => 'task/one', 'workflow_task_attempt' => 4, 'history_events' => $events, 'next_history_page_token' => $next];
    }
}
