<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker\CancellationDelivery;
use DurableWorkflow\Worker\CancellationScopeDeliveryReceipt;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeBoundaryReceiptTest extends TestCase
{
    public function test_native_preparation_and_delivery_are_proved_from_every_original_claim_page(): void
    {
        $history = self::history();
        $transport = self::transport([
            self::receipt(), self::page(array_slice($history, 0, 2), 'opaque-next'), self::page(array_slice($history, 2, 4)),
            self::receipt(true), self::page(array_slice($history, 0, 6), 'opaque-next'), self::page([$history[6]]),
        ]);
        $budget = new RequestBudget(5, hrtime(true) / 1e9 + 1.9);
        $prepared = self::boundary($transport, budget: $budget);
        self::assertNull($prepared->deliveryHistoryEventId);
        self::assertSame($history[5]['id'], $prepared->preparationHistoryEventId);
        self::assertSame(array_slice($history, 0, 6), $prepared->history);
        $delivered = self::boundary($transport, true, $prepared, $budget);
        self::assertSame($history[6]['id'], $delivered->deliveryHistoryEventId);
        self::assertSame($prepared->preparationHistoryEventId, $delivered->preparationHistoryEventId);
        self::assertSame($prepared->context->toArray(), $delivered->context->toArray());
        self::assertEquals($prepared->authorityDeadline, $delivered->authorityDeadline);
        self::assertSame($history, $delivered->history);
        self::assertCount(6, $transport->fake->requests);
        self::assertSame([1, 1, 1, 1, 1, 1], $transport->timeouts);
        foreach ($transport->fake->requests as $request) {
            self::assertSame('original-owner', $request['body']['lease_owner']);
            self::assertSame(4, $request['body']['workflow_task_attempt']);
            self::assertSame('Bearer worker', $request['headers']['Authorization']);
            self::assertSame('sdk-scope-fixture', $request['headers']['X-Namespace']);
            self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
        }
        foreach ([0, 3] as $index) {
            self::assertSame(['lease_owner'=>'original-owner', 'workflow_task_attempt'=>4, ...self::body()],
                $transport->fake->requests[$index]['body']);
        }
        self::assertStringEndsWith('/task%2Fone/cancellation-scopes/prepare', $transport->fake->requests[0]['uri']);
        self::assertStringEndsWith('/task%2Fone/cancellation-scopes/deliver', $transport->fake->requests[3]['uri']);
    }

    #[DataProvider('phases')]
    public function test_lost_reply_reconciles_the_same_claim_boundary_and_original_preparation(bool $delivering): void
    {
        $transport = self::transport([new TransportException('Accepted reply lost.', transientConnectionFailure: true),
            self::receipt($delivering), self::page(self::history($delivering))]);
        $receipt = self::boundary($transport, $delivering, $delivering ? self::preparation() : null);
        self::assertSame(self::history()[5]['id'], $receipt->preparationHistoryEventId);
        self::assertSame($transport->fake->requests[0], $transport->fake->requests[1]);
        self::assertSame([1, 1, 1], $transport->timeouts);
        self::assertCount(3, $transport->fake->requests);
    }

    public static function phases(): array { return [[false], [true]]; }

    public function test_a_replacement_claim_retains_the_original_preparation_context_and_ceiling(): void
    {
        $original = self::preparation();
        $receipt = array_replace(self::receipt(true), ['task_id'=>'replacement-task', 'lease_owner'=>'replacement-owner', 'workflow_task_attempt'=>17]);
        $page = array_replace(self::page(self::history()), ['task_id'=>'replacement-task', 'workflow_task_attempt'=>17]);
        $transport = self::transport([$receipt, $page]);
        $proved = self::client($transport)->cancellationScopeBoundaryOnClaim('replacement-task', self::runId(), self::workflowId(),
            'replacement-owner', 17, self::scopeId(), self::delivery(), 'deliver', new RequestBudget(5), $original);
        self::assertSame($original->preparationHistoryEventId, $proved->preparationHistoryEventId);
        self::assertSame($original->context->toArray(), $proved->context->toArray());
        self::assertEquals($original->authorityDeadline, $proved->authorityDeadline);
        self::assertSame('original', $proved->history[5]['payload']['task']['lease_owner']);
        self::assertSame(1, $proved->history[5]['payload']['task']['attempt_count']);
    }

    #[DataProvider('invalidReceipts')]
    public function test_invalid_preparation_acknowledgement_cannot_authorize_history_or_cleanup(array $changes, ?string $missing = null): void
    {
        $receipt = array_replace(self::receipt(), $changes);
        if ($missing !== null) { unset($receipt[$missing]); }
        $transport = self::transport([$receipt]);
        try {
            self::boundary($transport);
            self::fail('Unproved acknowledgement must abort.');
        } catch (WorkflowClaimAborted) {
            self::assertCount(1, $transport->fake->requests);
        }
    }

    public static function invalidReceipts(): array
    {
        return [
            'not prepared'=>[['prepared'=>false]], 'premature delivery'=>[['delivered'=>true]],
            'changed task'=>[['task_id'=>'other']], 'changed run'=>[['workflow_run_id'=>'other']],
            'changed owner'=>[['lease_owner'=>'other']], 'changed attempt'=>[['workflow_task_attempt'=>5]],
            'changed scope'=>[['scope_id'=>'other']], 'changed request'=>[['request_id'=>'other']],
            'changed sequence'=>[['sequence'=>4]], 'changed kind'=>[['call_kind'=>'activity']],
            'changed span'=>[['sequence_span'=>2]], 'borrowed operation'=>[['operation_sequence'=>1]],
            'changed operation span'=>[['operation_sequence_span'=>2]], 'released claim'=>[['claim_released'=>true]],
            'missing operation address'=>[[], 'operation_sequence'],
            'created task'=>[['created_task_ids'=>['other']]], 'refusal'=>[['reason'=>'refused']],
            'changed preparation event'=>[['preparation_history_event_id'=>'other']], 'missing event'=>[[], 'history_event_id'],
            'missing cursor'=>[[], 'history_refresh_page_token'], 'missing context'=>[[], 'cancellation'],
            'wrong context type'=>[['cancellation'=>'invalid']], 'missing authority'=>[[], 'authority_deadline_at'],
            'invalid date'=>[['authority_deadline_at'=>'2026-02-30T00:00:30Z']],
            'renewed authority'=>[['authority_deadline_at'=>'2026-10-04T00:00:31Z']],
            'before request'=>[['authority_deadline_at'=>'2026-10-03T00:00:00Z']],
            'populated activity'=>[['activity_members'=>[['activity_execution_id'=>'borrowed']]]],
            'missing timers'=>[[], 'timer_members'], 'missing waits'=>[[], 'wait_members'], 'missing children'=>[[], 'child_members'],
        ];
    }

    #[DataProvider('invalidHistories')]
    public function test_canonical_history_must_prove_the_original_request_tree_preparation_and_delivery(string $fault): void
    {
        $history = self::history();
        switch ($fault) {
            case 'missing start': array_shift($history); array_shift($history); break;
            case 'changed start run': $history[0]['payload']['workflow_run_id']='other'; break;
            case 'changed start workflow': $history[1]['payload']['workflow_instance_id']='other'; break;
            case 'duplicate start': $history[]=array_replace($history[1], ['id'=>'late-start', 'sequence'=>8]); break;
            case 'foreign namespace': $history[6]['namespace']='other'; break;
            case 'repeated event': $history[]=$history[6]; break;
            case 'out of order': $history=[$history[0], $history[2], $history[1], ...array_slice($history, 3)]; break;
            case 'invalid event': $history[]='invalid'; break;
            case 'invalid late payload': $history[]=['id'=>'later', 'sequence'=>8, 'namespace'=>'sdk-scope-fixture', 'event_type'=>'Unknown', 'payload'=>null]; break;
            case 'missing request': unset($history[4]); break;
            case 'changed request context': $history[4]['payload']['cancellation']['root_context']['reason']='changed'; break;
            case 'missing preparation': unset($history[5]); break;
            case 'missing delivery': array_pop($history); break;
            case 'changed preparation identity': $history[5]['id']='other'; break;
            case 'changed preparation schema': $history[5]['payload']['schema']='durable-workflow.cancellation-scope-preparation/v4'; break;
            case 'missing frozen descendants': unset($history[5]['payload']['descendant_members']); break;
            case 'populated projection': $history[5]['payload']['activity_members']=[['activity_execution_id'=>'borrowed']]; break;
            case 'populated descendant': $history[5]['payload']['descendant_members']=[['scope_id'=>'borrowed']]; break;
            case 'changed authority': $history[5]['payload']['authority_deadline_at']='2026-10-04T00:00:27Z'; break;
            case 'changed delivery boundary': $history[6]['payload']['call_kind']='activity'; break;
            case 'changed delivery preparation': $history[6]['payload']['preparation_history_event_id']='other'; break;
            case 'changed delivered event': $history[6]['id']='other'; break;
            case 'late delivery': $history[6]['timestamp']='2026-10-04T00:00:31Z'; break;
            case 'missing parent scope': unset($history[2]); break;
            case 'admitted member omitted':
                $history[]= ['id'=>'admission', 'sequence'=>8, 'namespace'=>'sdk-scope-fixture', 'event_type'=>'TimerScheduled',
                    'payload'=>['sequence'=>4, 'cancellation_scope_id'=>self::scopeId()]];
                // Place the admission before preparation so its member must be frozen.
                $admission=array_pop($history); $admission['sequence']=6;
                $history[5]['sequence']=7; $history[6]['sequence']=8;
                array_splice($history, 5, 0, [$admission]); break;
        }
        $transport = self::transport([self::receipt(true), self::page(array_values($history))]);
        $this->expectException(WorkflowClaimAborted::class);
        self::boundary($transport, true, self::preparation());
    }

    public static function invalidHistories(): array
    {
        return array_map(static fn (string $fault): array => [$fault], [
            'missing start', 'changed start run', 'changed start workflow', 'duplicate start', 'foreign namespace', 'repeated event',
            'out of order', 'invalid event', 'invalid late payload', 'missing request', 'changed request context', 'missing preparation',
            'missing delivery', 'changed preparation identity', 'changed preparation schema', 'missing frozen descendants',
            'populated projection', 'populated descendant', 'changed authority', 'changed delivery boundary',
            'changed delivery preparation', 'changed delivered event', 'late delivery', 'missing parent scope', 'admitted member omitted',
        ]);
    }

    #[DataProvider('substitutedPreparations')]
    public function test_delivery_cannot_replace_a_previously_verified_preparation_even_with_matching_new_history(string $fault): void
    {
        $history = self::history(); $receipt = self::receipt(true);
        if ($fault === 'identity') {
            $history[5]['id']='substituted';
            $history[6]['payload']['preparation_history_event_id']='substituted';
            $receipt['preparation_history_event_id']='substituted';
        } else {
            foreach ([5, 6] as $index) { $history[$index]['payload']['authority_deadline_at']='2026-10-04T00:00:27Z'; }
            $receipt['authority_deadline_at']='2026-10-04T00:00:27Z';
        }
        $transport = self::transport([$receipt, self::page($history)]);
        $this->expectException(WorkflowClaimAborted::class);
        self::boundary($transport, true, self::preparation());
    }

    public static function substitutedPreparations(): array { return [['identity'], ['authority']]; }

    #[DataProvider('invalidPages')]
    public function test_every_original_claim_page_must_be_complete_and_use_a_new_opaque_cursor(array $changes, ?string $missing = null): void
    {
        $page = array_replace(self::page(self::history(false)), $changes);
        if ($missing !== null) { unset($page[$missing]); }
        $transport = self::transport([self::receipt(), $page]);
        $this->expectException(WorkflowClaimAborted::class);
        self::boundary($transport);
    }

    public static function invalidPages(): array
    {
        return [[['task_id'=>'other']], [['workflow_task_attempt'=>5]], [['history_events'=>['bad'=>[]]]],
            [['next_history_page_token'=>'opaque-start']], [['next_history_page_token'=>'']], [['next_history_page_token'=>1]],
            [[], 'next_history_page_token']];
    }

    public function test_an_early_preparation_page_does_not_hide_a_malformed_later_page(): void
    {
        $transport = self::transport([self::receipt(), self::page(self::history(false), 'later'), self::page(['invalid'])]);
        try { self::boundary($transport); self::fail('Incomplete proof must abort.'); } catch (WorkflowClaimAborted) {
            self::assertCount(3, $transport->fake->requests);
        }
    }

    public function test_pending_activity_stop_cannot_claim_delivery_or_retry(): void
    {
        $transport=self::transport([array_replace(self::receipt(true), ['delivered'=>false, 'reason'=>'cancellation_scope_activity_stop_not_acknowledged'])]);
        try { self::boundary($transport, true, self::preparation()); self::fail('Pending stop is not delivery.'); } catch (WorkflowClaimAborted) {
            self::assertCount(1, $transport->fake->requests);
        }
    }

    public function test_persistent_uncertainty_has_only_one_reconciliation_without_inventing_a_receipt(): void
    {
        $error=new TransportException('Lost.', transientConnectionFailure:true);
        $transport=self::transport([$error, $error]);
        try { self::boundary($transport); self::fail('Uncertain mutation must abort.'); } catch (WorkflowClaimAborted) {
            self::assertCount(2, $transport->fake->requests);
            self::assertSame($transport->fake->requests[0], $transport->fake->requests[1]);
        }
    }

    public function test_expired_original_budget_cannot_fetch_or_return_a_positive_receipt(): void
    {
        $transport = self::transport([], static function (): array { usleep(150000); return self::receipt(); });
        try { self::boundary($transport, budget:new RequestBudget(5, hrtime(true)/1e9+1.1)); self::fail('Expired budget must abort.'); }
        catch (WorkflowClaimAborted) { self::assertCount(1, $transport->fake->requests); }
    }

    public function test_delivery_requires_the_previously_verified_preparation_before_io(): void
    {
        $transport = self::transport([]);
        $this->expectException(\InvalidArgumentException::class);
        try { self::boundary($transport, true); } finally { self::assertSame([], $transport->fake->requests); }
    }

    public function test_default_replay_still_refuses_a_preparation_without_committed_delivery(): void
    {
        $history=self::history(false);
        $this->expectException(WorkflowClaimAborted::class);
        new CommittedCancellationScopeHistory($history, self::runId(), self::workflowId(), new CancellationScopeHistory($history,self::runId()));
    }

    public function test_a_verified_preparation_cannot_be_borrowed_by_a_different_run_before_io(): void
    {
        $original=self::preparation(); $transport=self::transport([]);
        $this->expectException(\InvalidArgumentException::class);
        try {
            self::client($transport)->cancellationScopeBoundaryOnClaim('task/one','other-run',self::workflowId(),'original-owner',4,
                self::scopeId(),self::delivery(),'deliver',new RequestBudget(5),$original);
        } finally { self::assertSame([], $transport->fake->requests); }
    }

    public function test_the_published_default_and_unbounded_client_cannot_negotiate_scope_receipt_proof(): void
    {
        foreach ([new Client('https://server.example',workerProtocolVersion:'1.20'),
            (new Client('https://server.example'))->withBoundedWorkerRequests()] as $client) {
            try {
                $client->cancellationScopeBoundaryOnClaim('task/one',self::runId(),self::workflowId(),'original-owner',4,
                    self::scopeId(),self::delivery(),'prepare',new RequestBudget(5));
                self::fail('Source scope proof requires both candidate protocol and bounded requests.');
            } catch (LogicException) { self::assertTrue(true); }
        }
    }

    public function test_equivalent_timezone_representation_preserves_the_original_authority_instant(): void
    {
        $receipt=self::receipt(true); $receipt['authority_deadline_at']='2026-10-04T02:00:30.123456+02:00';
        $transport=self::transport([$receipt,self::page(self::history())]);
        $original=self::preparation(); $proved=self::boundary($transport,true,$original);
        self::assertEquals($original->authorityDeadline,$proved->authorityDeadline);
    }

    private static function boundary(BoundedTransport $transport, bool $delivering=false, ?CancellationScopeDeliveryReceipt $preparation=null, ?RequestBudget $budget=null): CancellationScopeDeliveryReceipt
    {
        return self::client($transport)->cancellationScopeBoundaryOnClaim('task/one', self::runId(), self::workflowId(), 'original-owner', 4,
            self::scopeId(), self::delivery(), $delivering ? 'deliver' : 'prepare', $budget ?? new RequestBudget(5,hrtime(true)/1e9+1.9), $preparation);
    }

    private static function preparation(): CancellationScopeDeliveryReceipt
    {
        return self::boundary(self::transport([self::receipt(),self::page(self::history(false))]));
    }

    private static function client(BoundedTransport $transport): Client
    {
        return (new Client('https://server.example', namespace:'sdk-scope-fixture', transport:$transport,
            controlToken:'control', workerToken:'worker', workerProtocolVersion:'1.20'))->withBoundedWorkerRequests();
    }

    private static function transport(array $responses, ?\Closure $handler=null): BoundedTransport
    {
        return new class($responses,$handler) implements BoundedTransport {
            public FakeTransport $fake; public array $timeouts=[];
            public function __construct(array $responses, ?\Closure $handler) { $this->fake=new FakeTransport($responses,$handler); }
            public function supportsBoundedRequests(): bool { return true; }
            public function send(string $method,string $uri,array $headers,?array $body=null): ?array { throw new LogicException('Bounded transport required.'); }
            public function sendBounded(string $method,string $uri,array $headers,?array $body,int $timeoutSeconds): ?array {
                $this->timeouts[]=$timeoutSeconds; return $this->fake->send($method,$uri,$headers,$body);
            }
        };
    }

    private static function fixture(): array
    {
        return json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'),true,flags:JSON_THROW_ON_ERROR)['unshielded'];
    }
    private static function history(bool $delivered=true): array { return array_slice(self::fixture()['history'],0,$delivered?7:6); }
    private static function runId(): string { return self::fixture()['task']['run_id']; }
    private static function workflowId(): string { return self::fixture()['task']['workflow_id']; }
    private static function scopeId(): string { return self::fixture()['scope_id']; }
    private static function body(): array
    {
        $payload=self::history()[5]['payload'];
        return array_intersect_key($payload,array_flip(['scope_id','request_id','sequence','call_kind','sequence_span','operation_sequence','operation_sequence_span']));
    }
    private static function delivery(): CancellationDelivery
    {
        return CancellationDelivery::fromPayload([...self::body(),'workflow_command_id'=>self::body()['request_id']]);
    }
    private static function receipt(bool $delivered=false): array
    {
        $history=self::history(); $prepared=$history[5]; $payload=$prepared['payload'];
        return ['prepared'=>true,'delivered'=>$delivered,'claim_released'=>false,'task_id'=>'task/one','workflow_run_id'=>self::runId(),
            'lease_owner'=>'original-owner','workflow_task_attempt'=>4,'created_task_ids'=>[],'reason'=>null,
            'history_event_id'=>$delivered?$history[6]['id']:$prepared['id'],'preparation_history_event_id'=>$prepared['id'],
            'history_refresh_page_token'=>'opaque-start','cancellation'=>$payload['cancellation'],
            'authority_deadline_at'=>$payload['authority_deadline_at'],'activity_members'=>[],'timer_members'=>[],'wait_members'=>[],'child_members'=>[],...self::body()];
    }
    private static function page(array $events,?string $next=null): array
    {
        return ['task_id'=>'task/one','workflow_task_attempt'=>4,'history_events'=>$events,'next_history_page_token'=>$next];
    }
}
