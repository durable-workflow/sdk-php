<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker\CancellationDelivery;
use DurableWorkflow\Worker\CancellationScopeDeliveryClaim;
use DurableWorkflow\Worker\CancellationScopeDeliveryIntent;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeDeliveryClaimTest extends TestCase
{
    public function test_preparation_replay_and_delivery_preserve_one_original_claim_and_budget(): void
    {
        $fixture = self::fixture();
        $transport = self::transport(self::responses($fixture));
        $cleaned = false;
        $handler = static function (WorkflowContext $context) use (&$cleaned): string {
            return $context->cancellationScope(static function () use ($context, &$cleaned): string {
                return $context->cancellationScope(static function () use ($context, &$cleaned): string {
                    try { $context->sleep(60); }
                    catch (WorkflowCancelled) { $cleaned = true; return 'cleaned'; }
                    return 'ordinary';
                });
            });
        };
        $replayer = new Replayer(new AvroPayloadCodec());
        $replay = static fn (array $history) => $replayer->replay($handler, $history, [], 'default', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        $selected = $replay(array_slice($fixture['history'], 0, 5))->cancellationScopeDelivery;
        self::assertInstanceOf(CancellationScopeDeliveryIntent::class, $selected);
        $claim = self::claim($transport, $fixture, $selected, new RequestBudget(5, hrtime(true) / 1e9 + 1.9));
        $prepared = $claim->prepare();
        self::assertSame($prepared, $claim->prepare());
        self::assertCount(2, $transport->fake->requests);
        self::assertFalse($cleaned);
        $replayed = $replay($prepared->history)->cancellationScopeDelivery;
        $delivered = $claim->deliver($replayed);
        self::assertSame($delivered, $claim->deliver($replayed));
        self::assertSame($prepared->preparationHistoryEventId, $delivered->preparationHistoryEventId);
        self::assertSame($selected->context->toArray(), $delivered->context->toArray());
        self::assertCount(4, $transport->fake->requests);
        self::assertSame([1, 1, 1, 1], $transport->timeouts);
        self::assertFalse($cleaned);
        $final = $replay($delivered->history);
        self::assertTrue($cleaned);
        self::assertNull($final->cancellationScopeDelivery);
        self::assertSame('cleaned', (new AvroPayloadCodec())->decodeEnvelope($final->commands[0]['result']));
        foreach ($transport->fake->requests as $request) {
            self::assertSame('replacement-owner', $request['body']['lease_owner']);
            self::assertSame(19, $request['body']['workflow_task_attempt']);
        }
    }

    public function test_newly_proved_narrower_ceiling_bounds_every_history_read_and_delivery(): void
    {
        $fixture = self::fixture(1.9);
        $transport = self::transport(self::responses($fixture));
        $claim = self::claim($transport, $fixture);
        $prepared = $claim->prepare();
        $claim->deliver(self::intent($fixture, true));
        self::assertSame([4, 1, 1, 1], $transport->timeouts);
        self::assertSame(self::intent($fixture)->context->toArray(), $prepared->context->toArray());
        self::assertLessThan($prepared->context->deadline(), $prepared->authorityDeadline);
    }

    public function test_new_ceiling_in_final_fraction_refuses_history_before_any_delivery(): void
    {
        $fixture = self::fixture(0.9);
        $transport = self::transport(self::responses($fixture));
        $claim = self::claim($transport, $fixture);
        $this->expectException(WorkflowClaimAborted::class);
        try { $claim->prepare(); }
        finally { self::assertCount(1, $transport->fake->requests); }
    }

    public function test_delivery_refuses_the_unprepared_selection_and_requires_replay_before_io(): void
    {
        $fixture = self::fixture();
        $transport = self::transport(self::responses($fixture));
        $claim = self::claim($transport, $fixture);
        try { $claim->deliver(self::intent($fixture)); self::fail('Preparation is required.'); }
        catch (WorkflowClaimAborted) { self::assertCount(0, $transport->fake->requests); }
        $claim->prepare();
        try { $claim->deliver(self::intent($fixture)); self::fail('Prepared history must be replayed.'); }
        catch (WorkflowClaimAborted) { self::assertCount(2, $transport->fake->requests); }
    }

    #[DataProvider('changes')]
    public function test_replayed_preparation_cannot_change_original_authority_before_delivery(string $change): void
    {
        $fixture = self::fixture();
        $transport = self::transport(self::responses($fixture));
        $claim = self::claim($transport, $fixture);
        $claim->prepare();
        $changed = self::changedIntent($fixture, $change);
        $this->expectException(WorkflowClaimAborted::class);
        try { $claim->deliver($changed); }
        finally { self::assertCount(2, $transport->fake->requests); }
    }

    public static function changes(): iterable
    {
        foreach (['context', 'boundary', 'preparation', 'deadline'] as $change) { yield $change => [$change]; }
    }

    #[DataProvider('preparedChanges')]
    public function test_replacement_claim_cannot_rebind_an_existing_original_preparation(string $change): void
    {
        $fixture = self::fixture();
        $transport = self::transport(self::responses($fixture));
        $claim = self::claim($transport, $fixture, self::changedIntent($fixture, $change));
        $this->expectException(WorkflowClaimAborted::class);
        try { $claim->prepare(); }
        finally { self::assertCount(2, $transport->fake->requests); }
    }

    public static function preparedChanges(): iterable { yield 'identity' => ['preparation']; yield 'ceiling' => ['deadline']; }

    public function test_new_canonical_context_cannot_replace_the_original_selected_metadata(): void
    {
        $original = self::fixture();
        $changed = $original;
        foreach ([4, 5, 6] as $index) { $changed['history'][$index]['payload']['cancellation']['root_context']['reason'] = 'substituted'; }
        $transport = self::transport(self::responses($changed));
        $claim = self::claim($transport, $original);
        $this->expectException(WorkflowClaimAborted::class);
        try { $claim->prepare(); }
        finally { self::assertCount(2, $transport->fake->requests); }
    }

    #[DataProvider('phases')]
    public function test_lost_reply_reconciles_one_original_boundary_without_renewing_the_budget(bool $delivering): void
    {
        $fixture = self::fixture();
        $responses = self::responses($fixture);
        array_splice($responses, $delivering ? 2 : 0, 0, [new TransportException('Lost accepted reply.', transientConnectionFailure: true)]);
        $transport = self::transport($responses);
        $claim = self::claim($transport, $fixture, budget: new RequestBudget(5, hrtime(true) / 1e9 + 1.9));
        $claim->prepare();
        $claim->deliver(self::intent($fixture, true));
        $index = $delivering ? 2 : 0;
        self::assertSame($transport->fake->requests[$index], $transport->fake->requests[$index + 1]);
        self::assertSame([1, 1, 1, 1, 1], $transport->timeouts);
    }

    public static function phases(): iterable { yield 'prepare' => [false]; yield 'deliver' => [true]; }

    public function test_expired_existing_preparation_refuses_even_its_first_reconciliation_request(): void
    {
        $fixture = self::fixture(-1);
        $transport = self::transport([]);
        $claim = self::claim($transport, $fixture, self::intent($fixture, true));
        $this->expectException(TransportException::class);
        try { $claim->prepare(); }
        finally { self::assertCount(0, $transport->fake->requests); }
    }

    public function test_a_later_budget_restriction_cannot_reopen_expired_authority(): void
    {
        $budget = new RequestBudget(5);
        $budget->restrictAuthorityDeadline(hrtime(true) / 1e9 - 1);
        $budget->restrictAuthorityDeadline(hrtime(true) / 1e9 + 60);
        $budget->restrictWallAuthorityDeadline(new DateTimeImmutable('+1 hour'));
        $this->expectException(TransportException::class);
        $budget->remainingSeconds();
    }

    public function test_restriction_cannot_extend_the_original_request_lifetime(): void
    {
        $budget = new RequestBudget(2);
        $budget->restrictAuthorityDeadline(hrtime(true) / 1e9 + 60);
        self::assertSame(1, $budget->remainingSeconds());
    }

    /** Native fixture shape/IDs are retained, with explicitly synthetic current timestamps. */
    private static function fixture(?float $remaining = null): array
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR)['unshielded'];
        $base = microtime(true) - 10;
        $origin = (float) (new DateTimeImmutable('2026-10-04T00:00:00.123456Z'))->format('U.u');
        $shift = static function (mixed $value) use (&$shift, $base, $origin): mixed {
            if (is_array($value)) { return array_map($shift, $value); }
            if (is_string($value) && str_starts_with($value, '2026-10-04T')) {
                return self::timestamp($base + (float) (new DateTimeImmutable($value))->format('U.u') - $origin);
            }
            return $value;
        };
        $fixture = $shift($fixture);
        if ($remaining !== null) {
            foreach ([5, 6] as $index) { $fixture['history'][$index]['payload']['authority_deadline_at'] = self::timestamp(microtime(true) + $remaining); }
            // One exact original ceiling for both events.
            $fixture['history'][6]['payload']['authority_deadline_at'] = $fixture['history'][5]['payload']['authority_deadline_at'];
        }
        return $fixture;
    }

    private static function timestamp(float $time): string
    {
        return DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', $time))->format('Y-m-d\TH:i:s.u\Z');
    }

    private static function intent(array $fixture, bool $prepared = false): CancellationScopeDeliveryIntent
    {
        $payload = $fixture['history'][5]['payload'];
        return new CancellationScopeDeliveryIntent(ScopedCancellationContext::fromArray($payload['cancellation']),
            CancellationDelivery::fromPayload([...$payload, 'workflow_command_id' => $payload['request_id']]),
            $prepared ? $fixture['history'][5]['id'] : null, $prepared ? new DateTimeImmutable($payload['authority_deadline_at']) : null);
    }

    private static function changedIntent(array $fixture, string $change): CancellationScopeDeliveryIntent
    {
        $original = self::intent($fixture, true);
        $context = $original->context->toArray();
        if ($change === 'context') { $context['root_context']['reason'] = 'substituted'; }
        $boundary = $change === 'boundary'
            ? CancellationDelivery::fromPayload([...$fixture['history'][5]['payload'], 'call_kind' => 'activity', 'workflow_command_id' => $original->context->requestId])
            : $original->boundary;
        return new CancellationScopeDeliveryIntent(ScopedCancellationContext::fromArray($context), $boundary,
            $change === 'preparation' ? 'substituted' : $original->preparationHistoryEventId,
            $change === 'deadline' ? $original->authorityDeadline->modify('-1 second') : $original->authorityDeadline);
    }

    private static function claim(BoundedTransport $transport, array $fixture, ?CancellationScopeDeliveryIntent $intent = null, ?RequestBudget $budget = null): CancellationScopeDeliveryClaim
    {
        $client = (new Client('https://server.example', namespace: 'sdk-scope-fixture', transport: $transport,
            workerToken: 'worker', workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
        return new CancellationScopeDeliveryClaim($client, 'replacement-task', 'replacement-owner', 19, $intent ?? self::intent($fixture), $budget);
    }

    private static function responses(array $fixture): array
    {
        $history = $fixture['history'];
        $payload = $history[5]['payload'];
        $receipt = ['prepared' => true, 'delivered' => false, 'claim_released' => false, 'task_id' => 'replacement-task',
            'workflow_run_id' => $fixture['task']['run_id'], 'lease_owner' => 'replacement-owner', 'workflow_task_attempt' => 19,
            'created_task_ids' => [], 'reason' => null, 'history_event_id' => $history[5]['id'],
            'preparation_history_event_id' => $history[5]['id'], 'history_refresh_page_token' => 'original-cursor',
            'cancellation' => $payload['cancellation'], 'authority_deadline_at' => $payload['authority_deadline_at'],
            'activity_members' => [], 'timer_members' => [], 'wait_members' => [], 'child_members' => [],
            ...array_intersect_key($payload, array_flip(['scope_id', 'request_id', 'sequence', 'call_kind', 'sequence_span', 'operation_sequence', 'operation_sequence_span']))];
        $page = ['task_id' => 'replacement-task', 'workflow_task_attempt' => 19, 'history_events' => array_slice($history, 0, 6), 'next_history_page_token' => null];
        return [$receipt, $page, [...$receipt, 'delivered' => true, 'history_event_id' => $history[6]['id']], [...$page, 'history_events' => $history]];
    }

    private static function transport(array $responses): ScopeDeliveryClaimTransport
    {
        return new ScopeDeliveryClaimTransport($responses);
    }
}

final class ScopeDeliveryClaimTransport implements BoundedTransport
{
    public FakeTransport $fake;
    public array $timeouts = [];
    public function __construct(array $responses) { $this->fake = new FakeTransport($responses); }
    public function supportsBoundedRequests(): bool { return true; }
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array { throw new LogicException('Bounded transport required.'); }
    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $this->timeouts[] = $timeoutSeconds;
        return $this->fake->send($method, $uri, $headers, $body);
    }
}
