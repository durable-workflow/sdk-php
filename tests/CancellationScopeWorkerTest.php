<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeWorkerTest extends TestCase
{
    #[DataProvider('startingBoundaries')]
    public function test_managed_worker_replays_each_canonical_boundary_before_cleanup(int $count): void
    {
        $transport = new ScopeWorkerTransport(self::fixture(), $count);
        $seen = [];
        $worker = self::worker($transport);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport));
        $worker->run(0);
        self::assertSame([], $transport->failures);
        self::assertCount(1, $transport->completions);
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertCount(1, $seen);
        $original = $transport->fixture['history'][5];
        self::assertSame($original['payload']['request_id'], $seen[0]['request_id']);
        self::assertSame(CancellationContext::fromArray($original['payload']['cancellation']['root_context'])->toArray(), $seen[0]['context']);
        self::assertSame(7, $seen[0]['history_count']);
        $phases = array_column($transport->scopeRequests(), 'phase');
        self::assertSame($count === 7 ? [] : ['prepare', 'deliver'], $phases);
        foreach ($transport->requests as $request) {
            if (in_array($request['phase'], ['prepare', 'deliver', 'history', 'complete'], true)) {
                self::assertSame('replacement-owner', $request['body']['lease_owner']);
                self::assertSame(19, $request['body']['workflow_task_attempt']);
            }
            if (in_array($request['phase'], ['prepare', 'deliver', 'history'], true)) {
                self::assertGreaterThanOrEqual(1, $request['timeout']);
                self::assertLessThanOrEqual(5, $request['timeout']);
            }
        }
        // This partial profile does not advertise complete portable scope support.
        $registered = array_values(array_filter($transport->requests, static fn (array $r): bool => $r['phase'] === 'register'))[0]['body'];
        self::assertNotContains('cancellation_scopes', $registered['capabilities']);
        self::assertArrayNotHasKey('cancellation_scopes', $registered['capability_manifest']);
    }

    public static function startingBoundaries(): iterable
    {
        yield 'requested' => [5];
        yield 'replacement prepared' => [6];
        yield 'replacement delivered' => [7];
    }

    #[DataProvider('lostPhases')]
    public function test_lost_reply_reconciles_the_original_mutation_without_entering_cleanup_early(string $phase): void
    {
        $transport = new ScopeWorkerTransport(self::fixture());
        $transport->loseReply = $phase;
        $seen = [];
        $worker = self::worker($transport);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport));
        self::assertTrue($worker->tick(0));
        self::assertCount(1, $seen);
        self::assertSame(7, $seen[0]['history_count']);
        self::assertSame([], $transport->failures);
        $requests = array_values(array_filter($transport->scopeRequests(), static fn (array $r): bool => $r['phase'] === $phase));
        self::assertCount(2, $requests);
        self::assertSame($requests[0]['body'], $requests[1]['body']);
        self::assertLessThanOrEqual($requests[0]['timeout'], $requests[1]['timeout']);
        self::assertSame(CancellationContext::fromArray($transport->fixture['history'][5]['payload']['cancellation']['root_context'])->toArray(), $seen[0]['context']);
    }

    public static function lostPhases(): iterable { yield 'prepare' => ['prepare']; yield 'deliver' => ['deliver']; }

    #[DataProvider('invalidAuthorities')]
    public function test_invalid_authority_never_runs_cleanup_or_completes_the_workflow(string $fault, int $scopeRequests): void
    {
        $transport = new ScopeWorkerTransport(self::fixture($fault === 'expired' ? -1 : null), $fault === 'expired' ? 6 : 5);
        $transport->fault = $fault;
        $seen = [];
        $worker = self::worker($transport);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport));
        self::assertTrue($worker->tick(0));
        self::assertSame([], $seen);
        self::assertSame([], $transport->completions);
        self::assertCount(1, $transport->failures);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
        self::assertCount($scopeRequests, $transport->scopeRequests());
    }

    public static function invalidAuthorities(): iterable
    {
        yield 'expired preparation' => ['expired', 0];
        yield 'preparation owner' => ['wrong owner', 1];
        yield 'prepared history missing' => ['missing preparation', 1];
        yield 'delivery history missing' => ['missing delivery', 2];
        yield 'prepared metadata changed' => ['changed context', 1];
        yield 'pending physical stop' => ['pending stop', 2];
    }

    public function test_the_same_narrowed_authority_covers_preparation_history_and_delivery(): void
    {
        $transport = new ScopeWorkerTransport(self::fixture(1.9));
        $seen = [];
        $worker = self::worker($transport);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport));
        self::assertTrue($worker->tick(0));
        self::assertSame([], $transport->failures);
        self::assertCount(1, $seen);
        $bounded = array_values(array_filter($transport->requests, static fn (array $r): bool => in_array($r['phase'], ['history', 'deliver'], true)));
        self::assertSame([1, 1, 1], array_column($bounded, 'timeout'));
    }

    public function test_scope_coordination_is_disabled_by_default(): void
    {
        $transport = new ScopeWorkerTransport(self::fixture());
        $seen = [];
        $worker = self::worker($transport, scopes: false);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport));
        self::assertTrue($worker->tick(0));
        self::assertSame([], $seen);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->scopeRequests());
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
        self::assertStringContainsString('cancellation_scope_execution_not_supported', $transport->failures[0]['failure']['message']);
    }

    #[DataProvider('prefixReplies')]
    public function test_retained_prefix_uses_the_original_scope_budget_before_preparation(bool $lostReply): void
    {
        $fixture = self::fixture(1.9);
        $deadline = $fixture['history'][5]['payload']['authority_deadline_at'];
        foreach ([4, 5, 6] as $index) {
            $fixture['history'][$index]['payload']['cancellation']['root_context']['cleanup_deadline_at'] = $deadline;
            $fixture['history'][$index]['payload']['cancellation']['lineage'][0]['cleanup_deadline_at'] = $deadline;
        }
        foreach ([5, 6] as $index) { $fixture['history'][$index]['payload']['sequence'] = 4; }
        $transport = new ScopeWorkerTransport($fixture);
        if ($lostReply) { $transport->loseReply = 'checkpoint'; }
        $seen = [];
        $worker = self::worker($transport);
        $worker->registerWorkflow('scoped', self::handler($seen, $transport, prefix: true));
        self::assertTrue($worker->tick(0));
        self::assertSame([], $transport->failures);
        self::assertCount(1, $seen);
        self::assertSame(8, $seen[0]['history_count']);
        $bounded = array_values(array_filter($transport->requests,
            static fn (array $r): bool => in_array($r['phase'], ['checkpoint', 'prepare', 'deliver', 'history'], true)));
        self::assertSame(array_fill(0, count($bounded), 1), array_column($bounded, 'timeout'));
        self::assertSame($lostReply ? ['checkpoint', 'checkpoint', 'history', 'prepare', 'history', 'deliver', 'history']
            : ['checkpoint', 'history', 'prepare', 'history', 'deliver', 'history'], array_column($bounded, 'phase'));
        if ($lostReply) { self::assertSame($bounded[0]['body'], $bounded[1]['body']); }
        self::assertSame(1, count(array_filter($transport->fixture['history'], static fn (array $e): bool => $e['event_type'] === 'SideEffectRecorded')));
    }

    public static function prefixReplies(): iterable { yield 'accepted' => [false]; yield 'lost reply' => [true]; }

    public function test_scope_opt_in_requires_explicit_cooperative_worker_authority(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Worker(new Client('https://server.example'), 'queue', enableCancellationScopes: true);
    }

    private static function worker(ScopeWorkerTransport $transport, bool $scopes = true): Worker
    {
        return new Worker(new Client('https://server.example', namespace: 'sdk-scope-fixture', workerToken: 'worker', controlToken: 'control',
            workerProtocolVersion: '1.20', transport: $transport), 'default', workerId: 'replacement-owner',
            enableCooperativeCancellation: true, enableCancellationScopes: $scopes);
    }

    private static function handler(array &$seen, ScopeWorkerTransport $transport, bool $prefix = false): \Closure
    {
        return static function (WorkflowContext $context) use (&$seen, $transport, $prefix): string {
            return $context->cancellationScope(static function () use ($context, &$seen, $transport, $prefix): string {
                $result = $context->cancellationScope(static function () use ($context, &$seen, $transport, $prefix): string {
                    if ($prefix && $context->sideEffect(static fn (): string => 'marker') !== 'marker') {
                        throw new LogicException('The original retained prefix changed.');
                    }
                    try { $context->sleep(60); }
                    catch (WorkflowCancelled $error) {
                        $seen[] = ['request_id' => $error->requestId, 'context' => $context->cancellationContext()->toArray()['root_context'],
                            'history_count' => $transport->historyCount];
                        return 'cleaned';
                    }
                    return 'ordinary';
                });
                if ($context->cancellationContext() !== null) { throw new LogicException('The unaffected parent borrowed cancellation.'); }
                return $result;
            });
        };
    }

    /** Native fixture shape/IDs retained with explicitly synthetic current timestamps. */
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
            $ceiling = self::timestamp(microtime(true) + $remaining);
            foreach ([5, 6] as $index) { $fixture['history'][$index]['payload']['authority_deadline_at'] = $ceiling; }
        }
        return $fixture;
    }

    private static function timestamp(float $time): string
    {
        return DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', $time))->format('Y-m-d\TH:i:s.u\Z');
    }
}

/** An installed SDK Worker drives these synthetic claim exchanges, not a mocked Worker. */
final class ScopeWorkerTransport implements BoundedTransport
{
    public array $requests = [];
    public array $completions = [];
    public array $failures = [];
    public ?string $loseReply = null;
    public ?string $fault = null;
    public int $historyCount = 0;
    private bool $checkpointed = false;
    private int $preparationIndex = 5;

    public function __construct(public array $fixture, private int $count = 5) {}
    public function supportsBoundedRequests(): bool { return true; }
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->exchange($uri, $body, null);
    }
    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->exchange($uri, $body, $timeoutSeconds);
    }
    public function scopeRequests(): array
    {
        return array_values(array_filter($this->requests, static fn (array $r): bool => in_array($r['phase'], ['prepare', 'deliver'], true)));
    }

    private function exchange(string $uri, ?array $body, ?int $timeout): array
    {
        $phase = basename($uri);
        $this->requests[] = compact('phase', 'body', 'timeout');
        if (str_ends_with($uri, '/cluster/info')) {
            return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => ['cooperative_cancellation' => true]]];
        }
        if ($phase === 'register') { return ['registered' => true]; }
        if (str_contains($uri, '/worker/registrations/')) {
            return ['worker_id' => 'replacement-owner', 'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
        }
        if (str_ends_with($uri, '/workflow-tasks/poll')) {
            $this->historyCount = $this->count;
            return ['poll_status' => 'leased', 'task' => [...$this->fixture['task'], 'task_id' => 'replacement-task',
                'workflow_task_attempt' => 19, 'lease_owner' => 'replacement-owner', 'workflow_type' => 'scoped',
                'payload_codec' => 'avro', 'history_events' => array_slice($this->fixture['history'], 0, $this->count)]];
        }
        if ($phase === 'poll') { return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']; }
        if ($phase === 'heartbeat') {
            return ['task_id' => 'replacement-task', 'lease_owner' => 'replacement-owner', 'workflow_task_attempt' => 19, 'renewed' => true];
        }
        if ($phase === 'history') {
            $count = match ($this->fault) { 'missing preparation' => 5, 'missing delivery' => 6, default => $this->count };
            $history = array_slice($this->fixture['history'], 0, $count);
            if ($this->fault === 'changed context') {
                foreach ([4, 5] as $index) { $history[$index]['payload']['cancellation']['root_context']['reason'] = 'substituted'; }
            }
            $this->historyCount = $count;
            return ['task_id' => 'replacement-task', 'workflow_task_attempt' => 19, 'history_events' => $history, 'next_history_page_token' => null];
        }
        if ($phase === 'checkpoint') {
            if (!$this->checkpointed) {
                array_splice($this->fixture['history'], 5, 0, [[
                    'id' => 'synthetic-prefix-event', 'namespace' => 'sdk-scope-fixture', 'event_type' => 'SideEffectRecorded',
                    'timestamp' => $this->fixture['history'][4]['timestamp'],
                    'payload' => ['sequence' => $body['start_sequence'], 'result' => $body['commands'][0]['result']],
                ]]);
                foreach ($this->fixture['history'] as $index => &$event) { $event['sequence'] = $index + 1; }
                unset($event);
                ++$this->preparationIndex;
                ++$this->count;
                $this->checkpointed = true;
            }
            if ($this->loseReply === 'checkpoint') {
                $this->loseReply = null;
                throw new TransportException('Accepted checkpoint reply lost.', transientConnectionFailure: true);
            }
            return ['checkpointed' => true, 'duplicate' => true, 'reason' => null, 'task_id' => 'replacement-task',
                'workflow_run_id' => $this->fixture['task']['run_id'], 'lease_owner' => 'replacement-owner', 'workflow_task_attempt' => 19,
                'checkpoint_id' => $body['checkpoint_id'], 'start_sequence' => $body['start_sequence'],
                'next_sequence' => $body['start_sequence'] + count($body['commands']), 'history_refresh_page_token' => 'prefix-cursor'];
        }
        if (in_array($phase, ['prepare', 'deliver'], true)) {
            $this->count = $phase === 'prepare' ? max($this->preparationIndex + 1, $this->count) : $this->preparationIndex + 2;
            if ($this->loseReply === $phase) {
                $this->loseReply = null;
                throw new TransportException('Accepted mutation reply lost.', transientConnectionFailure: true);
            }
            $history = $this->fixture['history'];
            $payload = $history[$this->preparationIndex]['payload'];
            return ['prepared' => true, 'delivered' => $phase === 'deliver', 'claim_released' => false,
                'task_id' => 'replacement-task', 'workflow_run_id' => $this->fixture['task']['run_id'],
                'lease_owner' => $this->fault === 'wrong owner' ? 'borrowed-owner' : 'replacement-owner', 'workflow_task_attempt' => 19,
                'created_task_ids' => [], 'reason' => $this->fault === 'pending stop' && $phase === 'deliver' ? 'pending' : null,
                'history_event_id' => $history[$this->preparationIndex + ($phase === 'deliver' ? 1 : 0)]['id'],
                'preparation_history_event_id' => $history[$this->preparationIndex]['id'], 'history_refresh_page_token' => 'original-cursor',
                'cancellation' => $payload['cancellation'], 'authority_deadline_at' => $payload['authority_deadline_at'],
                'activity_members' => [], 'timer_members' => [], 'wait_members' => [], 'child_members' => [],
                ...array_intersect_key($payload, array_flip(['scope_id', 'request_id', 'sequence', 'call_kind', 'sequence_span', 'operation_sequence', 'operation_sequence_span'])),
                ...($this->fault === 'pending stop' && $phase === 'deliver' ? ['delivered' => false] : [])];
        }
        if ($phase === 'complete') { $this->completions[] = $body; return ['completed' => true]; }
        if ($phase === 'fail') { $this->failures[] = $body; return ['failed' => true]; }
        throw new LogicException('Unexpected scope Worker request '.$uri);
    }
}
