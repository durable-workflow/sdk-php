<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationWorkerTest extends TestCase
{
    #[DataProvider('deliveryReplyProvider')]
    public function testDeliveryRequiresCanonicalHistoryBeforeCleanup(string $reply): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->deliveryReply = $reply;
        $seen = null;
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$seen): string {
            try {
                $context->sleep(10);
            } catch (\DurableWorkflow\Exception\WorkflowCancelled $error) {
                $seen = $error->requestId;
                $context->cancellationShield(static fn () => $context->activity('cleanup'));
            }
            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame('request-1', $seen);
        self::assertSame(['schedule_activity'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame('cleanup', $transport->completions[0]['commands'][0]['activity_type']);
        self::assertSame([], $transport->failures);
        self::assertSame(1, count($transport->deliveries));
        self::assertSame(['lease_owner' => 'worker-1', 'workflow_task_attempt' => 3,
            'request_id' => 'request-1', 'sequence' => 1, 'call_kind' => 'timer', 'sequence_span' => 1], $transport->deliveries[0]);
        self::assertSame(['opaque-start', 'opaque-start'], array_column($transport->historyRequests, 'next_history_page_token'));
        foreach ([...$transport->historyRequests, ...$transport->deliveries, ...$transport->completions] as $body) {
            self::assertSame('worker-1', $body['lease_owner']);
            self::assertSame(3, $body['workflow_task_attempt']);
        }
    }

    public static function deliveryReplyProvider(): array
    {
        return [['accepted'], ['lost acknowledgment'], ['malformed acknowledgment']];
    }

    public function testCanonicalRefreshPreservesAcceptedStartBeforeWorkflowStarted(): void
    {
        $transport = new CooperativeWorkerTransport();
        array_unshift($transport->history, ['event_type' => 'StartAccepted', 'payload' => []]);
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): string {
            try { $context->sleep(10); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled $error) { return (string) $error->requestId; }
            return 'done';
        });
        $worker->tick(0);
        self::assertSame([], $transport->failures);
        self::assertCount(1, $transport->deliveries);
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
    }

    public function testObservedRequestRefreshesBothCanonicalPages(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->paginate = true;
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): string {
            try { $context->sleep(10); } catch (\DurableWorkflow\Exception\WorkflowCancelled) { return 'cancelled'; }
            return 'done';
        });
        $worker->tick(0);
        self::assertSame(['opaque-start', 'opaque-next', 'opaque-start', 'opaque-next'],
            array_column($transport->historyRequests, 'next_history_page_token'));
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
    }

    #[DataProvider('unprovenDeliveryProvider')]
    public function testUnprovenDeliveryDoesNotRunCleanupOrFailWorkflow(string $reply): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->deliveryReply = $reply;
        $caught = false;
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$caught): void {
            try { $context->sleep(10); } catch (\DurableWorkflow\Exception\WorkflowCancelled) { $caught = true; }
        });
        $worker->tick(0);
        self::assertFalse($caught);
        self::assertSame([], $transport->completions);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    public static function unprovenDeliveryProvider(): array
    {
        return [['not committed'], ['lost uncommitted acknowledgment']];
    }

    #[DataProvider('invalidRefreshProvider')]
    public function testMalformedCanonicalRefreshFailsClaimBeforeApplicationCode(string $fault): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->refreshFault = $fault;
        $called = false;
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$called): void { $called = true; });
        $worker->tick(0);
        self::assertFalse($called);
        self::assertSame([], $transport->deliveries);
        self::assertSame([], $transport->completions);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    public static function invalidRefreshProvider(): array
    {
        return [['missing request'], ['missing start'], ['malformed page'], ['malformed event'], ['malformed token'], ['repeated token']];
    }

    public function testDefaultWorkerRejectsObservedRequestsWithoutExecutingCode(): void
    {
        $transport = new CooperativeWorkerTransport();
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');
        $called = false;
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$called): void { $called = true; });
        $worker->tick(0);
        self::assertFalse($called);
        self::assertSame([], $transport->deliveries);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    #[DataProvider('changedObservationProvider')]
    public function testChangedHeartbeatCannotReplaceClaimObservation(string $field, string $value): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($field, $value): void {
            $transport->observation[$field] = $value;
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->sleep(10));
        $worker->tick(0);
        self::assertSame([], $transport->deliveries);
        self::assertSame([], $transport->completions);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    public static function changedObservationProvider(): array
    {
        return [['request_id', 'replacement'], ['requested_at', '2026-10-01T00:00:01Z'],
            ['cleanup_deadline_at', '2026-10-01T00:02:00Z']];
    }

    #[DataProvider('incapableRuntimeProvider')]
    public function testRegistrationRequiresExplicitRuntimeCapabilityBeforeAdvertising(mixed $version, mixed $capability): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->serverVersion = $version;
        $transport->serverCapability = $capability;
        try {
            $this->worker($transport)->run(0);
            self::fail('Registration must fail before advertising an unsupported capability.');
        } catch (WorkflowClaimAborted) {
            self::assertCount(1, $transport->requests);
            self::assertSame('https://server.example/api/cluster/info', $transport->requests[0]['uri']);
            self::assertSame('2', $transport->requests[0]['headers']['X-Durable-Workflow-Control-Plane-Version']);
        }
    }

    public static function incapableRuntimeProvider(): array
    {
        return [['1.19', true], ['1.20', false], ['1.20', 'true'], ['1.20', null], ['2.0', true], [null, true]];
    }

    public function testWorkerCannotEnableCancellationWithDefaultClientProtocol(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Worker(new Client('https://server.example'), 'queue', enableCooperativeCancellation: true);
    }

    public function testLostLocalLeaseDiscardsResultWithoutFailingWorkflow(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport): void {
            if ($transport->heartbeatCount === 3) { $transport->renewed = false; }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static fn (ActivityContext $context) => 'too late');
        $worker->tick(0);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->deliveries);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    #[DataProvider('localObservationProvider')]
    public function testLocalObservationDiscardsLateResultBeforeEncodingAndDeliversAtOriginalCall(bool $heartbeat): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $calls = 0;
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            try { $context->localActivity('effect'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) { $context->activity('cleanup'); }
        });
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($transport, &$calls, $heartbeat): \stdClass {
            ++$calls;
            $transport->makeRequestVisible();
            if ($heartbeat) { $context->heartbeat(); }
            return new \stdClass();
        });
        $worker->tick(0);
        self::assertSame(1, $calls);
        self::assertSame('local_activity', $transport->deliveries[0]['call_kind']);
        self::assertSame(1, $transport->deliveries[0]['sequence']);
        self::assertSame(['schedule_activity'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame('cleanup', $transport->completions[0]['commands'][0]['activity_type']);
        self::assertSame([], $transport->failures);
    }

    public static function localObservationProvider(): array
    {
        return [[false], [true]];
    }

    public function testEarlierLocalReportCommitsBeforeLaterCallDelivery(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            $context->localActivity('first');
            $context->localActivity('second');
        });
        $worker->registerActivity('first', static fn (ActivityContext $context) => 'committed first');
        $worker->registerActivity('second', static function (ActivityContext $context) use ($transport): void {
            $transport->makeRequestVisible();
            $context->heartbeat();
        });
        $worker->tick(0);
        self::assertSame(['record_local_activity'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame('first', $transport->completions[0]['commands'][0]['activity_type']);
        self::assertSame([], $transport->deliveries);
        self::assertSame([], $transport->failures);
    }

    public function testDeliveredRequestAllowsShieldedLocalCleanupWithOriginalIdentity(): void
    {
        $transport = new CooperativeWorkerTransport();
        $worker = $this->worker($transport);
        $calls = 0;
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): string {
            try { $context->sleep(10); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled $error) {
                $context->cancellationShield(static function () use ($context): void {
                    $context->throwIfCancellationRequested();
                    $context->localActivity('cleanup');
                });
                return (string) $error->requestId;
            }
            return 'done';
        });
        $worker->registerActivity('cleanup', static function (ActivityContext $context) use (&$calls): string {
            ++$calls;
            $context->heartbeat();
            return 'cleaned';
        });
        $worker->tick(0);
        self::assertSame(1, $calls);
        self::assertSame(['record_local_activity', 'complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame([], $transport->failures);
    }

    public function testShutdownDuringLocalCallbackAbandonsClaimWithoutRecordingResult(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($worker): \stdClass {
            $worker->requestShutdown();
            return new \stdClass();
        });
        $worker->tick(0);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->deliveries);
    }

    private function worker(CooperativeWorkerTransport $transport): Worker
    {
        return new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'worker-1', enableCooperativeCancellation: true);
    }
}

final class CooperativeWorkerTransport implements Transport
{
    public array $history;
    public array $observation;
    public array $completions = [];
    public array $deliveries = [];
    public array $failures = [];
    public array $historyRequests = [];
    public array $requests = [];
    public int $heartbeatCount = 0;
    public bool $renewed = true;
    public mixed $serverVersion = '1.20';
    public mixed $serverCapability = true;
    public bool $requestVisible = true;
    public bool $paginate = false;
    public string $deliveryReply = 'accepted';
    public ?string $refreshFault = null;
    public ?\Closure $onHeartbeat = null;

    public function __construct()
    {
        $this->observation = ['request_id' => 'request-1', 'requested_at' => '2026-10-01T00:00:00Z',
            'cleanup_deadline_at' => '2026-10-01T00:01:00Z', 'history_refresh_page_token' => 'opaque-start'];
        $this->history = [
            ['event_type' => 'WorkflowStarted', 'payload' => []],
            ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => $this->observation['requested_at'],
                'payload' => ['workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1',
                    'cleanup_deadline_at' => $this->observation['cleanup_deadline_at']]],
        ];
    }

    public function makeRequestVisible(): void
    {
        $this->requestVisible = true;
        $this->history[] = (new self())->history[1];
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        $this->requests[] = compact('method', 'uri', 'headers', 'body');
        if (str_ends_with($uri, '/cluster/info')) {
            return ['worker_protocol' => ['version' => $this->serverVersion,
                'server_capabilities' => ['cooperative_cancellation' => $this->serverCapability]]];
        }
        if (str_ends_with($uri, '/workflow-tasks/poll')) {
            return ['task' => ['task_id' => 'task-1', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1',
                'workflow_id' => 'workflow-1', 'run_id' => 'run-1', 'workflow_type' => 'cancel', 'payload_codec' => 'avro',
                'history_events' => [$this->history[0]],
                ...($this->requestVisible ? ['cancellation_request' => $this->observation] : [])], 'poll_status' => 'leased'];
        }
        if (str_ends_with($uri, '/poll')) { return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']; }
        if (str_ends_with($uri, '/heartbeat')) {
            ++$this->heartbeatCount;
            if ($this->onHeartbeat !== null) { ($this->onHeartbeat)($this); }
            return ['task_id' => 'task-1', 'lease_owner' => 'worker-1', 'workflow_task_attempt' => 3, 'renewed' => $this->renewed,
                ...($this->requestVisible ? ['cancellation_request' => $this->observation] : [])];
        }
        if (str_ends_with($uri, '/history')) {
            $this->historyRequests[] = $body;
            $events = $this->history;
            $next = null;
            if ($this->refreshFault === 'missing request') { $events = [$events[0]]; }
            if ($this->refreshFault === 'missing start') { $events = array_slice($events, 1); }
            if ($this->refreshFault === 'malformed page') { $events = 'invalid'; }
            if ($this->refreshFault === 'malformed event') { $events = ['invalid']; }
            if ($this->refreshFault === 'malformed token') { $next = []; }
            if ($this->refreshFault === 'repeated token') { $next = 'opaque-start'; }
            if ($this->paginate) {
                $events = $body['next_history_page_token'] === 'opaque-start' ? [$this->history[0]] : array_slice($this->history, 1);
                $next = $body['next_history_page_token'] === 'opaque-start' ? 'opaque-next' : null;
            }
            return ['history_events' => $events, 'next_history_page_token' => $next];
        }
        if (str_ends_with($uri, '/deliver-cancellation')) {
            $this->deliveries[] = $body;
            if (!in_array($this->deliveryReply, ['not committed', 'lost uncommitted acknowledgment'], true)) {
                $this->history[] = ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
                    'workflow_run_id' => 'run-1', 'workflow_command_id' => $body['request_id'], 'sequence' => $body['sequence'],
                    'call_kind' => $body['call_kind'], 'sequence_span' => $body['sequence_span'],
                    ...array_intersect_key($body, array_flip(['operation_sequence', 'operation_sequence_span'])),
                ]];
            }
            if (in_array($this->deliveryReply, ['lost acknowledgment', 'lost uncommitted acknowledgment'], true)) {
                throw new TransportException('Delivery acknowledgment lost.');
            }
            return ['delivered' => true, 'task_id' => 'task-1', ...array_diff_key($body, array_flip(['lease_owner', 'workflow_task_attempt'])),
                'operation_sequence' => $body['operation_sequence'] ?? null,
                'operation_sequence_span' => $body['operation_sequence_span'] ?? 1,
                ...($this->deliveryReply === 'malformed acknowledgment' ? ['call_kind' => 'invalid'] : [])];
        }
        if (str_ends_with($uri, '/complete')) { $this->completions[] = $body; return ['completed' => true]; }
        if (str_ends_with($uri, '/fail')) { $this->failures[] = $body; return ['failed' => true]; }
        throw new \RuntimeException('Unexpected request '.$method.' '.$uri);
    }
}
