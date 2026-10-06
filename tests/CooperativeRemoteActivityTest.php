<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class CooperativeRemoteActivityTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        if (!CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Remote activity supervision requires Unix process control.');
        }
        $this->directory = sys_get_temp_dir().'/dw-remote-owner-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
    }

    public static function stopProvider(): array
    {
        return [['cancel'], ['lease'], ['backend'], ['mismatch'], ['shutdown']];
    }

    #[DataProvider('stopProvider')]
    public function testBlockedRemoteCallbackStopsWithoutProgressOrLatePublication(string $reason): void
    {
        $transport = new RemoteOwnerTransport();
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $transport->onAcknowledgment = static function () use (&$pids): void {
            self::assertCount(2, $pids);
            foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0), 'Receipt cannot precede actual process stop.'); }
        };
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($entered, $reason, $worker): void {
            if (!is_file($entered)) { return; }
            if ($reason === 'backend') { throw new TransportException('status unavailable'); }
            if ($reason === 'shutdown') { $worker->requestShutdown(); return; }
            if ($reason === 'mismatch') { $transport->status['lease_owner'] = 'replacement'; return; }
            $transport->status['can_continue'] = false;
            $transport->status['cancel_requested'] = $reason === 'cancel';
            $transport->status['reason'] = $reason === 'cancel' ? 'activity_cancelled' : 'lease_expired';
            if ($reason === 'cancel') { $transport->status['cancellation_acknowledgement'] = RemoteOwnerTransport::receipt(); }
        };
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'too late';
        });
        $started = microtime(true);
        self::assertTrue($worker->tick(0));
        self::assertLessThan(3, microtime(true) - $started);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->userHeartbeats);
        self::assertSame($reason === 'cancel' ? [['activity_attempt_id' => 'attempt', 'lease_owner' => 'owner', 'request_id' => 'local-request']] : [],
            $transport->acknowledgments);
        self::assertCount(2, $pids);
        foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
        foreach ($transport->requests as $request) {
            self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version'] ?? '1.20');
            self::assertGreaterThanOrEqual(1, $request['timeout']);
            self::assertLessThanOrEqual(5, $request['timeout']);
        }
    }

    public static function invalidReceiptProvider(): array
    {
        return [
            ['absent', null], ['request_id', ''], ['root_request_id', []],
            ['cleanup_deadline_at', null], ['cancellation_history_event_id', false],
            ['callback_state', 'fenced'], ['cancel_requested', false], ['heartbeat_recorded', true],
            ['task_id', 'another-task'], ['activity_attempt_id', 'another-attempt'],
            ['lease_owner', 'another-owner'], ['can_continue', true],
        ];
    }

    #[DataProvider('invalidReceiptProvider')]
    public function testStopWithoutCanonicalCancellationCannotProduceReceipt(string $field, mixed $value): void
    {
        $transport = new RemoteOwnerTransport();
        $entered = $this->directory.'/entered';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($entered, $field, $value): void {
            if (!is_file($entered)) { return; }
            $transport->status['can_continue'] = false;
            $transport->status['cancel_requested'] = true;
            $receipt = RemoteOwnerTransport::receipt();
            if (in_array($field, ['cancel_requested', 'heartbeat_recorded', 'task_id', 'activity_attempt_id', 'lease_owner', 'can_continue'], true)) { $transport->status[$field] = $value; }
            else { $receipt[$field] = $value; }
            $transport->status['cancellation_acknowledgement'] = $field === 'absent' ? null : $receipt;
        };
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered): never {
            file_put_contents($entered, 'entered');
            sleep(60);
            throw new RuntimeException('Late callback must not run.');
        });
        $worker->tick(0);
        self::assertSame([], $transport->acknowledgments);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        $event = $field === 'cancel_requested' ? 'worker.activity_cancellation_acknowledgement_skipped'
            : 'worker.activity_cancellation_acknowledgement_failed';
        self::assertContains($event, $transport->events);
        self::assertNotContains('worker.activity_cancellation_acknowledged', $transport->events);
        $report = array_values(array_filter($transport->diagnostics, static fn (array $entry): bool => $entry['event'] === $event));
        self::assertCount(1, $report);
        self::assertTrue($report[0]['context']['callback_stopped']);
        self::assertSame('task', $report[0]['context']['task_id']);
        self::assertSame('attempt', $report[0]['context']['activity_attempt_id']);
        self::assertCount(2, $pids);
        foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
    }

    public static function failedAcknowledgmentProvider(): array
    {
        return [['transport'], ['refused'], ['mismatched'], ['unproved']];
    }

    #[DataProvider('failedAcknowledgmentProvider')]
    public function testUnprovedReceiptCannotBeReportedAsAcceptedOrPublishAResult(string $failure): void
    {
        $transport = new RemoteOwnerTransport();
        $transport->acknowledgmentFailure = $failure;
        $entered = $this->directory.'/entered';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($entered): void {
            if (!is_file($entered)) { return; }
            $transport->status['can_continue'] = false;
            $transport->status['cancel_requested'] = true;
            $transport->status['cancellation_acknowledgement'] = RemoteOwnerTransport::receipt();
        };
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered): never {
            file_put_contents($entered, 'entered');
            sleep(60);
            throw new RuntimeException('Late callback must not run.');
        });
        $worker->tick(0);
        self::assertCount(1, $transport->acknowledgments);
        self::assertNotContains('worker.activity_cancellation_acknowledged', $transport->events);
        self::assertContains('worker.activity_cancellation_acknowledgement_failed', $transport->events);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->userHeartbeats);
        foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
    }

    public static function transientReceiptProvider(): array
    {
        return [['status'], ['connection'], ['upstream'], ['lost_reply']];
    }

    #[DataProvider('transientReceiptProvider')]
    public function testJoinedCallbackReceiptRecoversTransientFailureWithOriginalIdentity(string $fault): void
    {
        $transport = new RemoteOwnerTransport();
        $transport->transientAcknowledgmentFault = $fault === 'status' ? null : $fault;
        $entered = $this->directory.'/entered';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $statusFailed = false;
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($entered, $fault, &$statusFailed, &$pids): void {
            if (!is_file($entered)) { return; }
            $transport->status['can_continue'] = false;
            $transport->status['cancel_requested'] = true;
            $transport->status['cancellation_acknowledgement'] = RemoteOwnerTransport::receipt();
            if ($fault === 'status' && !$statusFailed && count($pids) === 2
                && !posix_kill($pids[0], 0) && !posix_kill($pids[1], 0)) {
                $statusFailed = true;
                throw new TransportException('Stop receipt discovery connection failed.', transientConnectionFailure: true);
            }
        };
        $transport->onAcknowledgment = static function () use (&$pids): void {
            foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0), 'A receipt requires a stopped and joined callback.'); }
        };
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered): never {
            file_put_contents($entered, 'entered');
            sleep(60);
            throw new RuntimeException('Stopped callback must not resume.');
        });
        $started = microtime(true);
        $worker->tick(0);
        self::assertLessThan(3, microtime(true) - $started);
        self::assertSame($fault === 'status', $statusFailed);
        self::assertCount($fault === 'status' ? 1 : 2, $transport->acknowledgments);
        foreach ($transport->acknowledgments as $request) {
            self::assertSame(['activity_attempt_id' => 'attempt', 'lease_owner' => 'owner', 'request_id' => 'local-request'], $request);
        }
        self::assertContains('worker.activity_cancellation_acknowledged', $transport->events);
        self::assertNotContains('worker.activity_cancellation_acknowledgement_failed', $transport->events);
        self::assertSame(1, $transport->retainedStopReceipts);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->userHeartbeats);
        foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
    }

    public static function receiptBudgetProvider(): array
    {
        return [['expired'], ['invalid'], ['original_deadline'], ['persistent']];
    }

    #[DataProvider('receiptBudgetProvider')]
    public function testJoinedStopReportCannotRestartBudgetOrPublishAfterRefusal(string $fault): void
    {
        $transport = new RemoteOwnerTransport();
        $entered = $this->directory.'/entered';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $transport->persistentTransientFailure = $fault === 'persistent';
        $transport->transientAcknowledgmentFault = in_array($fault, ['persistent', 'original_deadline'], true) ? 'connection' : null;
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($entered, $fault, &$pids): void {
            if (!is_file($entered)) { return; }
            $transport->status['can_continue'] = false;
            $transport->status['cancel_requested'] = true;
            $receipt = RemoteOwnerTransport::receipt();
            if ($fault === 'expired') { $receipt['cleanup_deadline_at'] = '2000-01-01T00:00:00Z'; }
            if ($fault === 'invalid') { $receipt['cleanup_deadline_at'] = '2026-02-30T00:00:00Z'; }
            if ($fault === 'original_deadline' && count($pids) === 2
                && !posix_kill($pids[0], 0) && !posix_kill($pids[1], 0)) {
                $receipt['cleanup_deadline_at'] = (new \DateTimeImmutable('+1 second'))
                    ->modify('+200 milliseconds')->format('Y-m-d\TH:i:s.uP');
            }
            $transport->status['cancellation_acknowledgement'] = $receipt;
        };
        $transport->onAcknowledgment = static function () use (&$pids, $fault): void {
            foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
            if ($fault === 'original_deadline') { usleep(350_000); }
        };
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered): never {
            file_put_contents($entered, 'entered');
            sleep(60);
            throw new RuntimeException('Stopped callback must not resume.');
        });
        $started = microtime(true);
        $worker->tick(0);
        self::assertLessThan(3, microtime(true) - $started);
        self::assertCount(match ($fault) { 'persistent' => 3, 'original_deadline' => 1, default => 0 }, $transport->acknowledgments);
        self::assertSame(0, $transport->retainedStopReceipts);
        self::assertNotContains('worker.activity_cancellation_acknowledged', $transport->events);
        self::assertContains('worker.activity_cancellation_acknowledgement_failed', $transport->events);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->userHeartbeats);
        foreach ($pids as $pid) { self::assertFalse(posix_kill($pid, 0)); }
    }

    public function testUserProgressIsProxiedAndResultTypesSurvive(): void
    {
        $transport = new RemoteOwnerTransport();
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $worker->registerActivity('remote', static function (ActivityContext $context): array {
            $context->heartbeat(['message' => 'authored', 'current' => 1]);
            return ['bytes' => \DurableWorkflow\Codec\AvroBinaryValue::fromBytes("\0\xff"), 'value' => 42, 'null' => null];
        });
        $worker->tick(0);
        self::assertCount(1, $transport->userHeartbeats);
        self::assertSame(['message' => 'authored', 'current' => 1], $transport->userHeartbeats[0]['details']);
        self::assertCount(1, $transport->completions);
        $decoded = (new \DurableWorkflow\Codec\AvroPayloadCodec())->decode($transport->completions[0]['result']['blob']);
        self::assertInstanceOf(\DurableWorkflow\Codec\AvroBinaryValue::class, $decoded['bytes']);
        self::assertSame("\0\xff", $decoded['bytes']->bytes);
        self::assertSame(42, $decoded['value']);
        self::assertNull($decoded['null']);
        self::assertSame([], $transport->failures);
    }

    public function testOwnershipLossDiscardsAResultBeforeEncoding(): void
    {
        $transport = new RemoteOwnerTransport();
        $ready = $this->directory.'/ready';
        $transport->observe = static function (RemoteOwnerTransport $transport) use ($ready): void {
            if (is_file($ready)) { $transport->status['can_continue'] = false; }
        };
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($ready): \stdClass {
            file_put_contents($ready, 'ready');
            return new \stdClass();
        });
        $worker->tick(0);
        self::assertFileExists($ready);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
    }

    public static function failureProvider(): array
    {
        return [['business'], ['cancelled']];
    }

    #[DataProvider('failureProvider')]
    public function testApplicationFailurePreservesItsOriginalClassification(string $kind): void
    {
        $transport = new RemoteOwnerTransport();
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($kind): never {
            throw $kind === 'business' ? new RemoteOwnerBusinessFailure('original') : new ActivityCancelled('authored cancel');
        });
        $worker->tick(0);
        self::assertSame([], $transport->completions);
        self::assertCount(1, $transport->failures);
        self::assertSame($kind === 'business' ? RemoteOwnerBusinessFailure::class : ActivityCancelled::class,
            $transport->failures[0]['failure']['type']);
        self::assertSame($kind === 'cancelled', $transport->failures[0]['failure']['non_retryable']);
    }

    public static function deadlineProvider(): array
    {
        return [['lease_expires_at'], ['heartbeat'], ['start_to_close'], ['schedule_to_close'], ['invalid']];
    }

    #[DataProvider('deadlineProvider')]
    public function testExpiredOrInvalidObservedDeadlinePreventsCallbackStart(string $field): void
    {
        $transport = new RemoteOwnerTransport();
        $expired = '2000-01-01T00:00:00Z';
        if ($field === 'invalid') { $transport->status['lease_expires_at'] = 'tomorrow'; }
        elseif ($field === 'lease_expires_at') { $transport->status[$field] = $expired; }
        else { $transport->status['deadlines'] = [$field => $expired]; }
        $entered = $this->directory.'/entered';
        $pids = [];
        $worker = $this->worker($transport, $pids);
        $worker->registerActivity('remote', static function (ActivityContext $context) use ($entered): string {
            file_put_contents($entered, 'unsafe');
            return 'unsafe';
        });
        $worker->tick(0);
        self::assertFileDoesNotExist($entered);
        self::assertSame([], $pids);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
    }

    private function worker(RemoteOwnerTransport $transport, array &$pids): Worker
    {
        return new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'owner', enableCooperativeCancellation: true,
            diagnosticListener: static function (string $event, array $context) use (&$pids, $transport): void {
                $transport->events[] = $event;
                $transport->diagnostics[] = ['event' => $event, 'context' => $context];
                if ($event === 'worker.activity_process_started') { $pids = [$context['relay_pid'], $context['callback_pid']]; }
                if ($event === 'worker.claim_aborted') { $transport->aborts[] = $context['message']; }
            });
    }
}

final class RemoteOwnerBusinessFailure extends RuntimeException {}

final class RemoteOwnerTransport implements BoundedTransport
{
    public array $status = ['task_id' => 'task', 'activity_attempt_id' => 'attempt', 'lease_owner' => 'owner',
        'can_continue' => true, 'cancel_requested' => false, 'heartbeat_recorded' => false,
        'reason' => null, 'lease_expires_at' => '2100-01-01T00:00:00Z'];
    public array $completions = [];
    public array $failures = [];
    public array $userHeartbeats = [];
    public array $requests = [];
    public array $aborts = [];
    public array $acknowledgments = [];
    public array $events = [];
    public array $diagnostics = [];
    public ?string $acknowledgmentFailure = null;
    public ?string $transientAcknowledgmentFault = null;
    public bool $persistentTransientFailure = false;
    public int $retainedStopReceipts = 0;
    public ?\Closure $onAcknowledgment = null;
    public ?\Closure $observe = null;
    private bool $polled = false;

    public function supportsBoundedRequests(): bool { return true; }

    public static function receipt(): array
    {
        return ['request_id' => 'local-request', 'root_request_id' => 'root-request',
            'cleanup_deadline_at' => '2100-01-01T00:00:00Z', 'cancellation_history_event_id' => 'cancel-history',
            'callback_state' => 'unknown', 'history_event_id' => null, 'acknowledged_at' => null,
            'received_after_deadline' => null];
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $this->requests[] = ['uri' => $uri, 'headers' => $headers, 'body' => $body, 'timeout' => $timeoutSeconds];
        return $this->send($method, $uri, $headers, $body);
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        if (str_ends_with($uri, '/cluster/info')) {
            return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => ['cooperative_cancellation' => true]]];
        }
        if (str_ends_with($uri, '/worker/register')) { return ['registered' => true]; }
        if (str_ends_with($uri, '/activity-tasks/poll')) {
            if ($this->polled) { return ['task' => null]; }
            $this->polled = true;
            return ['task' => ['task_id' => 'task', 'activity_attempt_id' => 'attempt', 'lease_owner' => 'owner',
                'activity_type' => 'remote', 'attempt_number' => 1, 'arguments' => null, 'payload_codec' => 'avro']];
        }
        if (str_ends_with($uri, '/poll')) { return ['task' => null]; }
        if (str_ends_with($uri, '/status')) {
            $this->observe?->__invoke($this);
            return $this->status;
        }
        if (str_ends_with($uri, '/activity-tasks/task/heartbeat')) {
            $this->userHeartbeats[] = $body;
            return array_replace($this->status, ['heartbeat_recorded' => true]);
        }
        if (str_ends_with($uri, '/worker/heartbeat')) { return ['heartbeat_recorded' => true]; }
        if (str_ends_with($uri, '/acknowledge-cancellation')) {
            $this->onAcknowledgment?->__invoke();
            $this->acknowledgments[] = $body;
            if ($this->acknowledgmentFailure === 'transport') { throw new TransportException('Receipt transport unavailable.'); }
            if ((count($this->acknowledgments) === 1 || $this->persistentTransientFailure) && $this->transientAcknowledgmentFault !== null) {
                if ($this->transientAcknowledgmentFault === 'lost_reply') { $this->retainedStopReceipts = 1; }
                throw $this->transientAcknowledgmentFault === 'upstream'
                    ? new TransportException('Temporary upstream unavailable.', status: 503)
                    : new TransportException('Stop receipt connection interrupted.', transientConnectionFailure: true);
            }
            $duplicate = $this->retainedStopReceipts === 1;
            if ($this->acknowledgmentFailure === null) { $this->retainedStopReceipts = 1; }
            return ['task_id' => 'task', 'activity_attempt_id' => 'attempt', 'lease_owner' => 'owner',
                'request_id' => $this->acknowledgmentFailure === 'mismatched' ? 'other' : $body['request_id'],
                'acknowledged' => $this->acknowledgmentFailure !== 'refused', 'duplicate' => $duplicate,
                'reason' => $this->acknowledgmentFailure === 'refused' ? 'stale' : null,
                'history_event_id' => $this->acknowledgmentFailure === 'unproved' ? null : 'stop-history', 'heartbeat_recorded' => false];
        }
        if (str_ends_with($uri, '/complete')) { $this->completions[] = $body; return ['recorded' => true]; }
        if (str_ends_with($uri, '/fail')) { $this->failures[] = $body; return ['recorded' => true]; }
        throw new RuntimeException('Unexpected remote worker request: '.$uri);
    }
}
