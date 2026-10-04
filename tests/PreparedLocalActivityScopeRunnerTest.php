<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\PreparedLocalActivityAttempt;
use DurableWorkflow\Worker\PreparedLocalActivityRunner;
use DurableWorkflow\Worker\ScopedActivityCancellationObserved;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityScopeRunnerTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        if (!CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Prepared callback execution requires Unix process control.');
        }
        $this->directory = sys_get_temp_dir().'/dw-prepared-scope-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
    }

    #[DataProvider('stopBoundaryProvider')]
    public function test_valid_scope_stop_joins_only_its_member_and_commits_the_survivor(string $boundary): void
    {
        $transport = new ScopedPreparedTransport($this->directory, $boundary);
        $members = $this->members($transport);
        $started = hrtime(true) / 1e9;
        try {
            PreparedLocalActivityRunner::executeGroup($members);
            self::fail('The original scoped observation must follow survivor settlement.');
        } catch (ScopedActivityCancellationObserved $error) {
            self::assertSame('inner-request', $error->request->requestId);
            self::assertSame('root-request', $error->request->rootContext->rootRequestId);
            self::assertSame('inner', $error->request->scopeId);
            self::assertSame('2026-10-04T00:00:20.123456Z', $error->request->deadline()->format('Y-m-d\TH:i:s.u\Z'));
            self::assertSame('canonical-scope-cursor', $error->historyRefreshPageToken);
        }
        self::assertLessThan(5, hrtime(true) / 1e9 - $started);
        self::assertSame([], $transport->rootObservations);
        self::assertSame([['member' => 'target', 'request_id' => 'inner-request',
            'callback_alive' => false, 'relay_alive' => false]], $transport->stops);
        if ($boundary !== 'before-fork') {
            self::assertTrue($transport->survivorAliveAtStop);
        } else {
            self::assertArrayNotHasKey('target', $transport->pids);
            self::assertFileDoesNotExist($this->directory.'/target');
        }
        self::assertSame(['survivor'], array_column($transport->outcomes, 'member'));
        self::assertSame('survived', (new AvroPayloadCodec())->decodeEnvelope($transport->outcomes[0]['report']['result']));
        self::assertGreaterThan(0, $transport->survivorRenewalsAfterStop);
        self::assertFileDoesNotExist($this->directory.'/late');
        self::assertSame($boundary === 'heartbeat' ? 1 : 0, count($transport->operations('heartbeat')));
        foreach ($transport->requests as $request) {
            self::assertSame('original', $request['body']['lease_owner']);
            self::assertSame(4, $request['body']['workflow_task_attempt']);
            self::assertSame('tenant', $request['headers']['X-Namespace']);
            self::assertLessThanOrEqual(5, $request['timeout']);
        }
        $this->assertJoined($transport);
    }

    public static function stopBoundaryProvider(): array
    {
        return [['before-fork'], ['running'], ['heartbeat'], ['publication']];
    }

    public function test_single_callback_fenced_before_fork_acknowledges_without_running_application_code(): void
    {
        $transport = new ScopedPreparedTransport($this->directory, 'before-fork');
        $members = $this->members($transport);
        try {
            $members[0]['runner']->execute($members[0]['callback']);
            self::fail('A fenced single callback must not execute.');
        } catch (ScopedActivityCancellationObserved) {
            self::assertCount(1, $transport->stops);
        }
        self::assertSame([], $transport->pids);
        self::assertSame([], $transport->outcomes);
        self::assertSame([], $transport->rootObservations);
        self::assertFileDoesNotExist($this->directory.'/target');
    }

    public function test_unstarted_stop_reobservation_keeps_its_original_identity_without_duplicate_acknowledgment(): void
    {
        $transport = new ScopedPreparedTransport($this->directory, 'before-fork');
        $runner = $this->members($transport)[0]['runner'];
        $runner->acknowledgeUnstartedCancellation();
        $runner->acknowledgeUnstartedCancellation();
        self::assertCount(1, $transport->stops);
        self::assertSame('inner-request', $transport->stops[0]['request_id']);
        self::assertSame([], $transport->pids);
        self::assertSame([], $transport->rootObservations);
    }

    #[DataProvider('unsafeStopProvider')]
    public function test_untrustworthy_scope_or_stop_receipt_joins_all_callbacks_without_publication(bool $malformedFence, bool $failedAck): void
    {
        $transport = new ScopedPreparedTransport($this->directory, 'running');
        $transport->malformedFence = $malformedFence;
        $transport->failedAck = $failedAck;
        try {
            PreparedLocalActivityRunner::executeGroup($this->members($transport));
            self::fail('Untrustworthy control must abort the original hosting claim.');
        } catch (WorkflowClaimAborted $error) {
            self::assertNotInstanceOf(ScopedActivityCancellationObserved::class, $error);
        }
        self::assertCount($failedAck ? 1 : 0, $transport->operations('acknowledge-cancellation'));
        self::assertSame([], $transport->stops);
        self::assertSame([], $transport->outcomes);
        self::assertSame([], $transport->rootObservations);
        self::assertFileDoesNotExist($this->directory.'/late');
        $this->assertJoined($transport);
    }

    public static function unsafeStopProvider(): array
    {
        return [[true, false], [false, true]];
    }

    private function members(ScopedPreparedTransport $transport): array
    {
        $client = (new Client('https://server.example', namespace: 'tenant', transport: $transport,
            workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
        $members = [];
        foreach (['target' => 'inner', 'survivor' => 'sibling'] as $member => $scope) {
            $admission = $transport->admission($member);
            $attempt = PreparedLocalActivityAttempt::fromPreparation($admission, 'task', 'root-run', 'original', 4,
                'nonce-'.$member, cancellationScopeId: $scope);
            $runner = new PreparedLocalActivityRunner($client, $attempt, $admission, hrtime(true) / 1e9,
                static fn (): bool => false,
                static function (array $observation) use ($transport): void { $transport->rootObservations[] = $observation; },
                static function (int $relay, int $callback) use ($transport, $member): void {
                    $transport->pids[$member] = compact('relay', 'callback');
                },
                static function (RequestBudget $budget): void { $budget->remainingSeconds(); });
            $directory = $this->directory;
            $boundary = $transport->boundary;
            $callback = static function (Closure $heartbeat) use ($directory, $member, $boundary): string {
                file_put_contents($directory.'/'.$member, (string) getmypid());
                $until = hrtime(true) / 1e9 + 6;
                if ($member === 'target') {
                    if (in_array($boundary, ['heartbeat', 'publication'], true)) {
                        while (!is_file($directory.'/survivor') && hrtime(true) / 1e9 < $until) { usleep(10000); }
                        if ($boundary === 'heartbeat') { $heartbeat(['phase' => 'application heartbeat']); }
                        return 'stale target result';
                    }
                    sleep(30);
                    file_put_contents($directory.'/late', 'unfenced side effect');
                    return 'late';
                }
                while (!is_file($directory.'/ack') && hrtime(true) / 1e9 < $until) { usleep(10000); }
                if (!is_file($directory.'/ack')) { throw new \RuntimeException('No canonical target stop receipt.'); }
                // Stay alive long enough for supervision to renew after the
                // scoped member has stopped, without application heartbeats.
                usleep(1100000);
                return 'survived';
            };
            $members[] = ['runner' => $runner, 'callback' => $callback];
        }
        return $members;
    }

    private function assertJoined(ScopedPreparedTransport $transport): void
    {
        foreach ($transport->pids as $pids) {
            self::assertFalse(posix_kill($pids['callback'], 0));
            self::assertFalse(posix_kill($pids['relay'], 0));
        }
    }
}

/** Native-shaped control receipts around real supervised callback processes. */
final class ScopedPreparedTransport implements BoundedTransport
{
    public array $requests = [];
    public array $rootObservations = [];
    public array $pids = [];
    public array $stops = [];
    public array $outcomes = [];
    public bool $survivorAliveAtStop = false;
    public int $survivorRenewalsAfterStop = 0;
    public bool $malformedFence = false;
    public bool $failedAck = false;
    private readonly float $clockOrigin;

    public function __construct(private readonly string $directory, public readonly string $boundary)
    {
        $this->clockOrigin = hrtime(true) / 1e9;
    }

    public function supportsBoundedRequests(): bool { return true; }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $this->requests[] = ['uri' => $uri, 'headers' => $headers, 'body' => $body, 'timeout' => $timeoutSeconds];
        return $this->send($method, $uri, $headers, $body);
    }

    public function operations(string $operation): array
    {
        return array_values(array_filter($this->requests, static fn (array $request): bool => str_ends_with($request['uri'], '/'.$operation)));
    }

    public function admission(string $member): array
    {
        return ['prepared' => true, 'duplicate' => false, 'reason' => null,
            'workflow_task_id' => 'task', 'workflow_task_attempt' => 4, 'lease_owner' => 'original',
            'activity_execution_id' => 'execution-'.$member, 'activity_attempt_id' => 'attempt-'.$member,
            'worker_attempt_id' => 'nonce-'.$member, 'attempt_number' => 1,
            'server_time' => $this->time(), 'lease_expires_at' => $this->time(10),
            'workflow_lease_expires_at' => $this->time(10),
            'start_to_close_deadline_at' => '2026-10-04T00:00:30.123456Z',
            'schedule_to_close_deadline_at' => '2026-10-04T00:01:00.123456Z', 'heartbeat_deadline_at' => null];
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        if (!preg_match('#/local-activities/attempt-(target|survivor)/(control|heartbeat|outcome|acknowledge-cancellation)$#', $uri, $match)) {
            throw new \RuntimeException('Unexpected scoped prepared fixture request.');
        }
        [, $member, $operation] = $match;
        $reply = $this->admission($member);
        if ($operation === 'acknowledge-cancellation') {
            if ($this->failedAck) { return ['acknowledged' => false, 'duplicate' => false, 'reason' => 'lost_receipt']; }
            $pids = $this->pids[$member] ?? [];
            $this->stops[] = ['member' => $member, 'request_id' => $body['request_id'],
                'callback_alive' => isset($pids['callback']) && posix_kill($pids['callback'], 0),
                'relay_alive' => isset($pids['relay']) && posix_kill($pids['relay'], 0)];
            $this->survivorAliveAtStop = isset($this->pids['survivor']) && posix_kill($this->pids['survivor']['callback'], 0);
            file_put_contents($this->directory.'/ack', $body['request_id']);
            return ['acknowledged' => true, 'duplicate' => false, 'reason' => null, 'history_event_id' => 'canonical-joined-stop'];
        }
        if ($operation === 'outcome') {
            $this->outcomes[] = ['member' => $member, 'report' => $body['report']];
            return [...$reply, 'recorded' => true, 'workflow_run_id' => 'root-run', 'event_id' => 'canonical-'.$member.'-outcome',
                'event_type' => 'ActivityCompleted', 'recorded_at' => $this->time(), 'claim_released' => false, 'created_task_ids' => []];
        }
        $reply = [...$reply, 'active' => true, 'renewed' => $operation === 'control', 'stop_required' => false,
            'heartbeat_recorded' => false, 'heartbeat_history_event_id' => null];
        $running = is_file($this->directory.'/target') && is_file($this->directory.'/survivor');
        $cancel = $member === 'target' && match ($this->boundary) {
            'before-fork' => true,
            'running' => $running,
            'heartbeat' => $operation === 'heartbeat',
            'publication' => $running && !posix_kill($this->pids['target']['callback'], 0),
            default => false,
        };
        if ($cancel) {
            $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/scoped-run-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
            return [...$reply, 'active' => false, 'renewed' => false, 'stop_required' => true,
                'reason' => 'cancellation_scope_requested', 'fenced' => true,
                'cancellation_history_event_id' => 'canonical-target-fence', 'history_refresh_page_token' => 'canonical-scope-cursor',
                'cancellation_scope' => ['schema' => 'durable-workflow.activity-scope-cancellation/v1',
                    'workflow_run_id' => 'root-run', 'scope_id' => $this->malformedFence ? 'sibling' : 'inner',
                    'request_id' => 'inner-request', 'request_history_event_id' => 'canonical-inner-request',
                    'cancellation' => $fixture['child']['scope_origin'], 'authority_deadline_at' => '2026-10-04T00:00:15.123456Z']];
        }
        if ($member === 'survivor' && is_file($this->directory.'/ack')) { ++$this->survivorRenewalsAfterStop; }
        return $reply;
    }

    private function time(float $offset = 0): string
    {
        $epoch = (float) (new DateTimeImmutable('2026-10-04T00:00:00.123456Z'))->format('U.u');
        return DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', $epoch + hrtime(true) / 1e9 - $this->clockOrigin + $offset))
            ->format('Y-m-d\TH:i:s.u\Z');
    }
}
