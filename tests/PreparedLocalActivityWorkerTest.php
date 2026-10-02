<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityWorkerTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        if (!CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Prepared callback execution requires Unix process control.');
        }
        $this->directory = sys_get_temp_dir().'/dw-prepared-worker-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
    }

    public function test_managed_worker_checkpoints_once_and_executes_more_than_three_admitted_calls(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->stopAuxiliaryPolls = true;
        $worker = $this->worker($transport);
        $calls = $this->directory.'/calls';
        $prefixCalls = 0;
        $worker->registerWorkflow('prepared', static function (WorkflowContext $context) use (&$prefixCalls): int {
            $prefix = $context->sideEffect(static function () use (&$prefixCalls): int { ++$prefixCalls; return 10; });
            $result = 0;
            for ($i = 0; $i < 5; ++$i) { $result += $context->localActivity('effect', [$prefix + $i]); }
            return $result;
        });
        $worker->registerActivity('effect', static function (ActivityContext $context, int $value) use ($calls): int {
            file_put_contents($calls, $context->activityAttemptId.'\n', FILE_APPEND);
            return $value;
        });
        $worker->run(0);
        self::assertSame([], $transport->failures);
        self::assertSame(1, $prefixCalls);
        self::assertCount(1, $transport->operations('checkpoint'));
        self::assertCount(5, $transport->operations('prepare'));
        self::assertCount(5, $transport->operations('outcome'));
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame(60, (new AvroPayloadCodec())->decodeEnvelope($transport->completions[0]['commands'][0]['result']));
        self::assertSame(5, substr_count((string) file_get_contents($calls), 'backend-attempt-'));
        self::assertContains('prepared_local_activities', $transport->registration['capabilities']);
        foreach ($transport->localRequests as $request) {
            self::assertSame('original', $request['body']['lease_owner']);
            self::assertSame(4, $request['body']['workflow_task_attempt']);
        }
        foreach ($transport->historyRequests as $request) {
            self::assertSame('opaque-origin', $request['next_history_page_token']);
            self::assertSame(4, $request['workflow_task_attempt']);
        }
    }

    public function test_real_application_heartbeat_uses_its_separate_endpoint(): void
    {
        $transport = new PreparedWorkerTransport();
        $worker = $this->worker($transport);
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) =>
            $context->localActivity('effect', [], ['heartbeat_timeout' => 8]));
        $worker->registerActivity('effect', static function (ActivityContext $context): string {
            $context->heartbeat(['phase' => 'real application progress']);
            return 'done';
        });
        $worker->tick(0);
        self::assertSame([], $transport->failures);
        self::assertCount(1, $transport->operations('heartbeat'));
        self::assertSame(['phase' => 'real application progress'], $transport->operations('heartbeat')[0]['body']['progress']);
        self::assertGreaterThan(1, count($transport->operations('control')));
        foreach ($transport->operations('control') as $request) {
            self::assertSame(true, $request['body']['renew_lease']);
            self::assertArrayNotHasKey('progress', $request['body']);
        }
    }

    public function test_blocked_callback_stops_without_heartbeats_then_shielded_cleanup_keeps_root_budget(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->cancelAfterFile = $this->directory.'/entered';
        $worker = $this->worker($transport);
        $late = $this->directory.'/late';
        $cleaned = $this->directory.'/cleaned';
        $worker->registerWorkflow('prepared', static function (WorkflowContext $context): void {
            try { $context->localActivity('effect'); }
            catch (WorkflowCancelled $cancelled) {
                $context->cancellationShield(static fn () => $context->localActivity('cleanup'));
                throw $cancelled;
            }
        });
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($transport, $late): string {
            file_put_contents($transport->cancelAfterFile, (string) getmypid());
            sleep(60);
            file_put_contents($late, 'stale side effect');
            return 'stale';
        });
        $worker->registerActivity('cleanup', static function (ActivityContext $context) use ($cleaned): string {
            file_put_contents($cleaned, $context->activityAttemptId);
            return 'cleaned';
        });
        $started = hrtime(true) / 1e9;
        $worker->tick(0);
        self::assertLessThan(3, hrtime(true) / 1e9 - $started);
        self::assertSame([], $transport->failures);
        self::assertFileDoesNotExist($late);
        self::assertFileExists($cleaned);
        self::assertCount(0, $transport->operations('heartbeat'));
        self::assertCount(1, $transport->operations('acknowledge-cancellation'));
        self::assertFalse($transport->callbackAliveAtStopAcknowledgment);
        self::assertCount(1, $transport->operations('outcome'), 'The original cancelled callback published an outcome.');
        self::assertSame(['fail_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame(WorkflowCancelled::class, $transport->completions[0]['commands'][0]['exception_type']);
        $cleanup = $transport->operations('prepare')[1]['body']['descriptor']['cancellation_cleanup'];
        self::assertSame(['request_id' => 'root-request', 'delivery_history_event_id' => 'delivery'], $cleanup);
        self::assertSame('2026-10-02T00:00:30.000000Z', $transport->attempt['cancellation_cleanup']['cleanup_deadline_at']);
    }

    public function test_refused_admission_never_starts_a_callback(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->malformedAdmission = true;
        $worker = $this->worker($transport);
        $entered = $this->directory.'/entered';
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered): void { file_put_contents($entered, 'unsafe'); });
        $worker->tick(0);
        self::assertFileDoesNotExist($entered);
        self::assertSame([], $transport->completions);
        self::assertCount(0, $transport->operations('outcome'));
    }

    public function test_retry_is_durably_scheduled_without_inline_callback_or_backoff(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->retryOutcome = true;
        $worker = $this->worker($transport);
        $calls = $this->directory.'/calls';
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) => $context->localActivity('effect', [], [
            'retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [60]],
        ]));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($calls): never {
            file_put_contents($calls, 'once', FILE_APPEND);
            throw new \RuntimeException('retry me');
        });
        $started = hrtime(true) / 1e9;
        $worker->tick(0);
        self::assertLessThan(2, hrtime(true) / 1e9 - $started);
        self::assertSame('once', file_get_contents($calls));
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertCount(1, $transport->operations('prepare'));
        self::assertCount(1, $transport->operations('outcome'));
    }

    public function test_lost_outcome_acknowledgment_replays_history_without_reexecuting_callback(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->loseOutcomeAck = true;
        $worker = $this->worker($transport);
        $calls = $this->directory.'/calls';
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($calls): string {
            file_put_contents($calls, 'once', FILE_APPEND);
            return 'durable';
        });
        $worker->tick(0);
        self::assertSame([], $transport->completions);
        $transport->epoch = 5;
        $worker->tick(0);
        self::assertSame('once', file_get_contents($calls));
        self::assertCount(1, $transport->operations('prepare'));
        self::assertCount(1, $transport->operations('outcome'));
        self::assertSame('durable', (new AvroPayloadCodec())->decodeEnvelope($transport->completions[0]['commands'][0]['result']));
    }

    public function test_unresolved_started_attempt_requests_recovery_without_starting_application_code(): void
    {
        $transport = new PreparedWorkerTransport();
        $transport->event('ActivityScheduled', ['sequence' => 1, 'activity_type' => 'effect', 'execution_mode' => 'local']);
        $transport->event('ActivityStarted', ['sequence' => 1, 'activity_execution_id' => 'old-execution', 'activity_attempt_id' => 'old-attempt']);
        $worker = $this->worker($transport);
        $entered = $this->directory.'/entered';
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered): void { file_put_contents($entered, 'unsafe'); });
        $worker->tick(0);
        self::assertFileDoesNotExist($entered);
        self::assertCount(1, $transport->operations('recover'));
        self::assertCount(0, $transport->operations('prepare'));
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->completions);
    }

    public function test_authority_budget_never_rounds_past_the_original_deadline(): void
    {
        $transport = new PreparedWorkerTransport();
        $client = (new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
        $client->preparedLocalActivityOperation('task', 'original', 4, 'control', [], 'attempt', new RequestBudget(5, hrtime(true) / 1e9 + 1.9));
        self::assertSame(1, $transport->timeouts[array_key_last($transport->timeouts)]);
        $before = count($transport->localRequests);
        try {
            $client->preparedLocalActivityOperation('task', 'original', 4, 'control', [], 'attempt', new RequestBudget(5, hrtime(true) / 1e9 + 0.9));
            self::fail('A whole-second transport cannot begin within a subsecond authority budget.');
        } catch (\DurableWorkflow\Exception\ServerException) {
            self::assertCount($before, $transport->localRequests);
        }
    }

    public function test_prepared_mode_requires_cooperation_and_explicit_backend_discovery(): void
    {
        $transport = new PreparedWorkerTransport();
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        try {
            new Worker($client, 'queue', enablePreparedLocalActivities: true);
            self::fail('Prepared execution cannot bypass cooperative ownership.');
        } catch (\InvalidArgumentException $error) {
            self::assertStringContainsString('cooperative', $error->getMessage());
        }
        $transport->serverPreparedCapability = false;
        try {
            $this->worker($transport)->run(0);
            self::fail('Prepared capability must not be advertised without the installed backend.');
        } catch (\DurableWorkflow\Worker\WorkflowClaimAborted $error) {
            self::assertStringContainsString('prepared_local_activity_not_supported', $error->getMessage());
        }
        self::assertSame([], $transport->registration);
        self::assertSame([], $transport->localRequests);
    }

    public function test_fixed_server_deadline_stops_a_blocked_callback_despite_repeated_live_controls(): void
    {
        $transport = new PreparedWorkerTransport();
        $worker = $this->worker($transport);
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $worker->registerWorkflow('prepared', static fn (WorkflowContext $context) => $context->localActivity('effect', [], ['start_to_close_timeout' => 2]));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, (string) getmypid());
            sleep(60);
            file_put_contents($late, 'too late');
            return 'late';
        });
        $started = hrtime(true) / 1e9;
        $worker->tick(0);
        self::assertLessThan(2, hrtime(true) / 1e9 - $started);
        self::assertFileExists($entered);
        self::assertFalse(posix_kill((int) file_get_contents($entered), 0));
        self::assertFileDoesNotExist($late);
        self::assertCount(0, $transport->operations('outcome'));
        self::assertCount(0, $transport->operations('acknowledge-cancellation'));
        self::assertSame([], $transport->completions);
    }

    private function worker(PreparedWorkerTransport $transport): Worker
    {
        return new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'original', clock: static fn (): float => 0,
            enableCooperativeCancellation: true, enablePreparedLocalActivities: true);
    }
}

/** Protocol fixture with real callbacks. It does not simulate a physical callback stop. */
final class PreparedWorkerTransport implements BoundedTransport
{
    public array $history = [['event_type' => 'WorkflowStarted', 'payload' => []]];
    public array $localRequests = [];
    public array $historyRequests = [];
    public array $completions = [];
    public array $failures = [];
    public array $registration = [];
    public array $timeouts = [];
    public array $attempt = [];
    public int $epoch = 4;
    public bool $malformedAdmission = false;
    public bool $retryOutcome = false;
    public bool $loseOutcomeAck = false;
    public bool $stopAuxiliaryPolls = false;
    public bool $serverPreparedCapability = true;
    public ?string $cancelAfterFile = null;
    public ?bool $callbackAliveAtStopAcknowledgment = null;
    private ?array $cancellation = null;
    private int $sequence = 0;
    private int $attemptCount = 0;

    public function supportsBoundedRequests(): bool { return true; }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $this->timeouts[] = $timeoutSeconds;
        return $this->send($method, $uri, $headers, $body);
    }

    public function operations(string $operation): array
    {
        return array_values(array_filter($this->localRequests, static fn (array $request): bool => str_ends_with($request['uri'], '/'.$operation)));
    }

    public function event(string $type, array $payload, ?string $id = null): void
    {
        $this->history[] = ['id' => $id ?? 'event-'.count($this->history), 'event_type' => $type,
            'recorded_at' => '2026-10-02T00:00:00.000000Z', 'payload' => $payload];
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        if (str_ends_with($uri, '/cluster/info')) {
            return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => [
                'cooperative_cancellation' => true, 'prepared_local_activities' => $this->serverPreparedCapability]]];
        }
        if (str_ends_with($uri, '/worker/register')) { $this->registration = $body; return ['registered' => true]; }
        if ($method === 'DELETE' && str_ends_with($uri, '/worker/registrations/original')) {
            return ['worker_id' => 'original', 'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
        }
        if (str_ends_with($uri, '/workflow-tasks/poll')) {
            return ['task' => ['task_id' => 'task', 'workflow_id' => 'workflow', 'run_id' => 'run',
                'workflow_type' => 'prepared', 'lease_owner' => 'original', 'workflow_task_attempt' => $this->epoch,
                'payload_codec' => 'avro', 'history_events' => $this->history], 'poll_status' => 'leased'];
        }
        if (str_ends_with($uri, '/poll')) {
            return $this->stopAuxiliaryPolls ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                : ['task' => null, 'poll_status' => 'empty'];
        }
        if (str_ends_with($uri, '/history')) {
            $this->historyRequests[] = $body;
            return ['history_events' => $this->history, 'next_history_page_token' => null];
        }
        if (str_ends_with($uri, '/deliver-cancellation')) {
            $this->event('CooperativeCancellationDelivered', ['workflow_run_id' => 'run', 'workflow_command_id' => 'root-request',
                'cancellation' => $this->cancellation, 'sequence' => $body['sequence'], 'call_kind' => $body['call_kind'],
                'sequence_span' => $body['sequence_span']], 'delivery');
            return ['delivered' => true, 'task_id' => 'task', 'request_id' => 'root-request', 'sequence' => $body['sequence'],
                'call_kind' => $body['call_kind'], 'sequence_span' => $body['sequence_span'],
                'operation_sequence' => null, 'operation_sequence_span' => 1];
        }
        if (str_contains($uri, '/local-activities/')) {
            $this->localRequests[] = compact('uri', 'body');
            $cursor = ['history_refresh_page_token' => 'opaque-origin'];
            if (str_ends_with($uri, '/checkpoint')) {
                foreach ($body['commands'] as $offset => $command) {
                    $this->event('SideEffectRecorded', ['sequence' => $body['start_sequence'] + $offset, 'result' => $command['result']]);
                }
                return ['checkpointed' => true, 'duplicate' => false, 'reason' => null, 'task_id' => 'task',
                    'workflow_run_id' => 'run', 'lease_owner' => 'original', 'workflow_task_attempt' => $this->epoch,
                    'checkpoint_id' => $body['checkpoint_id'], 'start_sequence' => $body['start_sequence'],
                    'next_sequence' => $body['start_sequence'] + count($body['commands']), ...$cursor];
            }
            if (str_ends_with($uri, '/recover')) {
                return ['recovered' => true, 'duplicate' => false, 'reason' => null, 'workflow_task_id' => 'task',
                    'activity_execution_id' => 'old-execution', 'activity_attempt_id' => 'old-attempt',
                    'callback_stop_state' => 'unknown', 'event_id' => 'recovery', 'event_type' => 'ActivityRetryScheduled',
                    'claim_released' => true, 'created_task_ids' => ['retry'], ...$cursor];
            }
            if (str_ends_with($uri, '/prepare')) {
                $descriptor = $body['descriptor'];
                $this->sequence = $body['sequence'];
                ++$this->attemptCount;
                $cleanup = isset($descriptor['cancellation_cleanup']) ? [
                    ...$descriptor['cancellation_cleanup'], 'root_request_id' => 'root-request',
                    'cleanup_deadline_at' => '2026-10-02T00:00:30.000000Z',
                ] : null;
                $this->attempt = ['prepared' => true, 'duplicate' => false, 'reason' => null,
                    'workflow_task_id' => 'task', 'workflow_task_attempt' => $this->epoch, 'lease_owner' => 'original',
                    'worker_attempt_id' => $body['worker_attempt_id'], 'attempt_number' => 1,
                    'activity_execution_id' => 'backend-execution-'.$this->attemptCount,
                    'activity_attempt_id' => 'backend-attempt-'.$this->attemptCount,
                    'server_time' => '2026-10-02T00:00:00.000000Z', 'lease_expires_at' => '2026-10-02T00:00:10.000000Z',
                    'start_to_close_deadline_at' => isset($descriptor['start_to_close_timeout'])
                        ? '2026-10-02T00:00:02.000000Z' : ($cleanup['cleanup_deadline_at'] ?? null),
                    'schedule_to_close_deadline_at' => $cleanup['cleanup_deadline_at'] ?? null,
                    'heartbeat_deadline_at' => isset($descriptor['heartbeat_timeout']) ? '2026-10-02T00:00:08.000000Z' : ($cleanup['cleanup_deadline_at'] ?? null),
                    'cancellation_cleanup' => $cleanup];
                $this->event('ActivityScheduled', ['sequence' => $this->sequence, 'activity_type' => $descriptor['activity_type'], 'execution_mode' => 'local']);
                $this->event('ActivityStarted', ['sequence' => $this->sequence, 'activity_execution_id' => $this->attempt['activity_execution_id'],
                    'activity_attempt_id' => $this->attempt['activity_attempt_id']]);
                return [...$this->attempt, ...$cursor, ...($this->malformedAdmission ? ['workflow_task_attempt' => 99] : [])];
            }
            if (str_ends_with($uri, '/acknowledge-cancellation')) {
                $this->callbackAliveAtStopAcknowledgment = posix_kill((int) file_get_contents($this->cancelAfterFile), 0);
                return ['acknowledged' => true, 'duplicate' => false, 'reason' => null, 'history_event_id' => 'joined-stop', ...$cursor];
            }
            if (str_ends_with($uri, '/outcome')) {
                $kind = $this->retryOutcome ? 'ActivityRetryScheduled' : 'ActivityCompleted';
                $this->event($kind, ['sequence' => $this->sequence, 'result' => $body['report']['result'] ?? null], 'outcome-'.$this->attemptCount);
                if ($this->loseOutcomeAck) { $this->loseOutcomeAck = false; throw new TransportException('Lost committed outcome acknowledgment.'); }
                return [...$this->attempt, 'recorded' => true, 'workflow_run_id' => 'run', 'event_id' => 'outcome-'.$this->attemptCount,
                    'event_type' => $kind, 'recorded_at' => '2026-10-02T00:00:00.000000Z',
                    'claim_released' => $this->retryOutcome, 'created_task_ids' => $this->retryOutcome ? ['retry'] : [], ...$cursor];
            }
            $reply = [...$this->attempt, 'active' => true, 'renewed' => $body['renew_lease'] ?? false, 'stop_required' => false,
                'workflow_lease_expires_at' => '2026-10-02T00:00:10.000000Z',
                'heartbeat_recorded' => str_ends_with($uri, '/heartbeat'),
                'heartbeat_history_event_id' => str_ends_with($uri, '/heartbeat') ? 'heartbeat-event' : null, ...$cursor];
            if ($this->cancelAfterFile !== null && is_file($this->cancelAfterFile) && $this->attempt['cancellation_cleanup'] === null) {
                $this->cancellation ??= ['schema' => 'durable-workflow.cancellation-context/v1',
                    'request_id' => 'root-request', 'root_request_id' => 'root-request',
                    'root_workflow_instance_id' => 'workflow', 'root_workflow_run_id' => 'run', 'parent_request_id' => null,
                    'reason' => 'operator request', 'requester' => ['type' => 'test'], 'source' => 'control_plane',
                    'requested_at' => '2026-10-02T00:00:00.000000Z', 'cleanup_deadline_at' => '2026-10-02T00:00:30.000000Z',
                    'lineage' => [['request_id' => 'root-request', 'workflow_instance_id' => 'workflow', 'workflow_run_id' => 'run']]];
                if (!in_array('CooperativeCancellationRequested', array_column($this->history, 'event_type'), true)) {
                    $this->event('CooperativeCancellationRequested', ['workflow_run_id' => 'run', 'workflow_command_id' => 'root-request',
                        'cleanup_deadline_at' => $this->cancellation['cleanup_deadline_at'], 'reason' => 'operator request',
                        'cancellation' => $this->cancellation]);
                    $this->event('ActivityCancelled', ['sequence' => $this->sequence]);
                }
                return [...$reply, 'active' => false, 'renewed' => false, 'stop_required' => true,
                    'heartbeat_recorded' => false, 'heartbeat_history_event_id' => null,
                    'reason' => 'cancellation_requested', 'cancellation_request' => $this->cancellation,
                    'fenced' => true, 'cancellation_history_event_id' => 'activity-cancelled'];
            }
            return $reply;
        }
        if (str_ends_with($uri, '/complete')) { $this->completions[] = $body; return ['completed' => true]; }
        if (str_ends_with($uri, '/fail')) { $this->failures[] = $body; return ['failed' => true]; }
        if (str_ends_with($uri, '/heartbeat')) {
            return ['task_id' => 'task', 'lease_owner' => 'original', 'workflow_task_attempt' => $this->epoch, 'renewed' => true];
        }
        throw new \RuntimeException('Unexpected prepared worker fixture request '.$uri);
    }
}
