<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\PreparedLocalActivityAttempt;
use DurableWorkflow\Worker\PreparedLocalActivityCall;
use DurableWorkflow\Worker\PreparedLocalActivityRunner;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityScopeCleanupTest extends TestCase
{
    #[DataProvider('historyStages')]
    public function test_cold_replay_derives_cleanup_proof_from_native_delivery_without_running_a_callback(string $stage, bool $recover): void
    {
        $fixture = self::fixture();
        $result = self::replay(self::history($fixture, $stage));
        $call = $result->preparedLocalActivity;
        self::assertNotNull($call);
        self::assertSame([], $result->commands);
        self::assertSame(4, $call->sequence);
        self::assertSame($recover, $call->recover);
        // Native's bridge receives the Server-normalized Avro blob. The SDK
        // sends the matching portable envelope through the Server API.
        $expected = $fixture['descriptor'];
        $expected['arguments'] = ['codec' => 'avro', 'blob' => $expected['arguments']];
        self::assertEquals($expected, $call->descriptor(new AvroPayloadCodec()));
        self::assertSame($fixture['original_admission']['cancellation_cleanup'], $call->cleanupSnapshot());
        self::assertCount(3, $call->descriptor(new AvroPayloadCodec())['cancellation_cleanup']);
        self::assertSame('2026-10-05T00:00:30.123456Z', $call->cleanupSnapshot()['cleanup_deadline_at']);
        self::assertSame('2026-10-05T00:00:15.123456Z', $call->cleanupSnapshot()['authority_deadline_at']);
    }

    public static function historyStages(): array
    {
        return [['before', false], ['started', true], ['retry', false], ['replacement_started', true]];
    }

    public function test_completed_native_cleanup_replays_once_and_preserves_parent_state_and_original_clock(): void
    {
        $fixture = self::fixture();
        $first = self::replay($fixture['history']);
        $replacement = self::replay($fixture['history']);
        self::assertNull($first->preparedLocalActivity);
        self::assertSame($first->commands, $replacement->commands);
        self::assertSame(['complete_workflow'], array_column($first->commands, 'type'));
        self::assertSame(['result' => 'cleaned', 'remaining' => 15.323456],
            (new AvroPayloadCodec())->decodeEnvelope($first->commands[0]['result']));
        self::assertFalse($fixture['stale_outcome']['recorded']);
        self::assertSame('unknown', $fixture['recovery']['callback_stop_state']);
    }

    #[DataProvider('admissionStages')]
    public function test_native_original_replacement_and_duplicate_admissions_keep_one_cleanup_budget(string $stage): void
    {
        $fixture = self::fixture();
        $admission = $fixture[$stage];
        $attempt = self::admit($admission);
        self::assertSame($fixture['scope_id'], $attempt->cancellationScopeId);
        self::assertSame($admission['activity_attempt_id'], $attempt->attemptId);
        self::assertSame($admission['workflow_task_attempt'], $attempt->workflowTaskAttempt);
        if ($stage !== 'original_admission') {
            $attempt->validateControl($fixture['late_control'], true);
            $attempt->validateOutcome($fixture['outcome']);
            self::assertNotSame($fixture['original_admission']['activity_attempt_id'], $attempt->attemptId);
        }
    }

    public static function admissionStages(): array
    {
        return [['original_admission'], ['replacement_admission'], ['late_duplicate_admission']];
    }

    #[DataProvider('receiptMutations')]
    public function test_altered_scoped_cleanup_receipt_cannot_admit_or_renew_a_callback(string $operation, string $field): void
    {
        $fixture = self::fixture();
        $reply = $fixture[$operation === 'admission' ? 'late_duplicate_admission' : 'late_control'];
        if ($field === 'missing proof') {
            unset($reply['cancellation_cleanup']);
        } elseif ($field === 'missing field') {
            unset($reply['cancellation_cleanup']['preparation_history_event_id']);
        } elseif ($field === 'scope membership') {
            $reply['cancellation_scope_id'] = 'unaffected-sibling';
        } elseif ($field === 'extra authority') {
            $reply['cancellation_cleanup']['lease_owner'] = 'replacement';
        } else {
            $reply['cancellation_cleanup'][$field] = str_ends_with($field, '_at')
                ? '2026-10-05T00:00:31.123456Z' : 'rebound-identity';
        }
        $attempt = self::admit($fixture['late_duplicate_admission']);
        $this->expectException(InvalidArgumentException::class);
        if ($operation === 'admission') {
            self::admit($reply);
        } else {
            $attempt->validateControl($reply, true);
        }
    }

    public static function receiptMutations(): array
    {
        $cases = [];
        foreach (['admission', 'control'] as $operation) {
            foreach (['scope_id', 'operation_scope_id', 'request_id', 'root_request_id',
                'delivery_history_event_id', 'preparation_history_event_id', 'cleanup_deadline_at',
                'authority_deadline_at', 'missing proof', 'missing field', 'scope membership', 'extra authority'] as $field) {
                $cases[$operation.' '.$field] = [$operation, $field];
            }
        }
        return $cases;
    }

    #[DataProvider('deadlineMutations')]
    public function test_immutable_request_deadline_cannot_replace_an_earlier_captured_execution_ceiling(string $operation, string $field): void
    {
        $fixture = self::fixture();
        $reply = $fixture[$operation === 'admission' ? 'late_duplicate_admission' : 'late_control'];
        $reply[$field] = '2026-10-05T00:00:30.123456Z';
        $attempt = self::admit($fixture['late_duplicate_admission']);
        $this->expectException(InvalidArgumentException::class);
        if ($operation === 'admission') {
            self::admit($reply);
        } else {
            $attempt->validateControl($reply, true);
        }
    }

    public static function deadlineMutations(): array
    {
        $cases = [];
        foreach (['admission', 'control'] as $operation) {
            foreach (['lease_expires_at', 'start_to_close_deadline_at', 'schedule_to_close_deadline_at', 'heartbeat_deadline_at'] as $field) {
                $cases[$operation.' '.$field] = [$operation, $field];
            }
        }
        return $cases;
    }

    public function test_application_heartbeat_cannot_extend_a_scoped_cleanup_ceiling(): void
    {
        $fixture = self::fixture();
        $admission = $fixture['replacement_admission'];
        $admission['heartbeat_deadline_at'] = '2026-10-05T00:00:11.000000Z';
        $attempt = self::admit($admission, 8);
        $reply = [...$fixture['late_control'], 'server_time' => '2026-10-05T00:00:10.000000Z',
            'renewed' => false, 'heartbeat_recorded' => true, 'heartbeat_history_event_id' => 'canonical-heartbeat'];
        $attempt->validateHeartbeat($reply);
        self::addToAssertionCount(1);
        $this->expectException(InvalidArgumentException::class);
        $attempt->validateHeartbeat([...$reply, 'heartbeat_deadline_at' => '2026-10-05T00:00:18.000000Z']);
    }

    #[DataProvider('invalidProofAuthoring')]
    public function test_cleanup_factory_cannot_borrow_another_scope_or_precede_delivery(string $scope, int $sequence): void
    {
        $fixture = self::fixture();
        $membership = $scope === 'delivered' ? $fixture['scope_id'] : ($scope === 'ancestor' ? $fixture['outer_scope_id'] : 'root');
        $command = WorkflowCommand::localActivity('tests.scoped-cleanup', [], [], static fn () => [], prepared: true)
            ->withAttributes(['cancellation_scope_id' => $membership]);
        $this->expectException(LogicException::class);
        PreparedLocalActivityCall::fromCommittedScopeDelivery($command, $sequence, false,
            self::history($fixture, 'before'), $fixture['task']['run_id'], $fixture['task']['workflow_id']);
    }

    public static function invalidProofAuthoring(): array
    {
        return [['root', 4], ['ancestor', 4], ['delivered', 3]];
    }

    public function test_unshielded_cleanup_is_refused_before_callback_admission(): void
    {
        $fixture = self::fixture();
        $this->expectException(WorkflowClaimAborted::class);
        self::replay(self::history($fixture, 'before'), false);
    }

    public function test_supervision_stops_a_running_cleanup_without_application_heartbeats_or_extra_budget(): void
    {
        if (!CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Prepared callback execution requires Unix process control.');
        }
        $directory = sys_get_temp_dir().'/dw-scoped-cleanup-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($directory, 0700));
        $fixture = self::fixture();
        $transport = new ScopedCleanupReceiptTransport($fixture['late_control']);
        $client = (new Client('https://server.example', transport: $transport,
            workerProtocolVersion: '1.20'))->withBoundedWorkerRequests();
        $pids = [];
        $admission = $fixture['late_duplicate_admission'];
        $started = hrtime(true) / 1e9;
        $runner = new PreparedLocalActivityRunner($client, self::admit($admission), $admission, $started,
            static fn (): bool => false,
            static function (): void { self::fail('Scoped cleanup cannot install whole-run cancellation.'); },
            static function (int $relay, int $callback) use (&$pids): void { $pids = [$relay, $callback]; },
            static function (RequestBudget $budget): void { $budget->remainingSeconds(); });
        try {
            try {
                $runner->execute(static function () use ($directory): string {
                    file_put_contents($directory.'/started', 'running');
                    sleep(10);
                    file_put_contents($directory.'/late', 'unfenced side effect');
                    return 'late';
                });
                self::fail('A callback cannot outlive its original captured authority.');
            } catch (WorkflowClaimAborted) {
                self::assertLessThan(5, hrtime(true) / 1e9 - $started);
            }
            self::assertFileExists($directory.'/started');
            self::assertFileDoesNotExist($directory.'/late');
            self::assertNotEmpty($transport->operations);
            self::assertSame(['control'], array_values(array_unique($transport->operations)));
            self::assertCount(2, $pids);
            foreach ($pids as $pid) {
                self::assertFalse(posix_kill($pid, 0), 'The callback and relay must both be joined.');
                self::assertSame(-1, pcntl_waitpid($pid, $status, WNOHANG));
            }
        } finally {
            foreach (glob($directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($directory);
        }
    }

    private static function admit(array $admission, ?int $heartbeatTimeout = null): PreparedLocalActivityAttempt
    {
        $fixture = self::fixture();
        $call = self::replay(self::history($fixture, 'before'))->preparedLocalActivity;
        return PreparedLocalActivityAttempt::fromPreparation($admission, $admission['workflow_task_id'],
            $fixture['task']['run_id'], $admission['lease_owner'], $admission['workflow_task_attempt'],
            $admission['worker_attempt_id'], $heartbeatTimeout, $call->cleanupSnapshot(), $fixture['scope_id']);
    }

    private static function replay(array $history, bool $shield = true): ReplayResult
    {
        $fixture = self::fixture();
        $workflow = static function (WorkflowContext $context) use ($shield): array {
            $result = $context->cancellationScope(static fn () => $context->cancellationScope(static function () use ($context, $shield): array {
                try { $context->sleep(10); } catch (WorkflowCancelled $cancelled) {
                    $cleanup = static fn () => $context->localActivity('tests.scoped-cleanup', [], [
                        'retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]],
                    ]);
                    $result = $shield ? $context->cancellationShield($cleanup) : $cleanup();
                    return ['result' => $result, 'remaining' => $cancelled->context->remaining()];
                }
                throw new LogicException('The original timer must deliver scope cancellation.');
            }));
            self::assertFalse($context->isCancellationRequested());
            self::assertNull($context->cancellationContext());
            return $result;
        };
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers', $fixture['task'],
            localActivityExecutor: static function (): never { throw new LogicException('Replay must never execute a cleanup callback inline.'); },
            prepareLocalActivities: true, allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    private static function history(array $fixture, string $stage): array
    {
        return array_slice($fixture['history'], 0, $fixture['history_ranges'][$stage]);
    }

    private static function fixture(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/scoped-local-cleanup.json'), true, flags: JSON_THROW_ON_ERROR);
    }
}

/** Canonical Native receipts with a frozen server clock, never a renewed root budget. */
final class ScopedCleanupReceiptTransport implements BoundedTransport
{
    public array $operations = [];

    public function __construct(private readonly array $control) {}

    public function supportsBoundedRequests(): bool { return true; }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        throw new LogicException('Scoped cleanup requires bounded worker requests.');
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $operation = basename($uri);
        $this->operations[] = $operation;
        if ($operation !== 'control' || $timeoutSeconds > 2
            || $body['lease_owner'] !== $this->control['lease_owner']
            || $body['workflow_task_attempt'] !== $this->control['workflow_task_attempt']) {
            throw new LogicException('Cleanup changed its original claim or tried to publish after authority expired.');
        }
        return $this->control;
    }
}
