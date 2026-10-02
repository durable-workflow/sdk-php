<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowTerminated;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CancellationPolicy;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowHandle;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/** Connected source qualification. Published defaults remain protocol 1.19. */
final class CooperativeCancellationTest extends TestCase
{
    private string $runtimeUrl;
    private string $token;
    private string $directory;

    /** @var array<int, true> */
    private array $workerProcesses = [];

    protected function setUp(): void
    {
        if (getenv('DURABLE_WORKFLOW_COOPERATIVE_QUALIFICATION') !== '1') {
            self::markTestSkipped('Candidate cooperative Server qualification is opt-in.');
        }
        $url = getenv('DURABLE_WORKFLOW_RUNTIME_URL');
        if (!is_string($url) || trim($url) === '') {
            self::fail('The cooperative qualification requires DURABLE_WORKFLOW_RUNTIME_URL.');
        }
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::fail('The connected worker proof requires pcntl and posix.');
        }
        $this->directory = sys_get_temp_dir().'/dw-connected-cooperative-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
        $this->runtimeUrl = $url;
        $token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN');
        $this->token = is_string($token) && $token !== '' ? $token : 'test-token';
        $protocol = $this->client()->clusterInfo()->raw['worker_protocol'];
        self::assertSame('1.20', $protocol['version']);
        self::assertTrue($protocol['server_capabilities']['cooperative_cancellation']);
        if (getenv('DURABLE_WORKFLOW_CHILD_POLICY_QUALIFICATION') === '1') {
            self::assertTrue($protocol['server_capabilities']['activity_cancellation_acknowledgement'] ?? false,
                'The exact Native source overlay must support remote stop receipts.');
        }
    }

    protected function tearDown(): void
    {
        $cleanupFailure = null;
        // A failed assertion in a scenario's first stop must not leave its
        // other worker holding the CI output pipe open after PHPUnit exits.
        foreach (array_keys($this->workerProcesses) as $pid) {
            try {
                $this->stopWorker($pid);
            } catch (Throwable $error) {
                $cleanupFailure ??= $error;
            }
        }
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
        if ($cleanupFailure !== null) {
            throw $cleanupFailure;
        }
    }

    #[DataProvider('booleanProvider')]
    public function testWaitingTimerRunsCleanupAfterLiveOrColdWorkerDelivery(bool $coldReplacement): void
    {
        $queue = $this->queue('timer');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['timer']);
            $this->awaitEvent($client, $handle, 'TimerScheduled');
            if ($coldReplacement) {
                $this->stopWorker($pid, true);
                $pid = 0;
                fclose($messages);
            }
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            if ($coldReplacement) {
                [$pid, $messages] = $this->spawnWorker($queue);
                $this->awaitMessage($messages, 'registered');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertSame(1, count(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'TimerCancelled')));
            self::assertNotContains('TimerFired', array_column($events, 'event_type'));
        } finally {
            if (is_resource($messages)) {
                fclose($messages);
            }
            $this->stopWorker($pid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testRequestBeforeClaimRetainsIdentityAfterAcceptedOrDiscardedReply(bool $loseReply): void
    {
        $queue = $this->queue('before-claim');
        $client = $this->client();
        $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['timer']);
        $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
        $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
        self::assertFalse($accepted['duplicate']);
        self::assertTrue($repeated['duplicate']);
        self::assertSame($accepted['cancellation_request'], $repeated['cancellation_request']);
        [$pid, $messages] = $this->spawnWorker($queue, $loseReply);
        try {
            $this->awaitMessage($messages, 'registered');
            if ($loseReply) {
                $this->awaitMessage($messages, 'delivery-reply-discarded');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertNotContains('TimerScheduled', array_column($events, 'event_type'));
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testLocalRequestStopsBlockedCallbackBeforeReturn(bool $userHeartbeat): void
    {
        $queue = $this->queue('local');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: $userHeartbeat);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['local']);
            $this->awaitMessage($messages, 'local-entered');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $started = microtime(true);
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertLessThan(10, microtime(true) - $started, 'Do not wait for the 60-second callback to return.');
            self::assertFileDoesNotExist($this->directory.'/late');
            foreach ($pids as $activityPid) { $this->assertProcessStops($activityPid); }
            $delivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            self::assertSame('local_activity', $delivery['payload']['call_kind']);
            self::assertSame(1, $delivery['payload']['sequence']);
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testPreparedLocalStopReceiptAndCleanupUseBackendAttempts(bool $userHeartbeat): void
    {
        $this->requirePreparedLocalSource();
        $queue = $this->queue('prepared-local');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: $userHeartbeat, preparedLocal: true);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['local']);
            $this->awaitMessage($messages, 'local-entered');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 30);
            $request = $accepted['cancellation_request'];
            $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
            self::assertTrue($repeated['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($request[$field], $repeated['cancellation_request'][$field]);
            }
            $events = $this->assertCancelledCleanup($client, $handle, $request['request_id'], $messages);
            self::assertLessThan((float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u'), microtime(true));
            foreach ($pids as $activityPid) { $this->assertProcessStops($activityPid); }
            self::assertFileDoesNotExist($this->directory.'/late');
            self::assertSame(1, count(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged')));
            $started = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityStarted'));
            self::assertCount(2, $started);
            foreach ($started as $event) { self::assertSame(1, $event['payload']['local_preparation']['version']); }
            $cleanup = $started[1]['payload']['local_preparation']['cancellation_cleanup'];
            self::assertSame($request['request_id'], $cleanup['request_id']);
            self::assertSame($request['request_id'], $cleanup['root_request_id']);
            self::assertEquals(new \DateTimeImmutable($request['cleanup_deadline_at']), new \DateTimeImmutable($cleanup['cleanup_deadline_at']));
            $completed = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCompleted'))[0];
            self::assertSame($started[1]['payload']['activity_attempt_id'], $completed['payload']['activity_attempt_id']);
            self::assertSame(1, $completed['payload']['local_outcome']['version']);
            fwrite(STDOUT, 'Prepared local source stop and cleanup: '.json_encode([
                'application_heartbeats' => $userHeartbeat, 'request_id' => $request['request_id'],
                'original_deadline' => $request['cleanup_deadline_at'], 'callback_pids' => $pids,
                'backend_attempt_ids' => array_column(array_column($started, 'payload'), 'activity_attempt_id'),
                'history' => $events,
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    public function testPreparedCleanupResumesAfterWorkerSigkillWithinOriginalThirtySeconds(): void
    {
        $this->requirePreparedLocalSource();
        $queue = $this->queue('prepared-cleanup-kill');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, blockCleanup: true, preparedLocal: true);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['local']);
            $this->awaitMessage($messages, 'local-entered');
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 30);
            $request = $accepted['cancellation_request'];
            $this->awaitMessage($messages, 'cleanup-entered');
            $before = $this->history($client, $handle);
            $delivery = array_values(array_filter($before, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            $pids = json_decode((string) file_get_contents($this->directory.'/cleanup-processes'), true, flags: JSON_THROW_ON_ERROR);
            $this->stopWorker($pid, true);
            $pid = 0;
            foreach ($pids as $activityPid) { $this->assertProcessStops($activityPid); }
            fclose($messages);
            [$pid, $messages] = $this->spawnWorker($queue, preparedLocal: true);
            $this->awaitMessage($messages, 'registered');
            $events = $this->assertCancelledCleanup($client, $handle, $request['request_id'], $messages);
            self::assertLessThan((float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u'), microtime(true));
            self::assertFileDoesNotExist($this->directory.'/cleanup-returned');
            $afterDelivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'));
            self::assertSame([$delivery], $afterDelivery, 'Replacement changed the original canonical delivery boundary.');
            $recovery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityRetryScheduled'));
            self::assertCount(1, $recovery);
            self::assertSame('unknown', $recovery[0]['payload']['local_recovery']['callback_stop_state']);
            $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
            self::assertTrue($repeated['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($request[$field], $repeated['cancellation_request'][$field]);
            }
            fwrite(STDOUT, 'Prepared cleanup source SIGKILL recovery: '.json_encode([
                'request_id' => $request['request_id'], 'original_deadline' => $request['cleanup_deadline_at'],
                'killed_callback_pids' => $pids, 'original_delivery' => $delivery, 'history' => $events,
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            if (is_resource($messages)) { fclose($messages); }
            $this->stopWorker($pid);
        }
    }

    private function requirePreparedLocalSource(): void
    {
        if (getenv('DURABLE_WORKFLOW_CHILD_POLICY_QUALIFICATION') !== '1') {
            self::markTestSkipped('Prepared local qualification requires the exact Native source overlay.');
        }
        self::assertTrue($this->client()->clusterInfo()->raw['worker_protocol']['server_capabilities']['prepared_local_activities'] ?? false);
    }

    #[DataProvider('booleanProvider')]
    public function testPreparedGroupStopsBothMembersAndReplaysCleanupWithinOriginalDeadline(bool $killDuringCleanup): void
    {
        $this->requirePreparedLocalSource();
        self::assertTrue($this->client()->clusterInfo()->raw['worker_protocol']['server_capabilities']['prepared_local_activity_groups'] ?? false);
        $queue = $this->queue('prepared-group');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, blockCleanup: $killDuringCleanup, preparedLocal: true);
        $killedPids = [];
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['local-group']);
            $this->awaitMessages($messages, ['group-entered-0', 'group-entered-1']);
            $callbackPids = [];
            foreach ([0, 1] as $index) {
                $callbackPids = [...$callbackPids, ...json_decode((string) file_get_contents($this->directory.'/group-processes-'.$index), true, flags: JSON_THROW_ON_ERROR)];
            }
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 30);
            $request = $accepted['cancellation_request'];
            $originalDelivery = null;
            if ($killDuringCleanup) {
                $this->awaitMessages($messages, ['group-cleanup-entered-0', 'group-cleanup-entered-1']);
                foreach ([0, 1] as $index) {
                    $killedPids = [...$killedPids, ...json_decode((string) file_get_contents($this->directory.'/group-cleanup-processes-'.$index), true, flags: JSON_THROW_ON_ERROR)];
                }
                $before = $this->history($client, $handle);
                $originalDelivery = array_values(array_filter($before, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
                $this->stopWorker($pid, true);
                $pid = 0;
                foreach ($killedPids as $activityPid) { $this->assertProcessStops($activityPid); }
                fclose($messages);
                [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, preparedLocal: true);
                $this->awaitMessage($messages, 'registered');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $request['request_id'], $messages, expectedCleanupCount: 2);
            self::assertLessThan((float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u'), microtime(true));
            foreach ($callbackPids as $activityPid) { $this->assertProcessStops($activityPid); }
            foreach ([0, 1] as $index) {
                self::assertFileDoesNotExist($this->directory.'/group-late-'.$index);
                self::assertFileDoesNotExist($this->directory.'/group-cleanup-late-'.$index);
            }
            $delivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'));
            if ($originalDelivery !== null) { self::assertSame([$originalDelivery], $delivery, 'Replacement changed the complete group delivery boundary.'); }
            self::assertSame(2, $delivery[0]['payload']['sequence_span']);
            self::assertCount(2, array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged'));
            $started = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityStarted'));
            self::assertCount($killDuringCleanup ? 6 : 4, $started);
            self::assertCount(count($started), array_unique(array_column(array_column($started, 'payload'), 'activity_attempt_id')));
            foreach (array_slice($started, 2) as $event) {
                $cleanup = $event['payload']['local_preparation']['cancellation_cleanup'];
                self::assertSame($request['request_id'], $cleanup['root_request_id']);
                self::assertSame($delivery[0]['id'], $cleanup['delivery_history_event_id']);
                self::assertEquals(new \DateTimeImmutable($request['cleanup_deadline_at']), new \DateTimeImmutable($cleanup['cleanup_deadline_at']));
            }
            $recoveries = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityRetryScheduled'));
            self::assertCount($killDuringCleanup ? 2 : 0, $recoveries);
            foreach ($recoveries as $event) { self::assertSame('unknown', $event['payload']['local_recovery']['callback_stop_state']); }
            $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
            self::assertTrue($repeated['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) { self::assertSame($request[$field], $repeated['cancellation_request'][$field]); }
            fwrite(STDOUT, 'Prepared group source cancellation: '.json_encode([
                'worker_sigkill_during_cleanup' => $killDuringCleanup, 'application_heartbeats' => false,
                'request_id' => $request['request_id'], 'original_deadline' => $request['cleanup_deadline_at'],
                'joined_work_pids' => $callbackPids, 'killed_cleanup_pids' => $killedPids, 'history' => $events,
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            if (is_resource($messages)) { fclose($messages); }
            $this->stopWorker($pid);
        }
    }

    public static function booleanProvider(): array
    {
        return [[false], [true]];
    }

    #[DataProvider('booleanProvider')]
    public function testOneWorkflowWorkerFinishesChildCleanupBeforeParentDelivery(bool $coldReplacement): void
    {
        if (getenv('DURABLE_WORKFLOW_CHILD_POLICY_QUALIFICATION') !== '1') {
            self::markTestSkipped('Child policy qualification requires the candidate Native backend.');
        }
        $queue = $this->queue('child-wait');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, observeChildWait: true);
        try {
            $this->awaitMessage($messages, 'registered');
            $parent = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['child-wait']);
            $this->awaitEvent($client, $parent, 'ChildRunStarted');
            $scheduled = array_values(array_filter($this->history($client, $parent),
                static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'))[0];
            $child = new WorkflowHandle($client, $scheduled['payload']['child_workflow_instance_id'],
                $scheduled['payload']['child_workflow_run_id']);
            $this->awaitEvent($client, $child, 'TimerScheduled');

            $accepted = $parent->requestSelectedRunCancellation('connected child cleanup', 30);
            $request = $accepted['cancellation_request'];
            $this->awaitMessage($messages, 'child-wait-released');
            $parentWaiting = $this->history($client, $parent);
            self::assertNotContains('CooperativeCancellationDelivered', array_column($parentWaiting, 'event_type'));
            $propagation = array_values(array_filter($parentWaiting,
                static fn (array $event): bool => $event['event_type'] === 'ChildCancellationRequested'))[0];
            self::assertSame('accepted', $propagation['payload']['request_outcome']);
            self::assertSame($request['request_id'], $propagation['payload']['root_request_id']);
            self::assertNotSame($request['request_id'], $propagation['payload']['child_request_id']);
            $diagnostics = $client->workflowDiagnostics($parent->workflowId, (string) $parent->selectedRunId);
            self::assertSame([], $diagnostics['pending_workflow_tasks'], 'The parent must surrender its waiting claim.');
            $duplicate = $parent->requestSelectedRunCancellation('a later request cannot extend the budget', 300);
            self::assertTrue($duplicate['duplicate']);
            self::assertSame($request, $duplicate['cancellation_request']);

            if ($coldReplacement) {
                $this->stopWorker($pid, true);
                $pid = 0;
                fclose($messages);
            }
            file_put_contents($this->directory.'/continue-child-wait', 'resume');
            if ($coldReplacement) {
                [$pid, $messages] = $this->spawnWorker($queue, observeChildWait: true);
                $this->awaitMessage($messages, 'registered');
            }
            $parentHistory = $this->assertCancelledCleanup($client, $parent, $request['request_id'], $messages);
            self::assertSame('cancelled', strtolower($child->describeSelectedRun()->status));
            $childHistory = $this->history($client, $child);
            $childKinds = array_column($childHistory, 'event_type');
            foreach (['CooperativeCancellationRequested', 'CooperativeCancellationDelivered', 'ActivityCompleted', 'WorkflowCancelled'] as $kind) {
                self::assertSame(1, count(array_filter($childKinds, static fn (string $value): bool => $value === $kind)), $kind);
            }
            self::assertNotContains('WorkflowTerminated', $childKinds);
            self::assertNotContains('WorkflowFailed', $childKinds);
            $resolved = array_values(array_filter($parentHistory,
                static fn (array $event): bool => $event['event_type'] === 'ChildCancellationResolved'))[0];
            self::assertSame('cancelled', $resolved['payload']['child_status']);

            $parentContext = json_decode((string) file_get_contents($this->directory.'/context-'.$request['request_id']), true, flags: JSON_THROW_ON_ERROR);
            $childContext = json_decode((string) file_get_contents($this->directory.'/context-'.$propagation['payload']['child_request_id']), true, flags: JSON_THROW_ON_ERROR);
            foreach (['root_request_id', 'root_workflow_instance_id', 'root_workflow_run_id', 'requested_at', 'cleanup_deadline_at', 'reason', 'requester'] as $field) {
                self::assertSame($parentContext[$field], $childContext[$field], $field);
            }
            self::assertSame($request['request_id'], $childContext['parent_request_id']);
            self::assertCount(2, $childContext['lineage']);
            $deadline = new \DateTimeImmutable($request['cleanup_deadline_at']);
            foreach ([$parentHistory, $childHistory] as $events) {
                $terminal = array_values(array_filter($events,
                    static fn (array $event): bool => $event['event_type'] === 'WorkflowCancelled'))[0];
                self::assertSame('Cooperative cancellation completed.', $terminal['payload']['reason']);
                self::assertLessThan($deadline, new \DateTimeImmutable($terminal['recorded_at'] ?? $terminal['timestamp']));
            }
            $observations = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
                file($this->directory.'/child-wait-deliveries', FILE_IGNORE_NEW_LINES) ?: []);
            $parentDeliveries = array_values(array_filter($observations,
                static fn (array $entry): bool => ($entry['body']['request_id'] ?? null) === $request['request_id']));
            self::assertCount(2, $parentDeliveries);
            self::assertTrue($parentDeliveries[0]['reply']['claim_released']);
            self::assertFalse($parentDeliveries[0]['reply']['delivered']);
            self::assertTrue($parentDeliveries[1]['reply']['delivered']);
            self::assertNotSame($parentDeliveries[0]['reply']['task_id'], $parentDeliveries[1]['reply']['task_id']);
            self::assertSame(!$coldReplacement, $parentDeliveries[0]['worker_pid'] === $parentDeliveries[1]['worker_pid']);
            $childDeliveries = array_values(array_filter($observations,
                static fn (array $entry): bool => ($entry['body']['request_id'] ?? null) === $childContext['request_id']));
            self::assertCount(1, $childDeliveries);
            self::assertSame($parentDeliveries[1]['worker_pid'], $childDeliveries[0]['worker_pid']);
            $parentDelivery = array_values(array_filter($parentHistory,
                static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            $childTerminal = array_values(array_filter($childHistory,
                static fn (array $event): bool => $event['event_type'] === 'WorkflowCancelled'))[0];
            self::assertGreaterThan(new \DateTimeImmutable($childTerminal['timestamp']), new \DateTimeImmutable($parentDelivery['timestamp']));
            foreach (['request_id', 'sequence', 'call_kind', 'sequence_span', 'operation_sequence', 'operation_sequence_span'] as $field) {
                self::assertSame($parentDeliveries[0]['body'][$field] ?? null, $parentDeliveries[1]['body'][$field] ?? null, $field);
            }
            fwrite(STDOUT, 'Connected single-worker child wait: '.json_encode([
                'cold_replacement' => $coldReplacement, 'parent_context' => $parentContext,
                'child_context' => $childContext, 'deliveries' => $observations,
                'parent_history' => $parentHistory, 'child_history' => $childHistory,
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            if (is_resource($messages)) { fclose($messages); }
            $this->stopWorker($pid);
        }
    }

    public static function remoteProvider(): array
    {
        return [[false, false], [true, false], [false, true], [true, true]];
    }

    #[DataProvider('remoteProvider')]
    public function testRemoteRequestStopsBlockedOwnerAfterLiveOrColdWorkflowDelivery(bool $userHeartbeat, bool $coldWorkflow): void
    {
        $queue = $this->queue('remote');
        $client = $this->client();
        [$workflowPid, $workflowMessages] = $this->spawnWorker($queue, pauseWorkflowClaim: $coldWorkflow);
        [$ownerPid, $ownerMessages] = $this->spawnWorker($queue, userHeartbeat: $userHeartbeat, remoteRole: true);
        try {
            $this->awaitMessage($workflowMessages, 'registered');
            $this->awaitMessage($ownerMessages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['remote']);
            self::assertNotNull($handle->selectedRunId);
            $this->awaitMessage($ownerMessages, 'remote-entered');
            $this->awaitMessage($ownerMessages, 'owner-heartbeat');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            foreach ($pids as $pid) { self::assertTrue(posix_kill($pid, 0), 'The remote callback must still be active.'); }
            $remainingWorkflowLease = 0.0;
            $heldWorkflowClaim = null;
            $deadWorkerId = $queue.'-'.$workflowPid;
            if ($coldWorkflow) {
                file_put_contents($this->directory.'/pause-workflow-claim', 'armed');
            }
            $started = microtime(true);
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            if ($coldWorkflow) {
                $this->awaitMessage($workflowMessages, 'workflow-claim-held');
                $heldWorkflowClaim = json_decode((string) file_get_contents($this->directory.'/held-workflow-claim'), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame($deadWorkerId, $heldWorkflowClaim['lease_owner']);
                self::assertGreaterThan(0, $heldWorkflowClaim['workflow_task_attempt']);
                $this->stopWorker($workflowPid, true);
                $workflowPid = 0;
                fclose($workflowMessages);
                $diagnostics = $client->workflowDiagnostics($handle->workflowId, $handle->selectedRunId);
                $observedAt = (float) (new \DateTimeImmutable($diagnostics['generated_at']))->format('U.u');
                foreach ($diagnostics['pending_workflow_tasks'] as $pending) {
                    if (($pending['status'] ?? null) === 'leased' && ($pending['lease_owner'] ?? null) === $deadWorkerId) {
                        $expiresAt = (float) (new \DateTimeImmutable($pending['lease_expires_at']))->format('U.u');
                        $remainingWorkflowLease = max($remainingWorkflowLease, $expiresAt - $observedAt);
                    }
                }
                // The isolated stack grants ten-second workflow leases. A dead
                // process cannot surrender a still-current claim immediately.
                self::assertGreaterThan(0, $remainingWorkflowLease, 'Kill after a real still-current task claim.');
                self::assertLessThanOrEqual(10, $remainingWorkflowLease, 'Unexpected qualification workflow lease.');
            }
            $workflowLeaseWait = $remainingWorkflowLease;
            $nextLeaseObservation = 0.0;
            $lastLeaseState = null;
            $observeLease = $coldWorkflow ? function () use (
                $client, $handle, $deadWorkerId, $started, &$workflowLeaseWait,
                &$nextLeaseObservation, &$lastLeaseState,
            ): void {
                if (microtime(true) < $nextLeaseObservation) { return; }
                $nextLeaseObservation = microtime(true) + 1;
                $diagnostics = $client->workflowDiagnostics($handle->workflowId, $handle->selectedRunId);
                $observedAt = (float) (new \DateTimeImmutable($diagnostics['generated_at']))->format('U.u');
                $state = $diagnostics['pending_workflow_tasks'];
                foreach ($state as $pending) {
                    if (($pending['status'] ?? null) === 'leased' && ($pending['lease_owner'] ?? null) === $deadWorkerId) {
                        $expiresAt = (float) (new \DateTimeImmutable($pending['lease_expires_at']))->format('U.u');
                        $remaining = max(0, $expiresAt - $observedAt);
                        self::assertLessThanOrEqual(10, $remaining, 'Unexpected qualification workflow lease.');
                        $workflowLeaseWait = max($workflowLeaseWait, microtime(true) - $started + $remaining);
                    }
                }
                if ($state !== $lastLeaseState) {
                    fwrite(STDOUT, 'Cold workflow tasks: '.json_encode($state, JSON_THROW_ON_ERROR)."\n");
                    $lastLeaseState = $state;
                }
            } : null;
            if ($coldWorkflow) {
                [$workflowPid, $workflowMessages] = $this->spawnWorker($queue);
                $this->awaitMessage($workflowMessages, 'registered');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $workflowMessages, $observeLease);
            $elapsed = microtime(true) - $started;
            fwrite(STDOUT, sprintf("Connected remote recovery: cold=%s observed_lease_wait=%.3fs elapsed=%.3fs\n",
                $coldWorkflow ? 'yes' : 'no', $workflowLeaseWait, $elapsed));
            $phases = array_map(static fn (array $event): array => array_intersect_key($event,
                ['event_type' => true, 'recorded_at' => true, 'timestamp' => true]), $events);
            fwrite(STDOUT, 'Remote cancellation phases: '.json_encode($phases, JSON_THROW_ON_ERROR)."\n");
            self::assertLessThan(10 + $workflowLeaseWait, $elapsed,
                'Recover within the observed workflow lease plus ten seconds, before the 60-second callback returns.');
            foreach ($pids as $pid) { $this->assertProcessStops($pid); }
            self::assertFileDoesNotExist($this->directory.'/late');
            $delivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            self::assertSame('activity', $delivery['payload']['call_kind']);
            if ($heldWorkflowClaim !== null) {
                self::assertGreaterThanOrEqual(
                    (float) (new \DateTimeImmutable($heldWorkflowClaim['lease_expires_at']))->format('U.u'),
                    (float) (new \DateTimeImmutable($delivery['timestamp']))->format('U.u'),
                    'Replacement must respect the killed owner\'s current lease.');
                try {
                    $client->completeWorkflowTask($heldWorkflowClaim['task_id'], $heldWorkflowClaim['lease_owner'],
                        $heldWorkflowClaim['workflow_task_attempt'], [['type' => 'complete_workflow']]);
                    self::fail('The killed workflow owner published a late completion.');
                } catch (\DurableWorkflow\Exception\ServerException $error) { self::assertSame(409, $error->status); }
            }
            self::assertSame(1, count(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancelled')));
            $heartbeats = array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityHeartbeatRecorded'
                && ($event['payload']['activity_type'] ?? null) === 'tests.php-cooperative-remote');
            self::assertSame($userHeartbeat, count($heartbeats) > 0, 'Owner checks must not manufacture user progress.');
            $fence = json_decode((string) file_get_contents($this->directory.'/remote-fence'), true, flags: JSON_THROW_ON_ERROR);
            if (getenv('DURABLE_WORKFLOW_CHILD_POLICY_QUALIFICATION') === '1') {
                $until = microtime(true) + 5;
                do {
                    $status = $client->activityTaskStatus($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner']);
                    if (($status['cancellation_acknowledgement']['callback_state'] ?? null) === 'stopped') { break; }
                    usleep(50_000);
                } while (microtime(true) < $until);
                $receipt = $status['cancellation_acknowledgement'];
                self::assertSame('stopped', $receipt['callback_state'], 'A stopped callback must leave its durable receipt.');
                self::assertSame($accepted['cancellation_request']['request_id'], $receipt['request_id']);
                self::assertSame($receipt['request_id'], $receipt['root_request_id']);
                self::assertSame($accepted['cancellation_request']['cleanup_deadline_at'], $receipt['cleanup_deadline_at']);
                self::assertFalse($receipt['received_after_deadline']);
                self::assertFalse($status['heartbeat_recorded']);
                self::assertFalse($status['can_continue']);
                $duplicate = $client->acknowledgeActivityCancellation($fence['task_id'], $fence['activity_attempt_id'],
                    $fence['lease_owner'], $receipt['request_id']);
                self::assertTrue($duplicate['acknowledged']);
                self::assertTrue($duplicate['duplicate']);
                self::assertSame($receipt['history_event_id'], $duplicate['history_event_id']);
                self::assertFalse($duplicate['heartbeat_recorded']);
                $events = $this->history($client, $handle);
                $acks = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged'));
                self::assertCount(1, $acks);
                self::assertSame('stopped', $acks[0]['payload']['callback_state']);
                self::assertSame('activity_worker', $acks[0]['payload']['evidence_source']);
                self::assertSame($fence['activity_attempt_id'], $acks[0]['payload']['activity_attempt_id']);
                self::assertSame($receipt['request_id'], $acks[0]['payload']['request_id']);
                self::assertSame($receipt['root_request_id'], $acks[0]['payload']['root_request_id']);
                self::assertSame($receipt['cleanup_deadline_at'], $acks[0]['payload']['cleanup_deadline_at']);
                self::assertSame($receipt['cancellation_history_event_id'], $acks[0]['payload']['cancellation_history_event_id']);
                fwrite(STDOUT, 'Remote stop receipt: '.json_encode([
                    'user_heartbeat' => $userHeartbeat, 'cold_workflow' => $coldWorkflow,
                    'processes_stopped' => $pids, 'claim' => $fence, 'status' => $status,
                    'duplicate' => $duplicate, 'history' => $acks,
                ], JSON_THROW_ON_ERROR)."\n");
            }
            foreach (['complete', 'fail'] as $outcome) {
                try {
                    if ($outcome === 'complete') { $client->completeActivityTask($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner'], 'late'); }
                    else { $client->failActivityTask($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner'], 'late', 'LateQualification'); }
                    self::fail('A cancelled remote attempt accepted a late '.$outcome.'.');
                } catch (\DurableWorkflow\Exception\ServerException $error) { self::assertSame(409, $error->status); }
            }
            self::assertSame($events, $this->history($client, $handle));
        } finally {
            if (is_file($this->directory.'/pause-workflow-claim')) { unlink($this->directory.'/pause-workflow-claim'); }
            if (is_resource($workflowMessages)) { fclose($workflowMessages); }
            fclose($ownerMessages);
            $this->stopWorker($workflowPid);
            $this->stopWorker($ownerPid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testRemoteOwnerShutdownOrSigkillReapsCallbackBeforeCleanup(bool $kill): void
    {
        $queue = $this->queue('owner-death');
        $client = $this->client();
        [$workflowPid, $workflowMessages] = $this->spawnWorker($queue);
        [$ownerPid, $ownerMessages] = $this->spawnWorker($queue, userHeartbeat: false, remoteRole: true);
        try {
            $this->awaitMessage($workflowMessages, 'registered');
            $this->awaitMessage($ownerMessages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['remote']);
            $this->awaitMessage($ownerMessages, 'remote-entered');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $this->stopWorker($ownerPid, $kill);
            $ownerPid = 0;
            foreach ($pids as $pid) { $this->assertProcessStops($pid); }
            self::assertFileDoesNotExist($this->directory.'/late');
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $workflowMessages);
        } finally {
            fclose($workflowMessages);
            fclose($ownerMessages);
            $this->stopWorker($workflowPid);
            $this->stopWorker($ownerPid);
        }
    }

    public function testCooperativeWorkerHydratesAndPublishesAboveInlinePayloads(): void
    {
        $namespace = $this->queue('payloads');
        $admin = $this->client();
        $admin->createNamespace($namespace);
        $admin->setNamespaceExternalStorage($namespace, 'local', thresholdBytes: 64,
            config: ['uri' => 'file:///app/storage/app/cooperative-payloads/'.$namespace]);
        $client = $this->client(namespace: $namespace);
        $value = str_repeat('bounded-payload-', 131072);
        self::assertGreaterThan(2097152, strlen($client->payloadCodec()->encode($value)));
        [$pid, $messages] = $this->spawnWorker($namespace, namespace: $namespace);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative-payload', $namespace, $namespace, [$value]);
            self::assertSame($value, $handle->result(timeoutSeconds: 30));
            $raw = (new Psr18Transport())->send('GET', $this->runtimeUrl.'/api/workflows/'.$handle->workflowId,
                ['Authorization' => 'Bearer '.$this->token, 'X-Namespace' => $namespace,
                    'X-Durable-Workflow-Control-Plane-Version' => '2']);
            self::assertArrayHasKey('external_payload', $raw['output_envelope']);
            $reference = $raw['output_envelope']['external_payload'];
            $blob = $client->payloadCodec()->encode($value);
            self::assertSame(strlen($blob), $reference['size_bytes']);
            self::assertSame(hash('sha256', $blob), $reference['sha256']);
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    public function testKilledActivityOwnerIsReclaimedBeforeCooperativeCleanup(): void
    {
        $queue = $this->queue('activity-reclaim');
        $client = $this->client();
        [$workflowPid, $workflowMessages] = $this->spawnWorker($queue);
        [$ownerPid, $ownerMessages] = $this->spawnWorker($queue, userHeartbeat: false,
            remoteRole: true, remoteBlockSeconds: 360);
        try {
            $this->awaitMessage($workflowMessages, 'registered');
            $this->awaitMessage($ownerMessages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['remote']);
            $this->awaitMessage($ownerMessages, 'remote-entered');
            $original = json_decode((string) file_get_contents($this->directory.'/remote-fence'), true, flags: JSON_THROW_ON_ERROR);
            $processes = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            self::assertSame(1, $original['attempt_number']);
            $leased = $client->activityTaskStatus($original['task_id'], $original['activity_attempt_id'], $original['lease_owner']);
            self::assertTrue($leased['can_continue']);
            self::assertSame('running', $leased['attempt_status']);
            $expiresAt = (float) (new \DateTimeImmutable($leased['lease_expires_at']))->format('U.u');
            self::assertGreaterThan(microtime(true), $expiresAt, 'Kill a genuinely current activity lease.');
            fwrite(STDOUT, 'SIGKILL original activity: '.json_encode([$original, $leased], JSON_THROW_ON_ERROR)."\n");
            $this->stopWorker($ownerPid, true);
            $ownerPid = 0;
            fclose($ownerMessages);
            foreach ($processes as $pid) { $this->assertProcessStops($pid); }
            self::assertFileDoesNotExist($this->directory.'/late');

            [$ownerPid, $ownerMessages] = $this->spawnWorker($queue, userHeartbeat: false, remoteRole: true);
            $this->awaitMessage($ownerMessages, 'registered');
            // Wait for Native's real five-minute lease and the normal repair
            // pass. No clock, database row or production lease is changed.
            $this->awaitReclaimedActivity($ownerMessages);
            $replacement = json_decode((string) file_get_contents($this->directory.'/remote-fence'), true, flags: JSON_THROW_ON_ERROR);
            self::assertSame($original['task_id'], $replacement['task_id']);
            self::assertNotSame($original['activity_attempt_id'], $replacement['activity_attempt_id']);
            self::assertNotSame($original['lease_owner'], $replacement['lease_owner']);
            self::assertSame(2, $replacement['attempt_number']);
            self::assertGreaterThanOrEqual($expiresAt, microtime(true), 'Reclaim must follow actual lease expiry.');
            fwrite(STDOUT, 'SIGKILL replacement activity: '.json_encode($replacement, JSON_THROW_ON_ERROR)."\n");

            $closed = $client->activityTaskStatus($original['task_id'], $original['activity_attempt_id'], $original['lease_owner']);
            self::assertSame('expired', $closed['attempt_status']);
            self::assertFalse($closed['can_continue']);
            $before = $this->history($client, $handle);
            foreach (['complete', 'fail'] as $operation) {
                try {
                    if ($operation === 'complete') {
                        $client->completeActivityTask($original['task_id'], $original['activity_attempt_id'], $original['lease_owner'], 'late');
                    } else {
                        $client->failActivityTask($original['task_id'], $original['activity_attempt_id'], $original['lease_owner'], 'late', 'LateQualification');
                    }
                    self::fail('The killed attempt accepted a late '.$operation.'.');
                } catch (\DurableWorkflow\Exception\ServerException $error) {
                    self::assertSame(409, $error->status);
                }
            }
            $heartbeat = $client->heartbeatActivityTask($original['task_id'], $original['activity_attempt_id'], $original['lease_owner'], ['late' => true]);
            self::assertFalse($heartbeat['can_continue']);
            self::assertFalse($heartbeat['heartbeat_recorded']);
            self::assertFalse($heartbeat['cancel_requested']);
            self::assertSame('attempt_closed', $heartbeat['reason']);
            self::assertSame($closed['lease_expires_at'], $heartbeat['lease_expires_at']);
            self::assertSame($closed['last_heartbeat_at'], $heartbeat['last_heartbeat_at']);
            self::assertSame($closed, $client->activityTaskStatus($original['task_id'], $original['activity_attempt_id'], $original['lease_owner']));
            self::assertSame($before, $this->history($client, $handle), 'Dead attempt changed canonical history.');

            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $workflowMessages);
            self::assertSame(1, count(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancelled')));
            self::assertFileDoesNotExist($this->directory.'/late');
            $processes = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            foreach ($processes as $pid) { $this->assertProcessStops($pid); }
            fwrite(STDOUT, 'SIGKILL reclaimed cancellation history: '.json_encode($events, JSON_THROW_ON_ERROR)."\n");
        } finally {
            if (is_resource($workflowMessages)) { fclose($workflowMessages); }
            if (is_resource($ownerMessages)) { fclose($ownerMessages); }
            $this->stopWorker($workflowPid);
            $this->stopWorker($ownerPid);
        }
    }

    #[DataProvider('cleanupCutoffProvider')]
    public function testDeadlineOrTerminationStopsBlockedCleanup(bool $terminate): void
    {
        $queue = $this->queue('cleanup-cutoff');
        $client = $this->client();
        $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['timer']);
        $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: $terminate ? 60 : 5);
        [$pid, $messages] = $this->spawnWorker($queue, blockCleanup: true);
        try {
            $this->awaitMessage($messages, 'registered');
            $this->awaitMessage($messages, 'cleanup-entered');
            $observed = microtime(true) + 2;
            while (!is_file($this->directory.'/cleanup-processes') && microtime(true) < $observed) {
                usleep(10_000);
            }
            self::assertFileExists($this->directory.'/cleanup-processes');
            $processes = json_decode((string) file_get_contents($this->directory.'/cleanup-processes'), true, flags: JSON_THROW_ON_ERROR);
            if ($terminate) { $handle->terminateSelectedRun('qualification termination during cleanup'); }
            if (!$terminate) {
                $deadline = (float) (new \DateTimeImmutable($accepted['cancellation_request']['cleanup_deadline_at']))->format('U.u');
                while (microtime(true) < $deadline) { usleep(10_000); }
            }
            foreach ($processes as $activityPid) { $this->assertProcessStops($activityPid); }
            self::assertFileDoesNotExist($this->directory.'/cleanup-returned');
            $stoppedAt = microtime(true);
            $diagnostics = $client->workflowDiagnostics($handle->workflowId, (string) $handle->selectedRunId);
            fwrite(STDOUT, 'Blocked cleanup stopped: '.json_encode([
                'terminate' => $terminate, 'stopped_at' => $stoppedAt,
                'cleanup_deadline_at' => $accepted['cancellation_request']['cleanup_deadline_at'],
                'pending_workflow_tasks' => $diagnostics['pending_workflow_tasks'],
            ], JSON_THROW_ON_ERROR)."\n");
            try {
                // A callback stops at its deadline. Durable closure may then
                // require the real ten-second workflow lease, ten-second
                // unscoped repair cadence and one bounded poll/recovery grace period.
                $handle->result(25, 0.1);
                self::fail('Blocked cleanup produced a successful workflow result.');
            } catch (WorkflowCancelled $error) {
                self::assertFalse($terminate, $error->getMessage());
            } catch (WorkflowTerminated $error) {
                self::assertTrue($terminate, $error->getMessage());
            }
            fwrite(STDOUT, sprintf("Blocked cleanup durable closure after callback stop: %.3fs\n", microtime(true) - $stoppedAt));
            $history = $this->history($client, $handle);
            $kinds = array_column($history, 'event_type');
            foreach (['CooperativeCancellationRequested', 'CooperativeCancellationDelivered'] as $kind) {
                self::assertSame(1, count(array_filter($kinds, static fn (string $value): bool => $value === $kind)));
                $event = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === $kind))[0];
                self::assertSame($accepted['cancellation_request']['request_id'], $event['payload']['workflow_command_id']);
            }
            self::assertSame($terminate ? 0 : 1, count(array_filter($kinds, static fn (string $value): bool => $value === 'WorkflowCancelled')));
            self::assertSame($terminate ? 1 : 0, count(array_filter($kinds, static fn (string $value): bool => $value === 'WorkflowTerminated')));
            foreach (['ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'WorkflowCompleted', 'WorkflowFailed'] as $kind) {
                self::assertNotContains($kind, $kinds);
            }
            fwrite(STDOUT, 'Blocked cleanup cutoff: '.json_encode(['terminate' => $terminate, 'history' => $history], JSON_THROW_ON_ERROR)."\n");
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    public static function cleanupCutoffProvider(): array
    {
        return [[false], [true]];
    }

    private function client(?Transport $transport = null, string $namespace = 'default'): Client
    {
        return new Client($this->runtimeUrl, namespace: $namespace, token: $this->token,
            transport: $transport, workerProtocolVersion: '1.20');
    }

    private function queue(string $kind): string
    {
        return 'php-cooperative-'.$kind.'-'.bin2hex(random_bytes(4));
    }

    /** @return array{int, resource} */
    private function spawnWorker(string $queue, bool $loseReply = false, bool $userHeartbeat = true, string $namespace = 'default', bool $remoteRole = false, bool $pauseWorkflowClaim = false, int $remoteBlockSeconds = 60, bool $blockCleanup = false, bool $observeChildWait = false, bool $preparedLocal = false): array
    {
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($sockets === false) {
            self::fail('Could not create the worker observation socket pair.');
        }
        [$parent, $child] = $sockets;
        $pid = pcntl_fork();
        if ($pid === -1) {
            fclose($parent);
            fclose($child);
            self::fail('Could not fork the cooperative worker.');
        }
        if ($pid === 0) {
            fclose($parent);
            $notify = static function (string $message) use ($child): void {
                fwrite($child, $message."\n");
                fflush($child);
            };
            try {
                // Construct transport and worker after fork. No inherited HTTP connection is used.
                $transport = $observeChildWait ? new ChildWaitObservationTransport($this->directory, $notify)
                    : ($remoteRole ? new RemoteOwnerObservationTransport($notify)
                    : ($pauseWorkflowClaim ? new PauseWorkflowClaimTransport($this->directory, $notify)
                        : ($loseReply ? new DiscardFirstDeliveryReplyTransport($notify) : null)));
                $failureReported = false;
                $worker = new Worker($this->client($transport, $namespace), $queue,
                    workerId: $queue.'-'.getmypid(), enableCooperativeCancellation: true, enablePreparedLocalActivities: $preparedLocal,
                    diagnosticListener: function (string $event, array $context) use ($notify, &$failureReported, $blockCleanup): void {
                        if ($event === 'worker.registered') {
                            $notify('registered');
                        }
                        if ($event === 'worker.activity_process_started'
                            && in_array($context['activity_type'] ?? null, ['tests.php-cooperative-work', 'tests.php-cooperative-remote'], true)) {
                            file_put_contents($this->directory.'/processes', json_encode([
                                $context['relay_pid'], $context['callback_pid'],
                            ], JSON_THROW_ON_ERROR));
                        }
                        if ($blockCleanup && $event === 'worker.activity_process_started'
                            && ($context['activity_type'] ?? null) === 'tests.php-cooperative-cleanup') {
                            file_put_contents($this->directory.'/cleanup-processes', json_encode([
                                $context['relay_pid'], $context['callback_pid'],
                            ], JSON_THROW_ON_ERROR));
                        }
                        if ($event === 'worker.activity_process_started'
                            && preg_match('/^tests\.php-cooperative-group-(work|cleanup)-([01])$/', (string) ($context['activity_type'] ?? ''), $member)) {
                            $file = $member[1] === 'work' ? 'group-processes-' : 'group-cleanup-processes-';
                            file_put_contents($this->directory.'/'.$file.$member[2], json_encode([
                                $context['relay_pid'], $context['callback_pid'],
                            ], JSON_THROW_ON_ERROR));
                        }
                        $exception = $context['exception'] ?? null;
                        if ($event === 'worker.retrying'
                            && $exception instanceof \DurableWorkflow\Exception\ServerException
                            && $exception->status === 429 && $exception->reason === 'long_poll_capacity_exhausted'
                            && ($exception->details['poll_status'] ?? null) === 'long_poll_capacity_exhausted'
                            && array_key_exists('task', $exception->details ?? []) && $exception->details['task'] === null
                            && ($exception->details['retryable'] ?? null) === true
                            && is_int($exception->details['retry_after_seconds'] ?? null)
                            && $exception->details['retry_after_seconds'] > 0) {
                            fwrite(STDOUT, 'Connected admitted poll retry: '.json_encode([
                                'operation' => $context['operation'], 'reason' => $exception->reason,
                                'delay_seconds' => $context['delay_seconds'],
                            ], JSON_THROW_ON_ERROR)."\n");
                            return;
                        }
                        if (!$failureReported && ($context['exception'] ?? null) instanceof Throwable) {
                            $failureReported = true;
                            $error = $context['exception'];
                            $causes = [];
                            do {
                                $causes[] = $error::class.': '.$error->getMessage();
                                $error = $error->getPrevious();
                            } while ($error !== null && count($causes) < 4);
                            fwrite(STDERR, 'Connected worker failure: '.implode(' | ', $causes)."\n");
                            $notify('worker-failure: '.implode(' | ', $causes));
                        }
                    });
                if (!$remoteRole) {
                    $worker->registerWorkflow('tests.php-cooperative', static function (WorkflowContext $context, string $kind) use ($queue, $preparedLocal): string {
                        try {
                            if ($kind === 'child-wait') {
                                $context->childWorkflow('tests.php-cooperative', ['timer'], [
                                    'task_queue' => $queue,
                                    'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
                                    'parent_close_policy' => ParentClosePolicy::RequestCancellation,
                                ]);
                            } elseif ($kind === 'local') {
                                $context->localActivity('tests.php-cooperative-work');
                            } elseif ($kind === 'local-group') {
                                $context->all([
                                    static fn () => $context->localActivity('tests.php-cooperative-group-work-0'),
                                    static fn () => $context->localActivity('tests.php-cooperative-group-work-1'),
                                ]);
                            } elseif ($kind === 'remote') {
                                $context->activity('tests.php-cooperative-remote');
                            } else {
                                $context->sleep(300);
                            }
                        } catch (WorkflowCancelled $error) {
                            if ($kind === 'local-group') {
                                $context->cancellationShield(static fn () => $context->all([
                                    static fn () => $context->localActivity('tests.php-cooperative-group-cleanup-0',
                                        [$error->requestId, $context->cancellationContext()?->toArray()], ['retry_policy' => ['max_attempts' => 3]]),
                                    static fn () => $context->localActivity('tests.php-cooperative-group-cleanup-1',
                                        [$error->requestId, $context->cancellationContext()?->toArray()], ['retry_policy' => ['max_attempts' => 3]]),
                                ]));
                                return (string) $error->requestId;
                            }
                            $context->cancellationShield(static fn () => $context->localActivity('tests.php-cooperative-cleanup',
                                [$error->requestId, $context->cancellationContext()?->toArray()],
                                $preparedLocal ? ['retry_policy' => ['max_attempts' => 3]] : []));
                            return (string) $error->requestId;
                        }
                        return 'not-cancelled';
                    });
                    $worker->registerWorkflow('tests.php-cooperative-payload', static fn (WorkflowContext $context, string $value): string => $value);
                }
                if ($remoteRole) {
                    $worker->registerActivity('tests.php-cooperative-remote', function (ActivityContext $context) use ($notify, $userHeartbeat, $remoteBlockSeconds): \stdClass {
                        file_put_contents($this->directory.'/remote-fence', json_encode([
                            'task_id' => $context->taskId, 'activity_attempt_id' => $context->activityAttemptId,
                            'lease_owner' => $context->leaseOwner,
                            'attempt_number' => $context->attemptNumber,
                        ], JSON_THROW_ON_ERROR));
                        $notify('remote-entered');
                        $deadline = microtime(true) + $remoteBlockSeconds;
                        while (microtime(true) < $deadline) {
                            usleep(100_000);
                            if ($userHeartbeat) { $context->heartbeat(['qualification' => 'remote-in-flight']); }
                        }
                        file_put_contents($this->directory.'/late', 'remote callback returned');
                        return new \stdClass();
                    });
                }
                $worker->registerActivity('tests.php-cooperative-work', function (ActivityContext $context) use ($notify, $userHeartbeat): \stdClass {
                    $notify('local-entered');
                    $deadline = microtime(true) + 60;
                    while (microtime(true) < $deadline) {
                        usleep(100_000);
                        if ($userHeartbeat) {
                            $context->heartbeat(['qualification' => 'local-in-flight']);
                        }
                    }
                    file_put_contents($this->directory.'/late', 'late callback returned');
                    // Encoding this value would fail. Cancellation must discard it first.
                    return new \stdClass();
                });
                $worker->registerActivity('tests.php-cooperative-cleanup', function (ActivityContext $context, string $requestId, ?array $cancellationContext = null) use ($notify, $blockCleanup): string {
                    if ($cancellationContext !== null) {
                        file_put_contents($this->directory.'/context-'.$requestId, json_encode($cancellationContext, JSON_THROW_ON_ERROR));
                    }
                    if ($blockCleanup) {
                        $notify('cleanup-entered');
                        sleep(60);
                        file_put_contents($this->directory.'/cleanup-returned', 'late cleanup returned');
                    }
                    $context->heartbeat(['request_id' => $requestId]);
                    return $requestId;
                });
                foreach ([0, 1] as $index) {
                    $worker->registerActivity('tests.php-cooperative-group-work-'.$index, function (ActivityContext $context) use ($notify, $index): string {
                        $notify('group-entered-'.$index);
                        sleep(60);
                        file_put_contents($this->directory.'/group-late-'.$index, 'stale callback returned');
                        return 'stale';
                    });
                    $worker->registerActivity('tests.php-cooperative-group-cleanup-'.$index,
                        function (ActivityContext $context, string $requestId, ?array $cancellationContext) use ($notify, $blockCleanup, $index): string {
                            file_put_contents($this->directory.'/context-'.$requestId.'-'.$index, json_encode($cancellationContext, JSON_THROW_ON_ERROR));
                            if ($blockCleanup) {
                                $notify('group-cleanup-entered-'.$index);
                                sleep(60);
                                file_put_contents($this->directory.'/group-cleanup-late-'.$index, 'stale cleanup returned');
                            }
                            return $requestId;
                        });
                }
                $worker->run(1);
                fclose($child);
                exit(0);
            } catch (Throwable $error) {
                $notify('error:'.$error::class.':'.$error->getMessage());
                fclose($child);
                exit(1);
            }
        }
        fclose($child);
        $this->workerProcesses[$pid] = true;
        return [$pid, $parent];
    }

    /** @param resource $messages */
    private function awaitMessage($messages, string $expected, int $timeoutSeconds = 15): void
    {
        stream_set_timeout($messages, $timeoutSeconds);
        self::assertSame($expected, trim((string) fgets($messages)), 'Unexpected worker observation.');
    }

    /** @param resource $messages
     * @param list<string> $expected
     */
    private function awaitMessages($messages, array $expected): void
    {
        foreach ($expected as $_) {
            stream_set_timeout($messages, 15);
            $message = trim((string) fgets($messages));
            self::assertContains($message, $expected, 'Unexpected group worker observation.');
            $expected = array_values(array_diff($expected, [$message]));
        }
        self::assertSame([], $expected);
    }

    /** @param resource $messages */
    private function awaitReclaimedActivity($messages): void
    {
        $deadline = microtime(true) + 330;
        $registeredHeartbeat = false;
        while (microtime(true) < $deadline) {
            stream_set_timeout($messages, max(1, (int) ceil($deadline - microtime(true))));
            $message = trim((string) fgets($messages));
            if ($message === 'owner-heartbeat') {
                $registeredHeartbeat = true;
                continue;
            }
            self::assertSame('remote-entered', $message, 'Unexpected reclaim observation.');
            self::assertTrue($registeredHeartbeat, 'Replacement registration must remain alive while awaiting lease expiry.');
            return;
        }
        self::fail('Actual activity lease and repair did not produce a replacement within 330 seconds.');
    }

    private function stopWorker(int $pid, bool $kill = false): void
    {
        if ($pid <= 0 || !isset($this->workerProcesses[$pid])) {
            return;
        }
        posix_kill($pid, $kill ? SIGKILL : SIGTERM);
        $deadline = microtime(true) + 10;
        do {
            $result = pcntl_waitpid($pid, $status, WNOHANG);
            if ($result === $pid) {
                unset($this->workerProcesses[$pid]);
                if ($kill) {
                    self::assertTrue(pcntl_wifsignaled($status));
                    self::assertSame(SIGKILL, pcntl_wtermsig($status));
                } else {
                    self::assertTrue(pcntl_wifexited($status));
                    self::assertSame(0, pcntl_wexitstatus($status), 'Cooperative worker failed.');
                }
                return;
            }
            usleep(50_000);
        } while (microtime(true) < $deadline);
        posix_kill($pid, SIGKILL);
        pcntl_waitpid($pid, $status);
        unset($this->workerProcesses[$pid]);
        self::fail('Cooperative worker did not stop after its shutdown request.');
    }

    private function assertProcessStops(int $pid): void
    {
        $deadline = microtime(true) + 3;
        do {
            if (!posix_kill($pid, 0)) { self::assertFalse(posix_kill($pid, 0)); return; }
            usleep(10_000);
        } while (microtime(true) < $deadline);
        self::fail('Activity process survived cancellation: '.$pid);
    }

    /** @return list<array<string, mixed>> */
    private function history(Client $client, WorkflowHandle $handle): array
    {
        $events = [];
        $token = null;
        $seen = [];
        for ($page = 0; $page < 50; $page++) {
            $history = $client->workflowHistory(
                $handle->workflowId,
                (string) $handle->selectedRunId,
                pageSize: 100,
                nextPageToken: $token,
            );
            $batch = $history['events'] ?? $history['history_events'] ?? [];
            self::assertIsArray($batch);
            $events = array_merge($events, $batch);
            $next = $history['next_page_token'] ?? null;
            if ($next === null) {
                return $events;
            }
            self::assertIsString($next);
            self::assertNotSame('', $next);
            self::assertNotContains($next, $seen, 'History repeated a page token.');
            self::assertNotEmpty($batch, 'A continuation must advance history.');
            $seen[] = $next;
            $token = $next;
        }
        self::fail('Connected scenario history exceeded its 50-page bound.');
    }

    private function awaitEvent(Client $client, WorkflowHandle $handle, string $kind): void
    {
        $deadline = microtime(true) + 20;
        do {
            if (in_array($kind, array_column($this->history($client, $handle), 'event_type'), true)) {
                return;
            }
            usleep(100_000);
        } while (microtime(true) < $deadline);
        throw new RuntimeException('Workflow did not record '.$kind.'.');
    }

    /** @param resource $messages
     *  @return list<array<string, mixed>>
     */
    private function assertCancelledCleanup(Client $client, WorkflowHandle $handle, string $requestId, $messages, ?\Closure $observe = null, int $expectedCleanupCount = 1): array
    {
        stream_set_blocking($messages, false);
        $deadline = microtime(true) + 30;
        do {
            if ($observe !== null) { $observe(); }
            $message = fgets($messages);
            if (is_string($message) && trim($message) !== '') {
                self::fail(trim($message));
            }
            $status = strtolower((string) $handle->describe()->status);
            if (in_array($status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                break;
            }
            usleep(100_000);
        } while (microtime(true) < $deadline);
        self::assertSame('cancelled', $status, 'Workflow did not finish its bounded cooperative cleanup.');
        try {
            $handle->result(1, 0.1);
            self::fail('A cooperatively cancelled run must retain its cancelled result.');
        } catch (WorkflowCancelled) {
        }
        $events = $this->history($client, $handle);
        $kinds = array_column($events, 'event_type');
        foreach (['CooperativeCancellationRequested', 'CooperativeCancellationDelivered', 'WorkflowCancelled'] as $kind) {
            self::assertSame(1, count(array_filter($kinds, static fn (string $value): bool => $value === $kind)), $kind);
        }
        self::assertSame($expectedCleanupCount, count(array_filter($kinds, static fn (string $value): bool => $value === 'ActivityCompleted')));
        foreach (['WorkflowCompleted', 'WorkflowFailed', 'ActivityFailed', 'ActivityTimedOut'] as $kind) {
            self::assertNotContains($kind, $kinds);
        }
        foreach ($events as $event) {
            if (in_array($event['event_type'], ['CooperativeCancellationRequested', 'CooperativeCancellationDelivered', 'WorkflowCancelled'], true)) {
                self::assertSame($requestId, $event['payload']['workflow_command_id']);
            }
            if ($event['event_type'] === 'ActivityCompleted') {
                self::assertSame($requestId, (new AvroPayloadCodec())->decodeEnvelope($event['payload']['result']));
            }
        }
        return $events;
    }
}

/** Records real delivery replies and pauses only after Server releases a child-wait claim. */
final class ChildWaitObservationTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    private readonly Psr18Transport $inner;

    public function __construct(private readonly string $directory, private readonly \Closure $notify)
    {
        $this->inner = new Psr18Transport();
    }

    public function supportsBoundedRequests(): bool { return $this->inner->supportsBoundedRequests(); }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->observe($uri, $body, $this->inner->send($method, $uri, $headers, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->observe($uri, $body, $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    private function observe(string $uri, ?array $body, ?array $reply): ?array
    {
        if (!str_ends_with($uri, '/deliver-cancellation')) { return $reply; }
        file_put_contents($this->directory.'/child-wait-deliveries', json_encode([
            'worker_pid' => getmypid(), 'body' => $body, 'reply' => array_intersect_key($reply ?? [], array_flip([
                'delivered', 'task_id', 'workflow_run_id', 'request_id', 'sequence', 'call_kind',
                'sequence_span', 'operation_sequence', 'operation_sequence_span', 'reason', 'claim_released',
            ])),
        ], JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);
        if (($reply['claim_released'] ?? false) === true) {
            ($this->notify)('child-wait-released');
            $deadline = microtime(true) + 10;
            while (!is_file($this->directory.'/continue-child-wait') && microtime(true) < $deadline) { usleep(10_000); }
            if (!is_file($this->directory.'/continue-child-wait')) {
                throw new RuntimeException('The connected child-wait observation was not released.');
            }
        }
        return $reply;
    }
}

/** Discards one successful Server response after its real durable commit. */
final class DiscardFirstDeliveryReplyTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    private Psr18Transport $inner;
    private bool $discarded = false;

    public function __construct(private readonly \Closure $notify)
    {
        $this->inner = new Psr18Transport();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->maybeDiscard($uri, $this->inner->send($method, $uri, $headers, $body));
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->inner->supportsBoundedRequests();
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->maybeDiscard($uri, $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    private function maybeDiscard(string $uri, ?array $response): ?array
    {
        if (!$this->discarded && str_ends_with($uri, '/deliver-cancellation')) {
            $this->discarded = true;
            ($this->notify)('delivery-reply-discarded');
            throw new TransportException('Qualification discarded a successful delivery reply.');
        }
        return $response;
    }
}

final class RemoteOwnerObservationTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    private readonly Psr18Transport $inner;
    private bool $notified = false;

    public function __construct(private readonly \Closure $notify) { $this->inner = new Psr18Transport(); }
    public function supportsBoundedRequests(): bool { return $this->inner->supportsBoundedRequests(); }
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->inner->send($method, $uri, $headers, $body);
    }
    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $reply = $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds);
        if (!$this->notified && str_ends_with($uri, '/worker/heartbeat') && ($reply['acknowledged'] ?? false) === true) {
            $this->notified = true;
            ($this->notify)('owner-heartbeat');
        }
        return $reply;
    }
}

/** Holds a real accepted poll reply before SDK replay so its owner can be killed. */
final class PauseWorkflowClaimTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    private readonly Psr18Transport $inner;
    private bool $paused = false;

    public function __construct(private readonly string $directory, private readonly \Closure $notify)
    {
        $this->inner = new Psr18Transport();
    }

    public function supportsBoundedRequests(): bool { return $this->inner->supportsBoundedRequests(); }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->pause($uri, $this->inner->send($method, $uri, $headers, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->pause($uri, $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    private function pause(string $uri, ?array $response): ?array
    {
        clearstatcache();
        $gate = $this->directory.'/pause-workflow-claim';
        if (!$this->paused && str_ends_with($uri, '/workflow-tasks/poll')
            && is_array($response['task'] ?? null) && is_file($gate)) {
            $this->paused = true;
            file_put_contents($this->directory.'/held-workflow-claim', json_encode(array_intersect_key($response['task'],
                ['task_id' => true, 'lease_owner' => true, 'workflow_task_attempt' => true, 'lease_expires_at' => true]), JSON_THROW_ON_ERROR));
            ($this->notify)('workflow-claim-held');
            $deadline = microtime(true) + 15;
            while (is_file($gate)) {
                if (microtime(true) >= $deadline) { throw new RuntimeException('Workflow claim fixture was not stopped.'); }
                usleep(50_000);
                clearstatcache();
            }
        }
        return $response;
    }
}
