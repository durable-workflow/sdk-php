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

    /** @var array<int, resource> */
    private array $externalProcesses = [];

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
        foreach ($this->externalProcesses as $process) {
            try { $this->stopExternalWorker($process); }
            catch (Throwable $error) { $cleanupFailure ??= $error; }
        }
        if (isset($this->directory)) {
            $artifactDirectory = getenv('DURABLE_WORKFLOW_POLYGLOT_ARTIFACT_DIRECTORY');
            if (is_string($artifactDirectory) && $artifactDirectory !== '' && $this->externalProcesses === []) {
                $target = $artifactDirectory.'/'.basename($this->directory);
                if (!is_dir($target)) { mkdir($target, 0700, true); }
                foreach (glob($this->directory.'/*') ?: [] as $file) { copy($file, $target.'/'.basename($file)); }
            }
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
        foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
            self::assertSame($accepted['cancellation_request'][$field], $repeated['cancellation_request'][$field]);
        }
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

    /** @return array<string, array{?CancellationPolicy}> */
    public static function localCancellationPolicies(): array
    {
        return [
            'historical_omission' => [null],
            'try_cancel' => [CancellationPolicy::TryCancel],
            'wait_cancellation_completed' => [CancellationPolicy::WaitCancellationCompleted],
        ];
    }

    #[DataProvider('localCancellationPolicies')]
    public function testPreparedCleanupResumesAfterWorkerSigkillWithinOriginalThirtySeconds(?CancellationPolicy $localPolicy): void
    {
        $this->requirePreparedLocalSource();
        $queue = $this->queue('prepared-cleanup-kill');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, blockCleanup: true, preparedLocal: true, localPolicy: $localPolicy);
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
            [$pid, $messages] = $this->spawnWorker($queue, preparedLocal: true, localPolicy: $localPolicy);
            $this->awaitMessage($messages, 'registered');
            $events = $this->assertCancelledCleanup($client, $handle, $request['request_id'], $messages);
            self::assertLessThan((float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u'), microtime(true));
            self::assertFileDoesNotExist($this->directory.'/cleanup-returned');
            $afterDelivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'));
            self::assertSame([$delivery], $afterDelivery, 'Replacement changed the original canonical delivery boundary.');
            if ($localPolicy !== null) {
                $scheduled = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityScheduled'))[0];
                self::assertSame($localPolicy->value, $scheduled['payload']['activity']['cancellation_policy']);
                if ($localPolicy === CancellationPolicy::WaitCancellationCompleted) {
                    $receipt = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged'))[0];
                    self::assertLessThan($delivery['sequence'], $receipt['sequence'], 'Local Wait must prove the stop before delivery.');
                }
            }
            $recovery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityRetryScheduled'));
            self::assertCount(1, $recovery);
            self::assertSame('unknown', $recovery[0]['payload']['local_recovery']['callback_stop_state']);
            $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
            self::assertTrue($repeated['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($request[$field], $repeated['cancellation_request'][$field]);
            }
            fwrite(STDOUT, 'Prepared cleanup source SIGKILL recovery: '.json_encode([
                'local_cancellation_policy' => $localPolicy?->value,
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

    public function testPolyglotCascadeRecoversPhpCleanupWithinOriginalThirtySeconds(): void
    {
        if (getenv('DURABLE_WORKFLOW_POLYGLOT_QUALIFICATION') !== '1') {
            self::markTestSkipped('The mixed cascade requires exact Python and Rust Source candidates.');
        }
        $this->requirePreparedLocalSource();
        $queue = $this->queue('polyglot');
        $client = $this->client();
        $this->spawnExternalWorker('python', $queue.'-python');
        $this->spawnExternalWorker('rust', $queue.'-rust');
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, blockCleanup: true, preparedLocal: true, localPolicy: CancellationPolicy::WaitCancellationCompleted);
        $parent = null;
        $child = null;
        try {
            $this->awaitMessage($messages, 'registered');
            $parent = $client->startWorkflow('tests.php-cooperative', $queue, $queue,
                ['polyglot', $queue.'-python', $queue.'-rust']);
            $this->awaitMessage($messages, 'local-entered');
            $this->awaitEvent($client, $parent, 'ChildWorkflowScheduled');
            $scheduled = array_values(array_filter($this->history($client, $parent),
                static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'))[0];
            $child = new WorkflowHandle($client, $scheduled['payload']['child_workflow_instance_id'],
                $scheduled['payload']['child_workflow_run_id']);
            self::assertSame($queue.'-python', $child->describe()->taskQueue);
            $this->awaitObservationFile('rust-entered.json');
            $grant = json_decode((string) file_get_contents($this->directory.'/rust-entered.json'), true, flags: JSON_THROW_ON_ERROR);
            $localGrant = json_decode((string) file_get_contents($this->directory.'/local-fence'), true, flags: JSON_THROW_ON_ERROR);
            $localPids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $accepted = $parent->requestSelectedRunCancellation('mixed-language recovery qualification', 30);
            $request = $accepted['cancellation_request'];
            self::assertEquals((new \DateTimeImmutable($request['requested_at']))->modify('+30 seconds'),
                new \DateTimeImmutable($request['cleanup_deadline_at']));
            $this->awaitMessage($messages, 'cleanup-entered');
            $before = $this->history($client, $parent);
            $delivery = array_values(array_filter($before,
                static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            self::assertSame(2, $delivery['payload']['sequence_span']);
            foreach ($localPids as $activityPid) { $this->assertProcessStops($activityPid); }
            $this->awaitObservationFile('rust-stopped.json');
            self::assertSame($grant, json_decode((string) file_get_contents($this->directory.'/rust-stopped.json'), true, flags: JSON_THROW_ON_ERROR));
            $cleaning = $this->cascade($client, $parent, $parent, $child, $request, 'cascade-during-cleanup');
            $cleaningRuns = array_column($cleaning['runs'], null, 'run_id');
            self::assertSame('cleaning_up', $cleaningRuns[$parent->selectedRunId]['lifecycle']);
            self::assertSame($delivery['payload']['sequence'], $cleaningRuns[$parent->selectedRunId]['delivery']['sequence']);
            self::assertSame($delivery['payload']['sequence_span'], $cleaningRuns[$parent->selectedRunId]['delivery']['sequence_span']);
            self::assertSame('cancelled', $cleaningRuns[$child->selectedRunId]['lifecycle']);
            $cleanupPids = json_decode((string) file_get_contents($this->directory.'/cleanup-processes'), true, flags: JSON_THROW_ON_ERROR);
            $firstBudget = json_decode((string) file_get_contents($this->directory.'/remaining-delivery-'.$pid.'.json'), true, flags: JSON_THROW_ON_ERROR);
            $cleanupDiagnostics = $client->workflowDiagnostics($parent->workflowId, (string) $parent->selectedRunId);
            $cleanupClaims = array_values(array_filter($cleanupDiagnostics['pending_workflow_tasks'],
                static fn (array $task): bool => $task['status'] === 'leased'));
            self::assertCount(1, $cleanupClaims, 'SIGKILL must interrupt an active cleanup claim.');
            $cleanupClaim = $cleanupClaims[0];
            self::assertSame($queue.'-'.$pid, $cleanupClaim['lease_owner']);
            self::assertFalse($cleanupClaim['lease_expired']);
            self::assertGreaterThan(microtime(true),
                (float) (new \DateTimeImmutable($cleanupClaim['lease_expires_at']))->format('U.u'));
            file_put_contents($this->directory.'/cleanup-claim-before-sigkill.json', json_encode([
                'observed_at' => microtime(true), 'worker_pid' => $pid,
                'claim' => $cleanupClaim, 'cancellation_request' => $request,
            ], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
            $this->stopWorker($pid, true);
            $pid = 0;
            foreach ($cleanupPids as $activityPid) { $this->assertProcessStops($activityPid); }
            fclose($messages);
            [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: false, preparedLocal: true, localPolicy: CancellationPolicy::WaitCancellationCompleted);
            $this->awaitMessage($messages, 'registered');
            $events = $this->assertCancelledCleanup($client, $parent, $request['request_id'], $messages);
            self::assertSame('cancelled', strtolower((string) $child->describe()->status));
            self::assertLessThan((float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u'), microtime(true));
            $childEvents = $this->history($client, $child);
            $this->awaitObservationFile('python-cleanup.json');
            $childContext = json_decode((string) file_get_contents($this->directory.'/python-cleanup.json'), true, flags: JSON_THROW_ON_ERROR);
            $rootContext = $delivery['payload']['cancellation'];
            self::assertSame($request['request_id'], $rootContext['root_request_id']);
            self::assertSame($request['request_id'], $childContext['root_request_id']);
            self::assertSame($request['request_id'], $childContext['parent_request_id']);
            self::assertNotSame($request['request_id'], $childContext['request_id']);
            foreach (['root_workflow_instance_id', 'root_workflow_run_id', 'reason', 'requester', 'source'] as $field) {
                self::assertSame($rootContext[$field], $childContext[$field], $field);
            }
            foreach (['requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertEquals(new \DateTimeImmutable($rootContext[$field]), new \DateTimeImmutable($childContext[$field]));
            }
            self::assertCount(2, $childContext['lineage']);
            $childDelivery = array_values(array_filter($childEvents,
                static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'));
            self::assertCount(1, $childDelivery);
            self::assertEquals($childContext, $childDelivery[0]['payload']['cancellation']);
            $remoteReceipt = array_values(array_filter($childEvents,
                static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged'));
            self::assertCount(1, $remoteReceipt);
            foreach (['activity_attempt_id', 'lease_owner'] as $field) {
                self::assertSame($grant[$field], $remoteReceipt[0]['payload'][$field], $field);
            }
            $remoteStatus = $client->activityTaskStatus($grant['task_id'], $grant['activity_attempt_id'], $grant['lease_owner']);
            self::assertSame($grant['task_id'], $remoteStatus['task_id']);
            self::assertFalse($remoteStatus['can_continue']);
            self::assertSame($childContext['request_id'], $remoteReceipt[0]['payload']['request_id']);
            self::assertSame($request['request_id'], $remoteReceipt[0]['payload']['root_request_id']);
            self::assertEquals(new \DateTimeImmutable($request['cleanup_deadline_at']),
                new \DateTimeImmutable($remoteReceipt[0]['payload']['cleanup_deadline_at']));
            self::assertSame('activity_worker', $remoteReceipt[0]['payload']['evidence_source']);
            self::assertSame('stopped', $remoteReceipt[0]['payload']['callback_state']);
            self::assertFalse($remoteReceipt[0]['payload']['received_after_deadline']);
            self::assertLessThan($childDelivery[0]['sequence'], $remoteReceipt[0]['sequence'], 'Wait must prove the stop before delivery.');
            $localReceipt = array_values(array_filter($events,
                static fn (array $event): bool => $event['event_type'] === 'ActivityCancellationAcknowledged'));
            self::assertCount(1, $localReceipt);
            self::assertSame($localGrant['activity_attempt_id'], $localReceipt[0]['payload']['activity_attempt_id']);
            self::assertSame($request['request_id'], $localReceipt[0]['payload']['root_request_id']);
            self::assertLessThan($delivery['sequence'], $localReceipt[0]['sequence'], 'Local Wait must prove the stop before delivery.');
            $localScheduled = array_values(array_filter($events, static fn (array $event): bool =>
                $event['event_type'] === 'ActivityScheduled' && ($event['payload']['activity_type'] ?? null) === 'tests.php-cooperative-work'))[0];
            self::assertSame(CancellationPolicy::WaitCancellationCompleted->value, $localScheduled['payload']['activity']['cancellation_policy']);
            self::assertSame([$delivery], array_values(array_filter($events,
                static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered')));
            $recoveries = array_values(array_filter($events,
                static fn (array $event): bool => $event['event_type'] === 'ActivityRetryScheduled'));
            self::assertCount(1, $recoveries);
            self::assertSame('unknown', $recoveries[0]['payload']['local_recovery']['callback_stop_state']);
            self::assertSame(1, count(array_filter($events,
                static fn (array $event): bool => $event['event_type'] === 'ActivityHeartbeatRecorded')),
                'Only the replacement cleanup callback emits an application heartbeat.');
            foreach ([$events, $childEvents] as $history) {
                self::assertSame(1, count(array_filter($history,
                    static fn (array $event): bool => $event['event_type'] === 'WorkflowCancelled')));
                foreach (['WorkflowCompleted', 'WorkflowFailed', 'WorkflowTerminated'] as $kind) {
                    self::assertNotContains($kind, array_column($history, 'event_type'));
                }
            }
            foreach ([[$events, $localGrant], [$childEvents, $grant]] as [$history, $initialGrant]) {
                foreach ($history as $event) {
                    if ($event['event_type'] === 'ActivityHeartbeatRecorded') {
                        self::assertNotSame($initialGrant['activity_attempt_id'], $event['payload']['activity_attempt_id']);
                    }
                }
            }
            $duplicate = $parent->requestSelectedRunCancellation('must retain the original request', 300);
            self::assertTrue($duplicate['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($request[$field], $duplicate['cancellation_request'][$field]);
            }
            foreach (['complete', 'fail'] as $outcome) {
                try {
                    if ($outcome === 'complete') { $client->completeActivityTask($grant['task_id'], $grant['activity_attempt_id'], $grant['lease_owner'], 'stale'); }
                    else { $client->failActivityTask($grant['task_id'], $grant['activity_attempt_id'], $grant['lease_owner'], 'stale', 'LateQualification'); }
                    self::fail('The stopped Rust attempt cannot publish '.$outcome.'.');
                } catch (\DurableWorkflow\Exception\ServerException $error) { self::assertSame(409, $error->status); }
            }
            self::assertSame($events, $this->history($client, $parent));
            self::assertSame($childEvents, $this->history($client, $child));
            self::assertFileDoesNotExist($this->directory.'/late');
            self::assertFileDoesNotExist($this->directory.'/cleanup-returned');
            $finished = $this->cascade($client, $parent, $parent, $child, $request, 'cascade-final');
            $fromChild = $this->cascade($client, $child, $parent, $child, $request, 'cascade-child-final');
            $finishedFromChild = $finished;
            $finishedFromChild['selected_run_id'] = $child->selectedRunId;
            self::assertSame($finishedFromChild, $fromChild, 'Both selected runs must explain the same cascade.');
            $finishedRuns = array_column($finished['runs'], null, 'run_id');
            foreach ([$parent, $child] as $handle) {
                $node = $finishedRuns[$handle->selectedRunId];
                self::assertSame('cancelled', $node['lifecycle']);
                self::assertSame('cancelled', $node['projected_status']);
                self::assertSame('WorkflowCancelled', $node['terminal_event_type']);
                self::assertSame('completed', $node['cleanup']['outcome']);
                self::assertSame($request['cleanup_deadline_at'], $node['cleanup']['cleanup_deadline_at']);
                self::assertLessThan(new \DateTimeImmutable($request['cleanup_deadline_at']),
                    new \DateTimeImmutable($node['cleanup']['finished_at']));
                self::assertCount(1, $node['activity_stops']);
                self::assertSame('reported_stopped', $node['activity_stops'][0]['callback_state']);
                self::assertFalse($node['activity_stops'][0]['received_after_deadline']);
            }
            $parentNode = $finishedRuns[$parent->selectedRunId];
            self::assertSame($cleaningRuns[$parent->selectedRunId]['delivery'], $parentNode['delivery']);
            self::assertSame($localGrant['activity_attempt_id'], $parentNode['activity_stops'][0]['activity_attempt_id']);
            self::assertSame('local', $parentNode['activity_stops'][0]['execution_mode']);
            self::assertSame($grant['activity_attempt_id'], $finishedRuns[$child->selectedRunId]['activity_stops'][0]['activity_attempt_id']);
            self::assertSame('remote', $finishedRuns[$child->selectedRunId]['activity_stops'][0]['execution_mode']);
            self::assertCount(1, $parentNode['cleanup_recovery']);
            foreach ($parentNode['cleanup_recovery'][0]['attempt'] as $field => $value) {
                self::assertSame($recoveries[0]['payload']['local_recovery'][$field], $value, $field);
            }
            self::assertSame('unknown', $parentNode['cleanup_recovery'][0]['attempt']['callback_stop_state']);
            self::assertNotSame($parentNode['cleanup_recovery'][0]['attempt']['original_lease_owner'],
                $parentNode['cleanup_recovery'][0]['attempt']['lease_owner']);
            self::assertSame('wait_cancellation_completed', $parentNode['child_propagation'][0]['policy']);
            $budget = json_decode((string) file_get_contents($this->directory.'/remaining-cleanup-final.json'), true, flags: JSON_THROW_ON_ERROR);
            self::assertNotSame($firstBudget['worker_pid'], $budget['worker_pid']);
            self::assertSame($firstBudget['at_delivery'], $budget['at_delivery'], 'Replacement replay keeps the original remaining-time decision.');
            self::assertGreaterThan(0, $budget['after_cleanup']);
            self::assertLessThan($budget['at_delivery'], $budget['after_cleanup']);
            $deadline = (float) (new \DateTimeImmutable($request['cleanup_deadline_at']))->format('U.u');
            self::assertEqualsWithDelta($deadline - (float) (new \DateTimeImmutable($delivery['timestamp']))->format('U.u'),
                $budget['at_delivery'], 0.000001);
            $cleanupEvent = array_values(array_filter($events, static fn (array $event): bool =>
                $event['event_type'] === 'ActivityCompleted' && ($event['payload']['activity_type'] ?? null) === 'tests.php-cooperative-cleanup'))[0];
            self::assertEqualsWithDelta($deadline - (float) (new \DateTimeImmutable($cleanupEvent['timestamp']))->format('U.u'),
                $budget['after_cleanup'], 0.000001);
            $this->assertCascadeCli($parent, $finished);
            fwrite(STDOUT, 'Mixed-language Source cascade: '.json_encode([
                'root_request' => $request, 'child_context' => $childContext,
                'initial_work_application_heartbeats' => false, 'cleanup_application_heartbeats' => 1,
                'worker_sigkill_during_cleanup' => true,
                'local_cancellation_policy' => CancellationPolicy::WaitCancellationCompleted->value,
                'rust_callback_grant' => $grant, 'php_local_grant' => $localGrant,
                'joined_local_pids' => $localPids, 'killed_cleanup_pids' => $cleanupPids,
                'parent_history' => $events, 'child_history' => $childEvents,
                'cancellation_cascade' => $finished,
                'remaining_time' => ['original_worker' => $firstBudget, 'replacement_worker' => $budget],
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            foreach (['parent' => $parent, 'child' => $child] as $role => $handle) {
                if ($handle !== null) { $this->retainRunEvidence($client, $handle, $role); }
            }
            if (is_resource($messages)) { fclose($messages); }
            $this->stopWorker($pid);
        }
    }

    /** @param array<string, mixed> $request @return array<string, mixed> */
    private function cascade(Client $client, WorkflowHandle $selected, WorkflowHandle $parent, WorkflowHandle $child, array $request, string $artifact): array
    {
        $diagnostics = $client->workflowDiagnostics($selected->workflowId, (string) $selected->selectedRunId);
        file_put_contents($this->directory.'/'.$artifact.'.json', json_encode($diagnostics, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
        self::assertTrue($diagnostics['cancellation_cascade_supported'] ?? false);
        $view = $diagnostics['cancellation_cascade'];
        self::assertSame('durable-workflow.cancellation-cascade/v1', $view['schema']);
        self::assertSame($selected->selectedRunId, $view['selected_run_id']);
        self::assertTrue($view['inspection_complete'], json_encode($view['findings'], JSON_THROW_ON_ERROR));
        self::assertFalse($view['truncated']);
        self::assertSame($request['request_id'], $view['root']['root_request_id']);
        self::assertSame($request['requested_at'], $view['root']['requested_at']);
        self::assertSame($request['cleanup_deadline_at'], $view['root']['cleanup_deadline_at']);
        self::assertCount(2, $view['runs']);
        self::assertCount(1, $view['edges']);
        self::assertSame($parent->selectedRunId, $view['edges'][0]['parent_run_id']);
        self::assertSame($child->selectedRunId, $view['edges'][0]['child_run_id']);
        foreach ($view['runs'] as $node) {
            self::assertTrue($node['same_root_budget']);
            self::assertSame($request['request_id'], $node['request']['root_request_id']);
            self::assertSame($request['requested_at'], $node['request']['requested_at']);
            self::assertSame($request['cleanup_deadline_at'], $node['request']['cleanup_deadline_at']);
        }

        return $view;
    }

    /** @param array<string, mixed> $view */
    private function assertCascadeCli(WorkflowHandle $parent, array $view): void
    {
        $binary = getenv('DURABLE_WORKFLOW_CANCELLATION_CLI_BINARY');
        if (!is_string($binary) || $binary === '') {
            fwrite(STDOUT, "CLI inspection was not selected for this Source qualification.\n");
            return;
        }
        self::assertFileExists($binary);
        foreach (['json', 'table'] as $format) {
            $path = $this->directory.'/cascade-cli-'.$format;
            $environment = getenv();
            $environment['DURABLE_WORKFLOW_AUTH_TOKEN'] = $this->token;
            $process = proc_open([PHP_BINARY, $binary, 'debug', 'workflow', $parent->workflowId,
                '--run-id='.(string) $parent->selectedRunId, '--server='.$this->runtimeUrl,
                '--namespace=default', '--output='.$format, '--no-ansi'], [
                0 => ['file', '/dev/null', 'r'], 1 => ['file', $path, 'w'], 2 => ['file', $path.'.stderr', 'w'],
            ], $pipes, __DIR__, $environment);
            self::assertIsResource($process);
            try {
                $deadline = microtime(true) + 15;
                do {
                    $status = proc_get_status($process);
                    if (!$status['running']) { break; }
                    usleep(50_000);
                } while (microtime(true) < $deadline);
                self::assertFalse($status['running'], 'CLI diagnostics exceeded the bounded qualification timeout.');
                self::assertSame(0, $status['exitcode'], (string) file_get_contents($path.'.stderr'));
            } finally {
                if (proc_get_status($process)['running']) { proc_terminate($process, SIGKILL); }
                proc_close($process);
            }
            $text = (string) file_get_contents($path);
            if ($format === 'json') {
                self::assertSame($view, json_decode($text, true, flags: JSON_THROW_ON_ERROR)['cancellation_cascade']);
            } else {
                foreach ([$view['root']['root_request_id'], $view['root']['cleanup_deadline_at'],
                    'Cancellation Cascade:', 'phase=cancelled', 'Cleanup: completed',
                    'callback=reported_stopped', 'callback=unknown', 'policy=wait_cancellation_completed'] as $expected) {
                    self::assertStringContainsString($expected, $text);
                }
                foreach ($view['runs'] as $node) {
                    self::assertStringContainsString($node['run_id'], $text);
                    self::assertStringContainsString($node['activity_stops'][0]['activity_attempt_id'], $text);
                }
            }
        }
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
            $cleanupDeliveryId = null;
            if ($killDuringCleanup) {
                $this->awaitMessages($messages, ['group-cleanup-entered-0', 'group-cleanup-entered-1']);
                foreach ([0, 1] as $index) {
                    $killedPids = [...$killedPids, ...json_decode((string) file_get_contents($this->directory.'/group-cleanup-processes-'.$index), true, flags: JSON_THROW_ON_ERROR)];
                }
                $before = $this->history($client, $handle);
                $originalDelivery = array_values(array_filter($before, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
                $cleanupStarted = array_values(array_filter($before, static fn (array $event): bool => $event['event_type'] === 'ActivityStarted' && isset($event['payload']['local_preparation']['cancellation_cleanup'])));
                $cleanupDeliveryId = $cleanupStarted[0]['payload']['local_preparation']['cancellation_cleanup']['delivery_history_event_id'];
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
                self::assertIsString($cleanup['delivery_history_event_id']);
                self::assertNotSame('', $cleanup['delivery_history_event_id']);
                $cleanupDeliveryId ??= $cleanup['delivery_history_event_id'];
                self::assertSame($cleanupDeliveryId, $cleanup['delivery_history_event_id']);
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
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($request[$field], $duplicate['cancellation_request'][$field]);
            }

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
        return [[false, false], [true, false], [false, true], [true, true],
            [false, false, CancellationPolicy::TryCancel],
            [false, false, CancellationPolicy::WaitCancellationCompleted]];
    }

    #[DataProvider('remoteProvider')]
    public function testRemoteRequestStopsBlockedOwnerAfterLiveOrColdWorkflowDelivery(bool $userHeartbeat, bool $coldWorkflow, ?CancellationPolicy $policy = null): void
    {
        $queue = $this->queue('remote');
        $client = $this->client();
        [$workflowPid, $workflowMessages] = $this->spawnWorker($queue, pauseWorkflowClaim: $coldWorkflow, remotePolicy: $policy);
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
                [$workflowPid, $workflowMessages] = $this->spawnWorker($queue, remotePolicy: $policy);
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
            if ($policy !== null) {
                $scheduled = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityScheduled'))[0];
                self::assertSame($policy->value, $scheduled['payload']['activity']['cancellation_policy']);
            }
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
                if ($policy === CancellationPolicy::WaitCancellationCompleted) {
                    self::assertLessThan(
                        array_search('CooperativeCancellationDelivered', array_column($events, 'event_type'), true),
                        array_search('ActivityCancellationAcknowledged', array_column($events, 'event_type'), true),
                        'Wait must record physical stop before delivering cancellation to workflow code.',
                    );
                }
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

    public function testBoundedAbandonKeepsRemoteCallbackIndependentAfterParentCancellation(): void
    {
        $queue = $this->queue('remote-abandon');
        $client = $this->client();
        [$workflowPid, $workflowMessages] = $this->spawnWorker($queue, remotePolicy: CancellationPolicy::Abandon);
        [$ownerPid, $ownerMessages] = $this->spawnWorker($queue, userHeartbeat: false, remoteRole: true,
            remoteBlockSeconds: 25, remotePolicy: CancellationPolicy::Abandon);
        try {
            $this->awaitMessage($workflowMessages, 'registered');
            $this->awaitMessage($ownerMessages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['remote']);
            $this->awaitMessage($ownerMessages, 'remote-entered');
            $this->awaitMessage($ownerMessages, 'owner-heartbeat');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $fence = json_decode((string) file_get_contents($this->directory.'/remote-fence'), true, flags: JSON_THROW_ON_ERROR);
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 30);
            $duplicate = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
            self::assertTrue($duplicate['duplicate']);
            foreach (['request_id', 'requested_at', 'cleanup_deadline_at'] as $field) {
                self::assertSame($accepted['cancellation_request'][$field], $duplicate['cancellation_request'][$field]);
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $workflowMessages);
            foreach ($pids as $pid) { self::assertTrue(posix_kill($pid, 0), 'Abandon must preserve the independent callback after parent closure.'); }
            self::assertNotContains('ActivityCancelled', array_column($events, 'event_type'));
            self::assertNotContains('ActivityCancellationAcknowledged', array_column($events, 'event_type'));
            $scheduled = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityScheduled'))[0];
            self::assertSame('abandon', $scheduled['payload']['activity']['cancellation_policy']);
            $totalDeadline = $scheduled['payload']['activity']['schedule_to_close_deadline_at'];
            self::assertIsString($totalDeadline);
            $status = $client->activityTaskStatus($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner']);
            self::assertTrue($status['can_continue']);
            $until = microtime(true) + 35;
            do {
                $events = $this->history($client, $handle);
                $completed = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityCompleted'
                    && ($event['payload']['activity_type'] ?? null) === 'tests.php-cooperative-remote'));
                if ($completed !== []) { break; }
                usleep(100_000);
            } while (microtime(true) < $until);
            self::assertCount(1, $completed, 'The independent callback must durably complete within its original finite lifetime.');
            self::assertSame('independent-completion', (new AvroPayloadCodec())->decodeEnvelope($completed[0]['payload']['result']));
            self::assertSame($totalDeadline, $completed[0]['payload']['activity']['schedule_to_close_deadline_at']);
            self::assertSame($fence['activity_attempt_id'], $completed[0]['payload']['activity_attempt_id']);
            self::assertSame('cancelled', strtolower((string) $handle->describe()->status));
            self::assertNotContains('WorkflowCompleted', array_column($events, 'event_type'));
            self::assertNotContains('ActivityCancellationAcknowledged', array_column($events, 'event_type'));
            foreach (['complete', 'fail'] as $outcome) {
                try {
                    if ($outcome === 'complete') { $client->completeActivityTask($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner'], 'stale'); }
                    else { $client->failActivityTask($fence['task_id'], $fence['activity_attempt_id'], $fence['lease_owner'], 'stale', 'LateQualification'); }
                    self::fail('The completed independent attempt accepted another '.$outcome.'.');
                } catch (\DurableWorkflow\Exception\ServerException $error) { self::assertSame(409, $error->status); }
            }
            self::assertSame($events, $this->history($client, $handle));
            fwrite(STDOUT, 'Bounded remote Abandon: '.json_encode([
                'root_request_id' => $accepted['cancellation_request']['request_id'],
                'cleanup_deadline_at' => $accepted['cancellation_request']['cleanup_deadline_at'],
                'original_activity_deadline_at' => $totalDeadline,
                'callback_pids_preserved_after_parent_close' => $pids,
                'completion' => $completed[0],
            ], JSON_THROW_ON_ERROR)."\n");
        } finally {
            fclose($workflowMessages);
            fclose($ownerMessages);
            $this->stopWorker($workflowPid);
            $this->stopWorker($ownerPid);
        }
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
    private function spawnWorker(string $queue, bool $loseReply = false, bool $userHeartbeat = true, string $namespace = 'default', bool $remoteRole = false, bool $pauseWorkflowClaim = false, int $remoteBlockSeconds = 60, bool $blockCleanup = false, bool $observeChildWait = false, bool $preparedLocal = false, ?CancellationPolicy $remotePolicy = null, ?CancellationPolicy $localPolicy = null): array
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
                        if (in_array($event, ['worker.claim_aborted', 'worker.claim_deferred'], true)) {
                            fwrite(STDOUT, 'Connected claim diagnostic: '.json_encode(['event' => $event, 'context' => $context], JSON_THROW_ON_ERROR)."\n");
                        }
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
                    $directory = $this->directory;
                    $worker->registerWorkflow('tests.php-cooperative', static function (WorkflowContext $context, string $kind, ?string $childQueue = null, ?string $remoteQueue = null) use ($queue, $preparedLocal, $remotePolicy, $localPolicy, $directory): string {
                        try {
                            if ($kind === 'child-wait') {
                                $context->childWorkflow('tests.php-cooperative', ['timer'], [
                                    'queue' => $queue,
                                    'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
                                    'parent_close_policy' => ParentClosePolicy::RequestCancellation,
                                ]);
                            } elseif ($kind === 'polyglot') {
                                if (!$preparedLocal || $childQueue === null || $remoteQueue === null) { throw new RuntimeException('Mixed Source queues and preparation are required.'); }
                                $context->all([
                                    static fn () => $context->childWorkflow('tests.polyglot-cancellation-child', [$remoteQueue], [
                                        'queue' => $childQueue,
                                        'cancellation_policy' => CancellationPolicy::WaitCancellationCompleted,
                                        'parent_close_policy' => ParentClosePolicy::RequestCancellation,
                                    ]),
                                    static fn () => $context->localActivity('tests.php-cooperative-work', [], $localPolicy === null ? [] : ['cancellation_policy' => $localPolicy]),
                                ]);
                            } elseif ($kind === 'local') {
                                $context->localActivity('tests.php-cooperative-work', [], $localPolicy === null ? [] : ['cancellation_policy' => $localPolicy]);
                            } elseif ($kind === 'local-group') {
                                $context->all([
                                    static fn () => $context->localActivity('tests.php-cooperative-group-work-0'),
                                    static fn () => $context->localActivity('tests.php-cooperative-group-work-1'),
                                ]);
                            } elseif ($kind === 'remote') {
                                $context->activity('tests.php-cooperative-remote', [], $remotePolicy === null ? [] : [
                                    'cancellation_policy' => $remotePolicy,
                                    'schedule_to_close_timeout' => 60,
                                ]);
                            } else {
                                $context->sleep(300);
                            }
                        } catch (WorkflowCancelled $error) {
                            $remainingAtDelivery = null;
                            if ($kind === 'polyglot') {
                                $remainingAtDelivery = $error->context?->remaining();
                                if ($remainingAtDelivery === null) { throw new RuntimeException('Mixed cleanup requires its recorded cancellation context.'); }
                                file_put_contents($directory.'/remaining-delivery-'.getmypid().'.json', json_encode([
                                    'worker_pid' => getmypid(), 'at_delivery' => $remainingAtDelivery,
                                ], JSON_THROW_ON_ERROR));
                            }
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
                            if ($kind === 'polyglot') {
                                file_put_contents($directory.'/remaining-cleanup-final.json', json_encode([
                                    'worker_pid' => getmypid(), 'at_delivery' => $remainingAtDelivery,
                                    'after_cleanup' => $error->context?->remaining(),
                                ], JSON_THROW_ON_ERROR));
                            }
                            return (string) $error->requestId;
                        }
                        return 'not-cancelled';
                    });
                    $worker->registerWorkflow('tests.php-cooperative-payload', static fn (WorkflowContext $context, string $value): string => $value);
                }
                if ($remoteRole) {
                    $worker->registerActivity('tests.php-cooperative-remote', function (ActivityContext $context) use ($notify, $userHeartbeat, $remoteBlockSeconds, $remotePolicy): mixed {
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
                        return $remotePolicy === CancellationPolicy::Abandon ? 'independent-completion' : new \stdClass();
                    });
                }
                $worker->registerActivity('tests.php-cooperative-work', function (ActivityContext $context) use ($notify, $userHeartbeat): \stdClass {
                    file_put_contents($this->directory.'/local-fence', json_encode([
                        'task_id' => $context->taskId, 'activity_attempt_id' => $context->activityAttemptId,
                        'lease_owner' => $context->leaseOwner, 'attempt_number' => $context->attemptNumber,
                    ], JSON_THROW_ON_ERROR));
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

    private function spawnExternalWorker(string $language, string $queue): void
    {
        $binary = getenv('DURABLE_WORKFLOW_POLYGLOT_'.strtoupper($language).'_BINARY');
        self::assertIsString($binary);
        self::assertNotSame('', $binary);
        $command = $language === 'python' ? [$binary, __DIR__.'/polyglot-python.py', $queue] : [$binary, $queue];
        $environment = getenv();
        $environment['DURABLE_WORKFLOW_SERVER_URL'] = $this->runtimeUrl;
        $environment['DURABLE_WORKFLOW_AUTH_TOKEN'] = $this->token;
        $environment['DW_CASCADE_DIRECTORY'] = $this->directory;
        $process = proc_open($command, [
            0 => ['file', '/dev/null', 'r'],
            1 => ['file', $this->directory.'/'.$language.'-worker.log', 'a'],
            2 => ['file', $this->directory.'/'.$language.'-worker.log', 'a'],
        ], $pipes, __DIR__, $environment);
        self::assertIsResource($process);
        $this->externalProcesses[(int) $process] = $process;
    }

    /** @param resource $process */
    private function stopExternalWorker($process): void
    {
        $id = (int) $process;
        if (!isset($this->externalProcesses[$id])) { return; }
        $status = proc_get_status($process);
        if ($status['running']) { proc_terminate($process, SIGTERM); }
        $deadline = microtime(true) + 10;
        do {
            $status = proc_get_status($process);
            if (!$status['running']) {
                $exit = proc_close($process);
                unset($this->externalProcesses[$id]);
                self::assertSame(0, $status['exitcode'] >= 0 ? $status['exitcode'] : $exit, 'Mixed-language worker failed.');
                return;
            }
            usleep(50_000);
        } while (microtime(true) < $deadline);
        proc_terminate($process, SIGKILL);
        proc_close($process);
        unset($this->externalProcesses[$id]);
        self::fail('Mixed-language worker did not stop and join.');
    }

    private function awaitObservationFile(string $name): void
    {
        $deadline = microtime(true) + 20;
        do {
            if (is_file($this->directory.'/'.$name)) { return; }
            usleep(50_000);
        } while (microtime(true) < $deadline);
        self::fail('Mixed-language worker did not retain '.$name.'.');
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

    private function retainRunEvidence(Client $client, WorkflowHandle $handle, string $role): void
    {
        try {
            file_put_contents($this->directory.'/'.$role.'-history.json', json_encode(
                $this->history($client, $handle), JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
            file_put_contents($this->directory.'/'.$role.'-diagnostics.json', json_encode(
                $client->workflowDiagnostics($handle->workflowId, (string) $handle->selectedRunId),
                JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
            file_put_contents($this->directory.'/'.$role.'-execution.json', json_encode(
                $handle->describe()->raw, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
        } catch (Throwable $error) {
            file_put_contents($this->directory.'/'.$role.'-capture-error.txt', $error::class.': '.$error->getMessage());
        }
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
