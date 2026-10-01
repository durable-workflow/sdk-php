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
    private string $directory;

    protected function setUp(): void
    {
        if (!\DurableWorkflow\Worker\CooperativeActivityExecutor::available()) {
            self::markTestSkipped('Cooperative worker execution requires Unix process control.');
        }
        $this->directory = sys_get_temp_dir().'/dw-cooperative-worker-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
    }

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

    #[DataProvider('unsupportedChildPolicyProvider')]
    public function testCooperativeChildPoliciesRequireTheWorkerOptIn(string $protocol, array $options): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: $protocol),
            'queue', workerId: 'worker-1');
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->childWorkflow('child', [], $options));

        self::assertTrue($worker->tick(0));
        self::assertSame([], $transport->completions);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
        self::assertStringContainsString('child_cancellation_policy_not_supported', $transport->failures[0]['failure']['message']);
        self::assertStringContainsString('worker-1', $transport->failures[0]['failure']['message']);
        self::assertStringContainsString('1.20', $transport->failures[0]['failure']['message']);
    }

    public static function unsupportedChildPolicyProvider(): iterable
    {
        foreach (['1.19', '1.20'] as $protocol) {
            yield [$protocol, ['parent_close_policy' => \DurableWorkflow\Worker\ParentClosePolicy::RequestCancellation]];
            yield [$protocol, ['cancellation_policy' => \DurableWorkflow\Worker\CancellationPolicy::TryCancel]];
            yield [$protocol, ['cancellation_policy' => \DurableWorkflow\Worker\CancellationPolicy::WaitCancellationCompleted]];
        }
    }

    public function testCapableWorkerTransmitsBothTypedChildPolicies(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->childWorkflow('python.child', [], [
            'parent_close_policy' => \DurableWorkflow\Worker\ParentClosePolicy::RequestCancellation,
            'cancellation_policy' => \DurableWorkflow\Worker\CancellationPolicy::WaitCancellationCompleted,
        ]));

        self::assertTrue($worker->tick(0));
        self::assertSame([], $transport->failures);
        self::assertSame('request_cancellation', $transport->completions[0]['commands'][0]['parent_close_policy']);
        self::assertSame('wait_cancellation_completed', $transport->completions[0]['commands'][0]['cancellation_policy']);
    }

    #[DataProvider('legacyChildPolicyProvider')]
    public function testLegacyChildPoliciesRemainAvailableWithoutCooperation(\DurableWorkflow\Worker\ParentClosePolicy $policy): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->childWorkflow('child', [], [
            'parent_close_policy' => $policy,
            'cancellation_policy' => \DurableWorkflow\Worker\CancellationPolicy::Abandon,
        ]));

        self::assertTrue($worker->tick(0));
        self::assertSame([], $transport->failures);
        self::assertSame($policy->value, $transport->completions[0]['commands'][0]['parent_close_policy']);
    }

    public static function legacyChildPolicyProvider(): iterable
    {
        foreach ([\DurableWorkflow\Worker\ParentClosePolicy::Abandon, \DurableWorkflow\Worker\ParentClosePolicy::RequestCancel, \DurableWorkflow\Worker\ParentClosePolicy::Terminate] as $policy) {
            yield [$policy];
        }
    }

    public function testPendingChildReleasesTheWorkerAndCleanupReplaysOnANewClaim(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->stopAuxiliaryPolls = false;
        $transport->pendingDeliveries = 1;
        $parentHistory = $transport->history;
        $seen = null;
        $worker = new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'worker-1', enableCooperativeCancellation: true,
            clock: static fn (): float => 1790812800.0);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$seen): string {
            try { $context->childWorkflow('child'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled $error) { $seen = $error->requestId; }
            return 'cleanup complete';
        });
        $worker->registerWorkflow('child', static fn (WorkflowContext $context): string => 'child complete');
        $worker->tick(0);

        self::assertNull($seen);
        self::assertCount(1, $transport->deliveries);
        self::assertSame([], $transport->failures);
        self::assertSame([], $transport->completions);
        self::assertSame(['opaque-start'], array_column($transport->historyRequests, 'next_history_page_token'));

        $transport->workflowType = 'child';
        $transport->taskId = 'child-task';
        $transport->requestVisible = false;
        $transport->history = [$parentHistory[0]];
        $worker->tick(0);
        self::assertNull($seen);
        self::assertCount(1, $transport->completions);
        self::assertSame('complete_workflow', $transport->completions[0]['commands'][0]['type']);

        $transport->workflowType = 'cancel';
        $transport->taskId = 'parent-resume-task';
        $transport->workflowTaskAttempt = 1;
        $transport->requestVisible = true;
        $transport->history = $parentHistory;
        $worker->tick(0);
        self::assertSame('request-1', $seen);
        self::assertCount(2, $transport->deliveries);
        self::assertCount(2, $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame(1, $transport->deliveries[1]['workflow_task_attempt']);
        foreach ($transport->deliveries as $body) {
            self::assertSame('request-1', $body['request_id']);
            self::assertSame('child', $body['call_kind']);
        }
        self::assertSame('2026-10-01T00:01:00Z', $transport->observation['cleanup_deadline_at']);
    }

    public function testExpiredChildWaitDoesNotRequestDeliveryOrPublishCleanupOrFailure(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->pendingDeliveries = 100;
        $seen = false;
        $worker = new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'worker-1', enableCooperativeCancellation: true,
            clock: static fn (): float => 1790812861.0);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$seen): void {
            try { $context->childWorkflow('child'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) { $seen = true; }
        });
        $worker->tick(0);
        self::assertFalse($seen);
        self::assertSame([], $transport->deliveries);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);
        self::assertSame('2026-10-01T00:01:00Z', $transport->observation['cleanup_deadline_at']);
    }

    #[DataProvider('cleanupTerminalFenceProvider')]
    public function testPendingChildHonorsTerminalClaimFenceWithoutSuppressingMismatchedDiagnostics(bool $matchingTask): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->pendingDeliveries = 100;
        $transport->onDelivery = static function () use ($matchingTask): void {
            throw new \DurableWorkflow\Exception\ServerException('Workflow run is already closed.', 409, 'run_closed', [
                'task_id' => $matchingTask ? 'task-1' : 'different-task',
                'can_continue' => false, 'task_status' => 'cancelled',
            ]);
        };
        $worker = $this->worker($transport);
        $seen = false;
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context) use (&$seen): void {
            try { $context->childWorkflow('child'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) { $seen = true; }
        });
        $worker->tick(0);
        self::assertFalse($seen);
        self::assertCount(1, $transport->deliveries);
        self::assertSame([], $transport->completions);
        self::assertCount($matchingTask ? 0 : 1, $transport->failures);
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
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($entered): void {
            if (is_file($entered)) { $transport->renewed = false; }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'too late';
        });
        $started = microtime(true);
        $worker->tick(0);
        self::assertLessThan(3, microtime(true) - $started);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
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
        $entered = $this->directory.'/entered';
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($entered): void {
            if (is_file($entered) && !$transport->requestVisible) { $transport->makeRequestVisible(); }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            try { $context->localActivity('effect'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) { $context->activity('cleanup'); }
        });
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $heartbeat): \stdClass {
            file_put_contents($entered, 'entered', FILE_APPEND);
            if ($heartbeat) { $context->heartbeat(); }
            return new \stdClass();
        });
        $worker->tick(0);
        self::assertSame('entered', file_get_contents($entered));
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
        $entered = $this->directory.'/second-entered';
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($entered): void {
            if (is_file($entered) && !$transport->requestVisible) { $transport->makeRequestVisible(); }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            $context->localActivity('first');
            $context->localActivity('second');
        });
        $worker->registerActivity('first', static fn (ActivityContext $context) => 'committed first');
        $worker->registerActivity('second', static function (ActivityContext $context) use ($entered): void {
            file_put_contents($entered, 'entered');
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
        $entered = $this->directory.'/cleanup-entered';
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
        $worker->registerActivity('cleanup', static function (ActivityContext $context) use ($entered): string {
            file_put_contents($entered, 'entered', FILE_APPEND);
            $context->heartbeat();
            return 'cleaned';
        });
        $worker->tick(0);
        self::assertSame('entered', file_get_contents($entered));
        self::assertSame(['record_local_activity', 'complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame([], $transport->failures);
    }

    public function testShutdownDuringLocalCallbackAbandonsClaimWithoutRecordingResult(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $transport->onHeartbeat = static function () use ($entered, $worker): void {
            if (is_file($entered)) { $worker->requestShutdown(); }
        };
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): \stdClass {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return new \stdClass();
        });
        $worker->tick(0);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->deliveries);
    }

    public function testBlockingLocalCallbackObservesRequestWithoutUserHeartbeat(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($entered): void {
            if (is_file($entered) && !$transport->requestVisible) { $transport->makeRequestVisible(); }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): string {
            try { $context->localActivity('effect'); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled $error) { return (string) $error->requestId; }
            return 'not cancelled';
        });
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): \stdClass {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return new \stdClass();
        });
        $started = microtime(true);
        $worker->tick(0);
        self::assertLessThan(3, microtime(true) - $started);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame('request-1', (new \DurableWorkflow\Codec\AvroPayloadCodec())->decodeEnvelope(
            $transport->completions[0]['commands'][0]['result']));
        self::assertCount(1, $transport->deliveries);
        self::assertSame('local_activity', $transport->deliveries[0]['call_kind']);
        self::assertSame([], $transport->failures);
    }

    public function testOriginalCleanupDeadlineStopsShieldedBlockedCallback(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->stopAuxiliaryPolls = false;
        $entered = $this->directory.'/cleanup-entered';
        $late = $this->directory.'/late';
        $now = 1790812800.0;
        $transport->onHeartbeat = static function () use ($entered, &$now): void {
            if (is_file($entered)) { $now = 1790812861.0; }
        };
        $worker = $this->worker($transport, static function () use (&$now): float { return $now; });
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            try { $context->sleep(10); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) {
                $context->cancellationShield(static fn () => $context->localActivity('cleanup'));
            }
        });
        $worker->registerActivity('cleanup', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'too late';
        });
        $worker->tick(0);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertCount(1, $transport->deliveries);
        self::assertSame([], $transport->completions);
        self::assertSame([], $transport->failures);

        // A revoked claim must leave the worker available for unrelated work.
        $transport->onHeartbeat = null;
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker->registerWorkflow('next', static fn (WorkflowContext $context): string => 'next workflow');
        $transport->workflowType = 'next';
        self::assertTrue($worker->tick(0));
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        self::assertSame([], $transport->failures);
    }

    #[DataProvider('cleanupTerminalFenceProvider')]
    public function testTerminalFenceStopsCleanupWithoutPublishingFailureAndKeepsWorkerUsable(bool $matchingTask): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->stopAuxiliaryPolls = false;
        $entered = $this->directory.'/cleanup-entered';
        $late = $this->directory.'/late';
        $transport->onHeartbeat = static function () use ($entered, $matchingTask): void {
            if (is_file($entered)) {
                throw new \DurableWorkflow\Exception\ServerException('Workflow run is already closed.', 409, 'run_closed', [
                    'task_id' => $matchingTask ? 'task-1' : 'different-task',
                    'can_continue' => false, 'task_status' => 'cancelled',
                ]);
            }
        };
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static function (WorkflowContext $context): void {
            try { $context->sleep(10); }
            catch (\DurableWorkflow\Exception\WorkflowCancelled) {
                $context->cancellationShield(static fn () => $context->localActivity('cleanup'));
            }
        });
        $worker->registerActivity('cleanup', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'too late';
        });
        $worker->tick(0);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertSame([], $transport->completions);
        if ($matchingTask) {
            self::assertSame([], $transport->failures);
        } else {
            self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
        }

        $transport->onHeartbeat = null;
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $transport->workflowType = 'next';
        $worker->registerWorkflow('next', static fn (WorkflowContext $context): string => 'next workflow');
        self::assertTrue($worker->tick(0));
        self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
    }

    public static function cleanupTerminalFenceProvider(): array
    {
        return [[true], [false]];
    }

    #[DataProvider('isolatedFailureProvider')]
    public function testIsolatedLocalFailureKeepsItsPublishedClassification(string $kind, string $type,
        string $outcome, bool $nonRetryable): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $entered = $this->directory.'/entered';
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect', [], [
            'retry_policy' => ['max_attempts' => 3, 'non_retryable_error_types' => ['CooperativeBusinessFailure']],
        ]));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($kind, $entered): mixed {
            file_put_contents($entered, 'call', FILE_APPEND);
            return match ($kind) {
                'business' => throw new CooperativeBusinessFailure('business failure'),
                'cancelled' => throw new \DurableWorkflow\Exception\ActivityCancelled('cancelled'),
                'timeout' => throw new \DurableWorkflow\Exception\LocalActivityTimedOut('heartbeat', 'timed out'),
                'invalid utf8' => throw new \RuntimeException("invalid\xFF"),
                'unencodable' => new \stdClass(),
            };
        });
        $worker->tick(0);
        $report = $transport->completions[0]['commands'][0];
        self::assertSame('record_local_activity', $report['type']);
        self::assertSame($type, $report['exception_type']);
        self::assertSame($outcome, $report['outcome']);
        self::assertSame($nonRetryable, $report['non_retryable']);
        self::assertCount($nonRetryable ? 1 : 3, $report['attempts']);
        self::assertSame(str_repeat('call', $nonRetryable ? 1 : 3), file_get_contents($entered));
        if ($kind === 'timeout') { self::assertSame('heartbeat', $report['timeout_kind']); }
        self::assertSame([], $transport->failures);
    }

    public static function isolatedFailureProvider(): array
    {
        return [
            ['business', CooperativeBusinessFailure::class, 'failed', true],
            ['cancelled', \DurableWorkflow\Exception\ActivityCancelled::class, 'cancelled', true],
            ['timeout', \DurableWorkflow\Exception\LocalActivityTimedOut::class, 'timed_out', false],
            ['invalid utf8', \DurableWorkflow\Exception\InvalidLocalActivityReport::class, 'failed', true],
            ['unencodable', \DurableWorkflow\Exception\InvalidLocalActivityReport::class, 'failed', true],
        ];
    }

    public function testLeaseRenewalsDoNotCountAsUserProgressForHeartbeatTimeout(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport, static fn (): float => microtime(true));
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect', [], [
            'heartbeat_timeout' => 1,
        ]));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'late';
        });
        $worker->tick(0);
        $report = $transport->completions[0]['commands'][0];
        self::assertSame('timed_out', $report['outcome']);
        self::assertSame('heartbeat', $report['timeout_kind']);
        self::assertSame([], $report['attempts'][0]['heartbeats']);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
        self::assertGreaterThan(1, $transport->heartbeatCount);
    }

    #[DataProvider('executionTimeoutProvider')]
    public function testExecutionTimeoutStopsTheActiveProcess(string $option, string $kind): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $entered = $this->directory.'/entered';
        $late = $this->directory.'/late';
        $now = 1790812800.0;
        $transport->onHeartbeat = static function () use ($entered, &$now): void {
            if (is_file($entered)) { $now = 1790812802.0; }
        };
        $worker = $this->worker($transport, static function () use (&$now): float { return $now; });
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect', [], [$option => 1]));
        $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered, $late): string {
            file_put_contents($entered, 'entered');
            sleep(60);
            file_put_contents($late, 'late');
            return 'late';
        });
        $worker->tick(0);
        $report = $transport->completions[0]['commands'][0];
        self::assertSame('timed_out', $report['outcome']);
        self::assertSame($kind, $report['timeout_kind']);
        self::assertFileExists($entered);
        self::assertFileDoesNotExist($late);
    }

    public static function executionTimeoutProvider(): array
    {
        return [['start_to_close_timeout', 'start_to_close'], ['schedule_to_close_timeout', 'schedule_to_close']];
    }

    public function testCallbackStorageRefusalAbandonsClaimWithoutApplicationFailure(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $worker = $this->worker($transport);
        $worker->registerWorkflow('cancel', static fn (WorkflowContext $context) => $context->localActivity('effect'));
        $worker->registerActivity('effect', static function (ActivityContext $context): never {
            throw new \DurableWorkflow\Exception\ServerException('storage fenced', 503, 'storage_pressure', [
                'reason' => 'storage_pressure', 'retryable' => true, 'retry_after_seconds' => 1,
                'storage_state' => 'fenced', 'request_admitted' => false,
            ]);
        });
        $worker->tick(0);
        self::assertSame([], $transport->completions);
        self::assertSame(WorkflowClaimAborted::class, $transport->failures[0]['failure']['type']);
    }

    public function testCancellationDuringRetryBackoffStopsFurtherAttempts(): void
    {
        $transport = new CooperativeWorkerTransport();
        $transport->requestVisible = false;
        $transport->history = [$transport->history[0]];
        $entered = $this->directory.'/entered';
        $requested = $this->directory.'/requested';
        $transport->onHeartbeat = static function (CooperativeWorkerTransport $transport) use ($requested): void {
            if (is_file($requested) && !$transport->requestVisible) { $transport->makeRequestVisible(); }
        };
        $producer = pcntl_fork();
        self::assertNotSame(-1, $producer);
        if ($producer === 0) {
            $deadline = microtime(true) + 5;
            while (!is_file($entered) && microtime(true) < $deadline) { usleep(10_000); }
            usleep(200_000);
            file_put_contents($requested, 'requested');
            exit(0);
        }
        try {
            $worker = $this->worker($transport);
            $worker->registerWorkflow('cancel', static function (WorkflowContext $context): string {
                try { $context->localActivity('effect', [], ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [5]]]); }
                catch (\DurableWorkflow\Exception\WorkflowCancelled $error) { return (string) $error->requestId; }
                return 'not cancelled';
            });
            $worker->registerActivity('effect', static function (ActivityContext $context) use ($entered): never {
                file_put_contents($entered, 'call', FILE_APPEND);
                throw new \RuntimeException('retry me');
            });
            $started = microtime(true);
            $worker->tick(0);
            self::assertLessThan(3, microtime(true) - $started);
            self::assertSame('call', file_get_contents($entered));
            self::assertCount(1, $transport->deliveries);
            self::assertSame(['complete_workflow'], array_column($transport->completions[0]['commands'], 'type'));
        } finally {
            if (pcntl_waitpid($producer, $status, WNOHANG) === 0) {
                posix_kill($producer, SIGKILL);
                pcntl_waitpid($producer, $status);
            }
        }
    }

    private function worker(CooperativeWorkerTransport $transport, ?\Closure $clock = null): Worker
    {
        return new Worker(new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20'),
            'queue', workerId: 'worker-1', enableCooperativeCancellation: true,
            clock: $clock ?? static fn (): float => 1790812800.0);
    }
}

final class CooperativeBusinessFailure extends \RuntimeException {}

final class CooperativeWorkerTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    public string $workflowType = 'cancel';
    public string $taskId = 'task-1';
    public int $workflowTaskAttempt = 3;
    public bool $stopAuxiliaryPolls = true;
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
    public int $pendingDeliveries = 0;
    public ?string $refreshFault = null;
    public ?\Closure $onHeartbeat = null;
    public ?\Closure $onDelivery = null;

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }

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
            return ['task' => ['task_id' => $this->taskId, 'workflow_task_attempt' => $this->workflowTaskAttempt, 'lease_owner' => 'worker-1',
                'workflow_id' => 'workflow-1', 'run_id' => 'run-1', 'workflow_type' => $this->workflowType, 'payload_codec' => 'avro',
                'history_events' => [$this->history[0]],
                ...($this->requestVisible ? ['cancellation_request' => $this->observation] : [])], 'poll_status' => 'leased'];
        }
        if (str_ends_with($uri, '/poll')) {
            return $this->stopAuxiliaryPolls
                ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                : ['task' => null, 'poll_status' => 'empty'];
        }
        if (str_ends_with($uri, '/heartbeat')) {
            ++$this->heartbeatCount;
            if ($this->onHeartbeat !== null) { ($this->onHeartbeat)($this); }
            return ['task_id' => $this->taskId, 'lease_owner' => 'worker-1', 'workflow_task_attempt' => $this->workflowTaskAttempt, 'renewed' => $this->renewed,
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
            if ($this->onDelivery !== null) { ($this->onDelivery)($this); }
            if ($this->pendingDeliveries > 0) {
                --$this->pendingDeliveries;
                return ['delivered' => false, 'task_id' => $this->taskId, 'reason' => 'cancellation_waiting_for_child', 'claim_released' => true];
            }
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
            return ['delivered' => true, 'task_id' => $this->taskId, ...array_diff_key($body, array_flip(['lease_owner', 'workflow_task_attempt'])),
                'operation_sequence' => $body['operation_sequence'] ?? null,
                'operation_sequence_span' => $body['operation_sequence_span'] ?? 1,
                ...($this->deliveryReply === 'malformed acknowledgment' ? ['call_kind' => 'invalid'] : [])];
        }
        if (str_ends_with($uri, '/complete')) { $this->completions[] = $body; return ['completed' => true]; }
        if (str_ends_with($uri, '/fail')) { $this->failures[] = $body; return ['failed' => true]; }
        throw new \RuntimeException('Unexpected request '.$method.' '.$uri);
    }
}
