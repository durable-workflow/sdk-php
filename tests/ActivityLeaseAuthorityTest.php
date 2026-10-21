<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\ActivityLeaseAuthority;
use DurableWorkflow\Worker\WorkflowClaimRevoked;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ActivityLeaseAuthorityTest extends TestCase
{
    private float $now = 100;

    private function authority(array $changes = []): ActivityLeaseAuthority
    {
        return new ActivityLeaseAuthority(array_replace(self::task(), $changes), 'tenant', 'owner', 'queue',
            self::stamp(0), 100, fn (): float => $this->now);
    }

    public function testRenewalChargesNetworkTimeWithoutChangingOriginalIdentity(): void
    {
        $authority = $this->authority();
        $this->now = 102;
        $authority->acknowledge(self::renewal(2, 12), 101);
        self::assertSame(9.0, $authority->remaining());
        self::assertSame('0', $authority->identity()['workflow_instance_id']);
        self::assertSame('017', $authority->identity()['workflow_run_id']);
        $this->now = 109;
        self::assertSame(2.0, $authority->remaining());
        $this->now = 111;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->remaining();
    }

    #[DataProvider('identityCorruption')]
    public function testRenewalRefusesEveryChangedOriginalIdentity(string $key, mixed $value): void
    {
        $authority = $this->authority();
        $reply = self::renewal(1, 20);
        $reply[$key] = $value;
        try {
            $authority->acknowledge($reply, 100);
            self::fail('A different activity identity acquired authority.');
        } catch (WorkflowClaimRevoked $error) {
            self::assertSame('invalid_activity_renewal', $error->reason);
        }
        // A subsequent valid response must not restore a refused claim.
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledge(self::renewal(2, 21), 100);
    }

    public static function identityCorruption(): iterable
    {
        foreach (['task_id', 'workflow_task_id', 'workflow_instance_id', 'workflow_run_id',
            'activity_execution_id', 'activity_attempt_id', 'lease_owner'] as $key) {
            yield $key => [$key, 'foreign'];
        }
        yield 'attempt generation' => ['attempt_number', 2];
        yield 'string generation' => ['attempt_number', '1'];
        yield 'refused' => ['can_continue', false];
        yield 'truthy' => ['lease_renewed', 1];
        yield 'cancelled' => ['cancel_requested', true];
        yield 'user progress' => ['heartbeat_recorded', true];
        yield 'reason' => ['reason', 'attempt_closed'];
        yield 'missing lease' => ['lease_expires_at', null];
        yield 'bad date' => ['server_time', '2026-02-30T00:00:00.000000Z'];
        yield 'offset' => ['server_time', '2026-01-01T00:00:00.000000+00:00'];
    }

    #[DataProvider('businessDeadlines')]
    public function testRenewalCannotExtendAnyOriginalBusinessDeadline(string $name): void
    {
        $authority = $this->authority(['deadlines' => [$name => self::stamp(3)]]);
        $this->now = 102;
        $authority->acknowledge(self::renewal(2, 100), 101);
        self::assertSame(1.0, $authority->remaining());
        $this->now = 103;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->remaining();
    }

    public static function businessDeadlines(): iterable
    {
        foreach (['start_to_close', 'schedule_to_close', 'run', 'execution'] as $name) {
            yield $name => [$name];
        }
    }

    public function testSuccessAfterPriorLeaseExpiredCannotReviveAuthority(): void
    {
        $authority = $this->authority();
        $this->now = 110;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledge(self::renewal(10, 100), 109);
    }

    public function testReplayOfSuccessfulRenewalDoesNotResetItsClock(): void
    {
        $authority = $this->authority();
        $this->now = 101;
        $reply = self::renewal(1, 20);
        $authority->acknowledge($reply, 100);
        $this->now = 102;
        $authority->acknowledge($reply, 100);
        self::assertSame(17.0, $authority->remaining());
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledge($reply, 102);
    }

    public function testRuntimeRenewalDoesNotPretendToBeApplicationProgress(): void
    {
        $authority = $this->heartbeatAuthority();
        $this->now = 102;
        $authority->acknowledge(self::renewal(2, 20) + ['last_heartbeat_at' => self::stamp(0),
            'heartbeat_deadline_at' => self::stamp(4)], 101);
        self::assertSame(1.0, $authority->remaining(), 'The full control round trip consumes progress budget.');
        $this->now = 103;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->remaining();
    }

    public function testOnlyRecordedProgressWithTheOriginalIntervalExtendsHeartbeatDeadline(): void
    {
        $authority = $this->heartbeatAuthority();
        $this->now = 102;
        $authority->acknowledgeProgress(self::progress(2, 6, 12), 101);
        self::assertSame(3.0, $authority->remaining());
        $this->now = 104;
        $authority->acknowledge(self::renewal(4, 20) + ['last_heartbeat_at' => self::stamp(2),
            'heartbeat_deadline_at' => self::stamp(6)], 103);
        self::assertSame(1.0, $authority->remaining());
    }

    public function testBufferedHeartbeatCannotRenewAnAlreadyExpiredOriginalReceipt(): void
    {
        $authority = $this->heartbeatAuthority();
        $this->now = 105;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledgeProgress(self::progress(4, 8, 20), 103, 104);
    }

    public function testBufferedTimelyProgressIsChargedBeforeCurrentContinuation(): void
    {
        $authority = $this->heartbeatAuthority();
        $this->now = 104;
        $authority->acknowledgeProgress(self::progress(2, 6, 12), 101, 102);
        self::assertSame(1.0, $authority->remaining());
    }

    public function testLargerHeartbeatIntervalCannotBeInventedByAReply(): void
    {
        $authority = $this->heartbeatAuthority();
        $this->now = 102;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledgeProgress(self::progress(2, 8, 12), 101);
    }

    public function testRequiredSessionCanRenewOnlyItsLeaseAndNeverItsOriginalTtl(): void
    {
        $session = self::session();
        $authority = $this->authority(['worker_session' => $session]);
        $this->now = 102;
        $authority->acknowledgeSession(array_replace($session, ['lease_expires_at' => self::stamp(9),
            'server_time' => self::stamp(2)]), 101);
        self::assertSame(4.0, $authority->remaining());
        $this->now = 106;
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->remaining();
    }

    #[DataProvider('sessionChanges')]
    public function testSessionChangeCannotAcquireOrExtendAnotherGrant(string $key, mixed $value): void
    {
        $authority = $this->authority(['worker_session' => self::session()]);
        $this->now = 101;
        $reply = array_replace(self::session(), ['server_time' => self::stamp(1),
            'lease_expires_at' => self::stamp(9), $key => $value]);
        $this->expectException(WorkflowClaimRevoked::class);
        $authority->acknowledgeSession($reply, 100);
    }

    public static function sessionChanges(): iterable
    {
        yield 'TTL' => ['ttl_expires_at', self::stamp(100)];
        yield 'owner' => ['lease_owner', 'another-owner'];
        yield 'namespace' => ['namespace', 'another-tenant'];
        yield 'id' => ['session_id', 'another-session'];
        yield 'queue' => ['queue', 'another-queue'];
        yield 'requirements' => ['requirements', ['gpu']];
        yield 'policy' => ['lease_seconds', 100];
        yield 'status' => ['status', 'closed'];
    }

    public function testJsonObjectMemberOrderDoesNotChangeSessionIdentity(): void
    {
        $authority = $this->authority(['worker_session' => self::session()]);
        $this->now = 101;
        $reply = array_replace(array_reverse(self::session(), true), ['server_time' => self::stamp(1)]);
        $authority->acknowledgeSession($reply, 100);
        self::assertSame(2.0, $authority->remaining());
    }

    private function heartbeatAuthority(): ActivityLeaseAuthority
    {
        return $this->authority(['deadlines' => ['heartbeat' => self::stamp(4)],
            'retry_policy' => ['heartbeat_timeout' => 4]]);
    }

    private static function task(): array
    {
        return ['task_id' => 'task', 'workflow_id' => '0', 'run_id' => '017',
            'activity_execution_id' => 'execution', 'activity_attempt_id' => 'attempt', 'attempt_number' => 1,
            'lease_owner' => 'owner', 'task_queue' => 'queue', 'namespace' => 'tenant',
            'lease_expires_at' => self::stamp(10)];
    }

    private static function renewal(int $server, int $expiry): array
    {
        return ['task_id' => 'task', 'workflow_task_id' => 'task', 'workflow_instance_id' => '0',
            'workflow_run_id' => '017', 'activity_execution_id' => 'execution', 'activity_attempt_id' => 'attempt',
            'attempt_number' => 1, 'lease_owner' => 'owner', 'lease_renewed' => true, 'can_continue' => true,
            'cancel_requested' => false, 'heartbeat_recorded' => false, 'reason' => null,
            'server_time' => self::stamp($server), 'lease_expires_at' => self::stamp($expiry)];
    }

    private static function progress(int $progress, int $deadline, int $expiry): array
    {
        return array_replace(self::renewal($progress, $expiry), ['heartbeat_recorded' => true,
            'last_heartbeat_at' => self::stamp($progress), 'heartbeat_deadline_at' => self::stamp($deadline)]);
    }

    private static function session(): array
    {
        return ['session_id' => 'session', 'status' => 'active', 'namespace' => 'tenant', 'lease_owner' => 'owner',
            'queue' => 'queue', 'requirements' => [], 'lease_seconds' => 4, 'ttl_seconds' => 7,
            'max_concurrent_activities' => 1, 'create_if_missing' => true, 'allow_reacquire_after_failure' => true,
            'lease_expires_at' => self::stamp(4), 'ttl_expires_at' => self::stamp(7)];
    }

    private static function stamp(int $seconds): string
    {
        return gmdate('Y-m-d\TH:i:s', 1767225600 + $seconds).'.000000Z';
    }
}
