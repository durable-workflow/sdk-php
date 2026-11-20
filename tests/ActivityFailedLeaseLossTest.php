<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\ActivityFailed;
use PHPUnit\Framework\TestCase;

final class ActivityFailedLeaseLossTest extends TestCase
{
    public function testServerSettledLeaseLossCarriesItsRecordedReason(): void
    {
        $expired = new ActivityFailed('Activity echo lost its original task lease before the attempt settled.', 'echo', null, false,
            ['exception_class' => ActivityFailed::SERVER_LEASE_LOST_TYPE, 'failure_reason' => 'lease_expired'], 'ActivityFailed');
        self::assertSame('lease_expired', $expired->leaseLossReason());
        $revoked = new ActivityFailed('lost', 'echo', null, false,
            ['exception_class' => ActivityFailed::SERVER_LEASE_LOST_TYPE, 'failure_reason' => 'worker_deregistered'], 'ActivityFailed');
        self::assertSame('worker_deregistered', $revoked->leaseLossReason());
        $bare = new ActivityFailed('lost', 'echo', ActivityFailed::SERVER_LEASE_LOST_TYPE);
        self::assertSame('lease_expired', $bare->leaseLossReason());
    }

    public function testAWorkerReleasedClaimIsWorkerLost(): void
    {
        $released = new ActivityFailed('released', 'echo', 'worker_lost', false,
            ['exception_type' => 'worker_lost', 'exception_class' => ActivityFailed::WORKER_LOST_CLASS], 'ActivityFailed');
        self::assertSame('worker_lost', $released->leaseLossReason());
        // Another SDK's class name with the same reason code.
        $python = new ActivityFailed('released', 'echo', null, false,
            ['exception_type' => 'worker_lost', 'exception_class' => 'ActivityWorkerLost'], 'ActivityFailed');
        self::assertSame('worker_lost', $python->leaseLossReason());
        // The class alone is not the reason code.
        self::assertNull((new ActivityFailed('x', 'echo', null, false,
            ['exception_class' => ActivityFailed::WORKER_LOST_CLASS]))->leaseLossReason());
    }

    public function testOtherFailuresHaveNoLeaseLossReason(): void
    {
        self::assertNull((new ActivityFailed('boom', 'echo', 'RuntimeException', false, ['exception_type' => 'RuntimeException']))->leaseLossReason());
        self::assertNull((new ActivityFailed('timed out', 'echo', null, false, ['timeout_kind' => 'start_to_close'], 'ActivityTimedOut'))->leaseLossReason());
    }
}
