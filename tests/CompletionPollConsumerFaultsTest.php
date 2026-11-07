<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollFaults;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollFaults.php';

final class CompletionPollConsumerFaultsTest extends TestCase
{
    public function testTransitAndExpiryAreSelectedFromTheOriginalProducerWindow(): void
    {
        self::assertSame(22.0, CompletionPollFaults::delay(self::grant(), 'transit', 60));
        self::assertSame(30.25, CompletionPollFaults::delay(self::grant(), 'expiry', 60));
    }

    public function testTheCallerBudgetCannotBeExtendedToReachAReportedExpiry(): void
    {
        $this->expectExceptionMessage('completion_fault_budget');
        CompletionPollFaults::delay(self::grant(), 'expiry', 30);
    }

    public function testMissingProducerClockDoesNotBecomeAWorkerWallClock(): void
    {
        $grant = self::grant();
        unset($grant['server_time']);
        $this->expectExceptionMessage('completion_fault_original_grant_required');
        CompletionPollFaults::delay($grant, 'transit', 60);
    }

    public function testAnAlreadyExpiredGrantCannotQualifyTheLiveTransitCase(): void
    {
        $grant = self::grant();
        $grant['task']['lease_expires_at'] = $grant['server_time'];
        $this->expectExceptionMessage('completion_fault_original_grant_required');
        CompletionPollFaults::delay($grant, 'transit', 60);
    }

    public function testFreshReplayClockDoesNotAlterOriginalLeaseIdentity(): void
    {
        $first = self::grant();
        $replay = $first;
        $replay['server_time'] = '2026-11-07T08:00:05.000000Z';
        self::assertSame(CompletionPollFaults::identity($first), CompletionPollFaults::identity($replay));
        $replay['task']['lease_expires_at'] = '2026-11-07T08:00:35.000000Z';
        self::assertNotSame(CompletionPollFaults::identity($first), CompletionPollFaults::identity($replay));
    }

    public function testReplayHistoryAndAttemptChangesRemainVisibleWithoutRetainingPayloads(): void
    {
        $first = self::grant();
        foreach (['workflow_task_attempt' => 2, 'history_events' => [['changed' => true]], 'ready_age_ms' => 1] as $field => $value) {
            $changed = $first;
            $changed['task'][$field] = $value;
            self::assertNotSame(CompletionPollFaults::identity($first), CompletionPollFaults::identity($changed));
        }
    }

    private static function grant(): array
    {
        return ['poll_status' => 'leased', 'server_time' => '2026-11-07T08:00:00.000000Z',
            'task' => ['task_id' => 'fixture-task', 'lease_owner' => 'fixture-worker', 'workflow_task_attempt' => 1,
                'lease_expires_at' => '2026-11-07T08:00:30.000000Z', 'ready_age_ms' => 0, 'history_events' => []]];
    }
}
