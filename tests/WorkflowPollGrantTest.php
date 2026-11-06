<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\WorkflowPollGrant;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowPollGrantTest extends TestCase
{
    #[DataProvider('grantFenceChanges')]
    public function testFreshGrantCannotAuthorizeAChangedTask(string $field, mixed $changed): void
    {
        $poll = self::freshWorkflowPoll();
        $grant = WorkflowPollGrant::observe($poll, 'worker-1', 0, 0.1);
        self::assertNotNull($grant);
        self::assertTrue($grant->isFreshFor($poll['task'], 0.2));
        $poll['task'][$field] = $changed;
        self::assertFalse($grant->isFreshFor($poll['task'], 0.2));
    }

    /** @return iterable<string, array{string, mixed}> */
    public static function grantFenceChanges(): iterable
    {
        yield 'other task' => ['task_id', 'workflow-task-2'];
        yield 'other owner' => ['lease_owner', 'worker-2'];
        yield 'other attempt' => ['workflow_task_attempt', 4];
        yield 'attempt type alias' => ['workflow_task_attempt', '3'];
        yield 'changed expiry' => ['lease_expires_at', '2026-07-18T00:01:00.000000Z'];
    }

    #[DataProvider('monotonicGrantBounds')]
    public function testServerLeaseAndMonotonicObservationBothBoundFreshness(float $received, float $now, bool $fresh): void
    {
        $poll = self::freshWorkflowPoll();
        $grant = WorkflowPollGrant::observe($poll, 'worker-1', 0, $received);
        self::assertNotNull($grant);
        self::assertSame($fresh, $grant->isFreshFor($poll['task'], $now));
    }

    /** @return iterable<string, array{float, float, bool}> */
    public static function monotonicGrantBounds(): iterable
    {
        yield 'fresh with margin' => [0.1, 0.2, true];
        yield 'exactly one second old' => [0.0, 1.0, false];
        yield 'backwards monotonic observation' => [0.2, 0.1, false];
        yield 'exactly ten seconds remain including request' => [19.5, 20.0, true];
        yield 'request time leaves under ten seconds' => [20.0, 20.1, false];
        yield 'nonfinite current reading' => [0.0, INF, false];
    }

    public function testInvalidOrForeignPollIdentityCannotCreateFreshAuthority(): void
    {
        $poll = self::freshWorkflowPoll();
        foreach (['task_id' => '', 'lease_owner' => 'other-worker', 'workflow_task_attempt' => '3'] as $key => $value) {
            $invalid = $poll;
            $invalid['task'][$key] = $value;
            self::assertNull(WorkflowPollGrant::observe($invalid, 'worker-1', 0, 0.1));
        }
        self::assertNull(WorkflowPollGrant::observe(['task' => null], 'worker-1', 0, 0.1));
        self::assertNull(WorkflowPollGrant::observe($poll, 'worker-1', 0.2, 0.1));
        self::assertNull(WorkflowPollGrant::observe($poll, 'worker-1', NAN, 0.1));
        self::assertNull(WorkflowPollGrant::observe($poll, 'worker-1', 0, INF));
    }

    /** @return array<string, mixed> */
    private static function freshWorkflowPoll(): array
    {
        return ['poll_status' => 'leased', 'server_time' => '2026-07-18T00:00:00.000000Z',
            'task' => ['task_id' => 'workflow-task-1', 'lease_owner' => 'worker-1', 'workflow_task_attempt' => 3,
                'lease_expires_at' => '2026-07-18T00:00:30.000000Z']];
    }
}
