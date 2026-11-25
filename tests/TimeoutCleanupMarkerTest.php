<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowTimeoutCleanupRequested;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\TimeoutCleanup;
use PHPUnit\Framework\TestCase;

/** The portable `failure.interruption` marker is derived from the delivered context, never a class name alone (server#1201). */
final class TimeoutCleanupMarkerTest extends TestCase
{
    public function testTheDeliveredTimeoutInterruptionIsMarked(): void
    {
        $context = CancellationContext::fromArray(self::snapshot('timeout'));
        $delivered = TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1', $context));
        self::assertSame(['interruption' => 'timeout_cleanup'], TimeoutCleanup::failureMarker($delivered));
        self::assertSame(['interruption' => 'timeout_cleanup'], TimeoutCleanup::failureMarker($delivered, 'run-1'));
    }

    public function testOtherFailuresAreNotMarked(): void
    {
        $timeout = CancellationContext::fromArray(self::snapshot('timeout'));
        $delivered = static fn (array $overrides): WorkflowCancelled => TimeoutCleanup::delivered(
            WorkflowCancelled::forContext($overrides['request_id'] ?? 'request-1',
                CancellationContext::fromArray([...self::snapshot('timeout'), ...$overrides])));
        foreach ([
            'constructed without context' => new WorkflowTimeoutCleanupRequested(),
            'constructed with only a request' => new WorkflowTimeoutCleanupRequested(requestId: 'request-1'),
            'constructed with a copied context' => new WorkflowTimeoutCleanupRequested(requestId: 'request-1', context: $timeout),
            'built by forContext, never delivered' => WorkflowCancelled::forContext('request-1', $timeout),
            'another request' => TimeoutCleanup::delivered(new WorkflowTimeoutCleanupRequested(requestId: 'request-2', context: $timeout)),
            'ordinary cancellation' => TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1',
                CancellationContext::fromArray(self::snapshot(null)))),
            'explicit cancellation' => TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1',
                CancellationContext::fromArray(self::snapshot('cancellation')))),
            'wrapped interruption' => new \RuntimeException('wrapped',
                previous: TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1', $timeout))),
            'descendant with a parent request' => $delivered(['request_id' => 'request-2', 'parent_request_id' => 'request-1',
                'lineage' => [
                    ['request_id' => 'request-1', 'workflow_instance_id' => 'workflow-1', 'workflow_run_id' => 'run-1'],
                    ['request_id' => 'request-2', 'workflow_instance_id' => 'child-1', 'workflow_run_id' => 'child-run-1'],
                ]]),
            'scoped delivery of the timeout request' => TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1',
                ScopedCancellationContext::fromRunContext($timeout))),
        ] as $case => $failure) {
            self::assertNull(TimeoutCleanup::failureMarker($failure), $case);
        }
        $root = TimeoutCleanup::delivered(WorkflowCancelled::forContext('request-1', $timeout));
        self::assertNull(TimeoutCleanup::failureMarker($root, 'another-run'), 'another run');
    }

    /** @return array<string, mixed> */
    private static function snapshot(?string $cause): array
    {
        return ['schema' => 'durable-workflow.cancellation-context/v1', 'request_id' => 'request-1',
            'root_request_id' => 'request-1', 'root_workflow_instance_id' => 'workflow-1', 'root_workflow_run_id' => 'run-1',
            'parent_request_id' => null, 'reason' => 'work deadline passed',
            'requester' => ['type' => 'system', 'id' => 'timeout_cleanup'], 'source' => 'system',
            'requested_at' => '2026-11-13T00:01:00.000000Z', 'cleanup_deadline_at' => '2026-11-13T00:01:10.000000Z',
            'lineage' => [['request_id' => 'request-1', 'workflow_instance_id' => 'workflow-1', 'workflow_run_id' => 'run-1']],
            ...($cause === null ? [] : ['cause' => $cause])];
    }
}
