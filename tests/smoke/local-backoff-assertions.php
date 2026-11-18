<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Smoke;

use RuntimeException;

/**
 * @param list<array<string, mixed>> $callbacks
 * @param list<array<string, mixed>> $history
 * @param list<int> $expectedDelays
 * @return list<float>
 */
function verifyLocalBackoff(array $callbacks, string $workflowId, string $runId, array $history, array $expectedDelays): array
{
    $count = count($expectedDelays) + 1;
    if (count($callbacks) !== $count || array_column($callbacks, 'attempt_number') !== range(1, $count)) {
        throw new RuntimeException('Require exactly the declared local callback attempts.');
    }
    $events = static fn (string $type): array => array_values(array_filter($history,
        static fn (array $event): bool => ($event['event_type'] ?? null) === $type));
    foreach (['ActivityScheduled' => 1, 'ActivityStarted' => $count, 'ActivityRetryScheduled' => $count - 1,
        'ActivityCompleted' => 1, 'WorkflowCompleted' => 1, 'ActivityFailed' => 0, 'WorkflowFailed' => 0] as $type => $expected) {
        if (count($events($type)) !== $expected) {
            throw new RuntimeException('Unexpected local history count for '.$type);
        }
    }
    $started = $events('ActivityStarted');
    $retries = $events('ActivityRetryScheduled');
    $executionId = $started[0]['payload']['activity_execution_id'] ?? null;
    if (!is_string($executionId) || $executionId === ''
        || count(array_unique(array_column($callbacks, 'activity_attempt_id'))) !== $count
        || count(array_unique(array_map(static fn (array $event): mixed => $event['payload']['activity_attempt_id'] ?? null, $started))) !== $count) {
        throw new RuntimeException('Require one durable local execution and distinct worker attempt identities.');
    }
    $gaps = [];
    foreach ($callbacks as $index => $callback) {
        $payload = $started[$index]['payload'] ?? [];
        if (($callback['workflow_id'] ?? null) !== $workflowId || ($callback['run_id'] ?? null) !== $runId
            || !is_string($callback['task_id'] ?? null) || $callback['task_id'] === ''
            || !is_string($callback['activity_attempt_id'] ?? null) || $callback['activity_attempt_id'] === ''
            || !is_int($callback['monotonic_ns'] ?? null)
            || ($payload['execution_mode'] ?? null) !== 'local'
            || ($payload['activity_execution_id'] ?? null) !== $executionId
            || ($payload['workflow_task_id'] ?? null) !== $callback['task_id']
            || ($payload['worker_attempt_id'] ?? null) !== $callback['activity_attempt_id']
            || !is_string($payload['activity_attempt_id'] ?? null) || $payload['activity_attempt_id'] === ''
            || ($payload['attempt_number'] ?? null) !== $index + 1) {
            throw new RuntimeException('Each local callback must bind its exact durable started identity.');
        }
        if ($index === $count - 1) {
            continue;
        }
        $retry = $retries[$index]['payload'] ?? [];
        if (($retry['execution_mode'] ?? null) !== 'local'
            || ($retry['activity_execution_id'] ?? null) !== $executionId
            || ($retry['worker_attempt_id'] ?? null) !== $callback['activity_attempt_id']
            || ($retry['retry_after_attempt_id'] ?? null) !== ($payload['activity_attempt_id'] ?? null)
            || ($retry['retry_after_attempt'] ?? null) !== $index + 1
            || ($retry['retry_backoff_seconds'] ?? null) !== $expectedDelays[$index]) {
            throw new RuntimeException('The durable local retry must preserve the actual attempt and exact delay.');
        }
        $gap = ($callbacks[$index + 1]['monotonic_ns'] - $callback['monotonic_ns']) / 1_000_000_000;
        if ($gap < $expectedDelays[$index] || $gap > ($expectedDelays[$index] === 0 ? 0.75 : 10)) {
            throw new RuntimeException('The real callback gap must include its declared delay within the finite budget.');
        }
        $gaps[] = $gap;
    }
    $terminal = $events('ActivityCompleted')[0]['payload'] ?? [];
    if (($terminal['execution_mode'] ?? null) !== 'local' || ($terminal['activity_execution_id'] ?? null) !== $executionId
        || ($terminal['worker_attempt_id'] ?? null) !== $callbacks[$count - 1]['activity_attempt_id']
        || ($terminal['attempt_number'] ?? null) !== $count) {
        throw new RuntimeException('The terminal local result must belong to the final observed attempt.');
    }

    return $gaps;
}
