<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Smoke;

use RuntimeException;

/**
 * @param list<array<string, mixed>> $callbacks
 * @param list<array<string, mixed>> $history
 */
function verifyActivityRetryIdentity(array $callbacks, string $workflowId, string $runId, array $history): void
{
    if (count($callbacks) !== 2 || array_column($callbacks, 'attempt_number') !== [1, 2]) {
        throw new RuntimeException('Require exactly the first and second real activity callbacks.');
    }
    foreach ($callbacks as $callback) {
        foreach (['activity_execution_id', 'workflow_id', 'run_id', 'task_id', 'activity_attempt_id'] as $field) {
            if (!is_string($callback[$field] ?? null) || $callback[$field] === '') {
                throw new RuntimeException('Missing received callback identity: '.$field);
            }
        }
        if ($callback['workflow_id'] !== $workflowId || $callback['run_id'] !== $runId) {
            throw new RuntimeException('The callback must belong to the exact started workflow and run.');
        }
    }
    [$first, $second] = $callbacks;
    if ($first['activity_execution_id'] !== $second['activity_execution_id']
        || $first['task_id'] === $second['task_id']
        || $first['activity_attempt_id'] === $second['activity_attempt_id']) {
        throw new RuntimeException('A retry must keep its logical execution and change task and attempt identity.');
    }
    foreach (['ActivityScheduled' => 1, 'ActivityStarted' => 2, 'ActivityRetryScheduled' => 1,
        'ActivityCompleted' => 1, 'WorkflowCompleted' => 1, 'ActivityFailed' => 0, 'WorkflowFailed' => 0] as $type => $count) {
        $events = array_values(array_filter($history, static fn (array $event): bool => ($event['event_type'] ?? null) === $type));
        if (count($events) !== $count) {
            throw new RuntimeException('Unexpected real history count for '.$type);
        }
        if ($type !== 'ActivityRetryScheduled') {
            continue;
        }
        $payload = $events[0]['payload'] ?? [];
        foreach ([
            'activity_execution_id' => $first['activity_execution_id'],
            'retry_of_task_id' => $first['task_id'], 'retry_task_id' => $second['task_id'],
            'retry_after_attempt_id' => $first['activity_attempt_id'],
            'retry_after_attempt' => 1, 'max_attempts' => 2, 'retry_backoff_seconds' => 0,
        ] as $field => $expected) {
            if (($payload[$field] ?? null) !== $expected) {
                throw new RuntimeException('The durable retry receipt must bind the actual callbacks: '.$field);
            }
        }
    }
    $started = array_values(array_filter($history,
        static fn (array $event): bool => ($event['event_type'] ?? null) === 'ActivityStarted'));
    foreach ($callbacks as $index => $callback) {
        $payload = $started[$index]['payload'] ?? [];
        if (($payload['activity_execution_id'] ?? null) !== $callback['activity_execution_id']
            || ($payload['task']['id'] ?? null) !== $callback['task_id']
            || ($payload['activity_attempt_id'] ?? null) !== $callback['activity_attempt_id']
            || ($payload['attempt_number'] ?? null) !== $callback['attempt_number']) {
            throw new RuntimeException('Each callback must match its durable ActivityStarted identities.');
        }
    }
}
