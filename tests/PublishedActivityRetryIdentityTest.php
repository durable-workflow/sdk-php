<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\TestCase;
use RuntimeException;

use function DurableWorkflow\Tests\Smoke\verifyActivityRetryIdentity;

require_once __DIR__.'/smoke/activity-retry-identity-assertions.php';

final class PublishedActivityRetryIdentityTest extends TestCase
{
    /** @return array{list<array<string, mixed>>, list<array<string, mixed>>} */
    private static function evidence(): array
    {
        $callbacks = [];
        foreach ([1, 2] as $attempt) {
            $callbacks[] = ['activity_execution_id' => 'execution', 'workflow_id' => 'workflow', 'run_id' => 'run',
                'task_id' => 'task-'.$attempt, 'activity_attempt_id' => 'attempt-'.$attempt, 'attempt_number' => $attempt];
        }
        $started = static fn (array $callback): array => ['event_type' => 'ActivityStarted', 'payload' => [
            'activity_execution_id' => $callback['activity_execution_id'], 'task' => ['id' => $callback['task_id']],
            'activity_attempt_id' => $callback['activity_attempt_id'], 'attempt_number' => $callback['attempt_number'],
        ]];
        $history = [
            ['event_type' => 'ActivityScheduled'], $started($callbacks[0]),
            ['event_type' => 'ActivityRetryScheduled', 'payload' => [
                'activity_execution_id' => 'execution', 'retry_of_task_id' => 'task-1', 'retry_task_id' => 'task-2',
                'retry_after_attempt_id' => 'attempt-1', 'retry_after_attempt' => 1, 'max_attempts' => 2, 'retry_backoff_seconds' => 0,
            ]],
            $started($callbacks[1]), ['event_type' => 'ActivityCompleted'], ['event_type' => 'WorkflowCompleted'],
        ];

        return [$callbacks, $history];
    }

    public function testBoundCallbackPairAndDurableRetryReceipt(): void
    {
        [$callbacks, $history] = self::evidence();
        verifyActivityRetryIdentity($callbacks, 'workflow', 'run', $history);
        self::assertCount(2, $callbacks);
    }

    public function testMissingLogicalIdentityCannotBorrowTaskIdentity(): void
    {
        [$callbacks, $history] = self::evidence();
        $callbacks[0]['activity_execution_id'] = null;
        $this->expectException(RuntimeException::class);
        verifyActivityRetryIdentity($callbacks, 'workflow', 'run', $history);
    }

    public function testChangedLogicalIdentityAndReusedAttemptIdentitiesRefuse(): void
    {
        foreach (['activity_execution_id', 'workflow_id', 'run_id', 'task_id', 'activity_attempt_id', 'attempt_number'] as $field) {
            [$callbacks, $history] = self::evidence();
            $callbacks[1][$field] = in_array($field, ['task_id', 'activity_attempt_id', 'attempt_number'], true)
                ? $callbacks[0][$field] : 'foreign';
            try {
                verifyActivityRetryIdentity($callbacks, 'workflow', 'run', $history);
                self::fail('Accepted changed or reused '.$field);
            } catch (RuntimeException $error) {
                self::assertNotSame('', $error->getMessage());
            }
        }
    }

    public function testHistoryFromAnotherAttemptCannotQualifyTheCallbackPair(): void
    {
        [$callbacks, $history] = self::evidence();
        $history[2]['payload']['retry_task_id'] = 'foreign-task';
        $this->expectException(RuntimeException::class);
        verifyActivityRetryIdentity($callbacks, 'workflow', 'run', $history);
    }

    public function testInventedSecondAttemptIdentityRefuses(): void
    {
        [$callbacks, $history] = self::evidence();
        $callbacks[1]['activity_attempt_id'] = 'unrelated-nonempty-attempt';
        $this->expectException(RuntimeException::class);
        verifyActivityRetryIdentity($callbacks, 'workflow', 'run', $history);
    }
}
