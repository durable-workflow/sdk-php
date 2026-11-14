<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\TestCase;
use RuntimeException;

use function DurableWorkflow\Tests\Smoke\verifyLocalBackoff;

require_once __DIR__.'/smoke/local-backoff-assertions.php';

final class PublishedLocalBackoffTest extends TestCase
{
    /**
     * @param list<int> $delays
     * @return array{list<array<string, mixed>>, list<array<string, mixed>>}
     */
    private static function evidence(array $delays): array
    {
        $callbacks = [];
        $history = [['event_type' => 'ActivityScheduled']];
        $time = 1_000_000_000;
        foreach (range(1, count($delays) + 1) as $number) {
            $callbacks[] = ['workflow_id' => 'workflow', 'run_id' => 'run', 'task_id' => 'workflow-task',
                'activity_attempt_id' => 'worker-'.$number, 'attempt_number' => $number, 'monotonic_ns' => $time];
            $payload = ['execution_mode' => 'local', 'activity_execution_id' => 'execution',
                'workflow_task_id' => 'workflow-task', 'worker_attempt_id' => 'worker-'.$number,
                'activity_attempt_id' => 'native-'.$number, 'attempt_number' => $number];
            $history[] = ['event_type' => 'ActivityStarted', 'payload' => $payload];
            if (array_key_exists($number - 1, $delays)) {
                $history[] = ['event_type' => 'ActivityRetryScheduled', 'payload' => $payload + [
                    'retry_after_attempt_id' => 'native-'.$number, 'retry_after_attempt' => $number,
                    'retry_backoff_seconds' => $delays[$number - 1]]];
                $time += $delays[$number - 1] * 1_000_000_000 + 1_000_000;
            } else {
                $history[] = ['event_type' => 'ActivityCompleted', 'payload' => $payload];
            }
        }
        $history[] = ['event_type' => 'WorkflowCompleted'];

        return [$callbacks, $history];
    }

    public function testRealDelayAndDurableReceiptMustAgree(): void
    {
        foreach ([[1, 2, 2, 2], [0, 0, 0, 0]] as $delays) {
            [$callbacks, $history] = self::evidence($delays);
            self::assertCount(4, verifyLocalBackoff($callbacks, 'workflow', 'run', $history, $delays));
        }
    }

    public function testZeroAfterTheListEndsRefuses(): void
    {
        [$callbacks, $history] = self::evidence([1, 2, 0, 0]);
        $this->expectException(RuntimeException::class);
        verifyLocalBackoff($callbacks, 'workflow', 'run', $history, [1, 2, 2, 2]);
    }

    public function testRecordedDelayCannotSubstituteForElapsedTime(): void
    {
        [$callbacks, $history] = self::evidence([1, 2, 2, 2]);
        $callbacks[3]['monotonic_ns'] = $callbacks[2]['monotonic_ns'] + 1_000_000;
        $this->expectException(RuntimeException::class);
        verifyLocalBackoff($callbacks, 'workflow', 'run', $history, [1, 2, 2, 2]);
    }

    public function testForeignAttemptsAndRunCannotQualifyTiming(): void
    {
        foreach (['activity_attempt_id', 'workflow_id', 'run_id'] as $field) {
            [$callbacks, $history] = self::evidence([1, 2, 2, 2]);
            $callbacks[4][$field] = 'foreign';
            try {
                verifyLocalBackoff($callbacks, 'workflow', 'run', $history, [1, 2, 2, 2]);
                self::fail('Accepted unrelated '.$field);
            } catch (RuntimeException $error) {
                self::assertNotSame('', $error->getMessage());
            }
        }
    }

    public function testEmptyPolicyCannotAddAnUndeclaredOneSecondDelay(): void
    {
        [$callbacks, $history] = self::evidence([0, 0, 0, 0]);
        $callbacks[1]['monotonic_ns'] += 1_000_000_000;
        $this->expectException(RuntimeException::class);
        verifyLocalBackoff($callbacks, 'workflow', 'run', $history, [0, 0, 0, 0]);
    }
}
