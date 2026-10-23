<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Failure\LocalFailureReports;
use DurableWorkflow\Transport\JsonDocument;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class LocalFailureReportTest extends TestCase
{
    public function testInitialAndColdWorkflowBranchesObserveTheSameNormalizedFailure(): void
    {
        $observations = [];
        $handler = static function (WorkflowContext $context) use (&$observations): string {
            try {
                $context->localActivity('local-failure', [], ['retry_policy' => ['max_attempts' => 1]]);
                return 'unexpected-success';
            } catch (ActivityFailed $failure) {
                $observations[] = [$failure->getMessage(), $failure->failureType, $failure->nonRetryable];
                return str_contains($failure->getMessage(), '[truncated failure report;') ? 'bounded-branch' : 'unbounded-branch';
            }
        };
        $calls = 0;
        $original = str_repeat('雪', 1024 * 1024);
        $local = static function () use (&$calls, $original): array {
            ++$calls;
            return ['outcome' => 'failed', 'message' => $original, 'exception_type' => 'OriginalLocalFailure',
                'non_retryable' => true, 'attempts' => [['attempt_number' => 1, 'outcome' => 'failed', 'message' => $original,
                    'exception_type' => 'OriginalLocalFailure', 'non_retryable' => true, 'duration_ms' => 0, 'heartbeats' => []]]];
        };
        $codec = new AvroPayloadCodec();
        $fresh = (new Replayer($codec))->replay($handler, [], [], 'queue', localActivityExecutor: $local);
        self::assertNull($fresh->terminalFailure);
        self::assertSame(['record_local_activity', 'complete_workflow'], array_column($fresh->commands, 'type'));
        $recorded = $fresh->commands[0];
        self::assertLessThanOrEqual(63488, strlen($recorded['message']));
        self::assertSame(1, preg_match('//u', $recorded['message']));
        self::assertStringContainsString('3145728', $recorded['message']);
        self::assertSame($recorded['message'], $recorded['attempts'][0]['message']);
        self::assertSame($recorded['message'], $observations[0][0]);
        $history = [
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'local-failure',
                'execution_mode' => 'local', 'arguments' => $codec->envelope([])]],
            ['event_type' => 'ActivityFailed', 'payload' => $recorded + ['sequence' => 1, 'execution_mode' => 'local']],
        ];
        $cold = (new Replayer($codec))->replay($handler, $history, [], 'queue', localActivityExecutor: static function (): never {
            self::fail('Cold replay repeated the local side effect.');
        });
        self::assertNull($cold->terminalFailure);
        self::assertSame(['complete_workflow'], array_column($cold->commands, 'type'));
        self::assertSame($observations[0], $observations[1]);
        self::assertSame($fresh->commands[1]['result'], $cold->commands[0]['result']);
        self::assertSame(1, $calls);
    }

    public function testAggregateAttemptMessagesFitBeforeObservationWithoutChangingRetries(): void
    {
        $message = str_repeat("\0雪", 1000000);
        $attempts = [];
        for ($number = 1; $number <= 30; ++$number) {
            $attempts[] = ['attempt_id' => 'original-attempt-'.$number, 'attempt_number' => $number,
                'outcome' => 'failed', 'message' => $message, 'exception_type' => 'OriginalFailure',
                'non_retryable' => false, 'retry_reason' => 'failure', 'backoff_seconds' => 3, 'heartbeats' => []];
        }
        $outcome = ['outcome' => 'failed', 'message' => $message, 'exception_type' => 'OriginalFailure',
            'non_retryable' => false, 'attempts' => $attempts];
        $bounded = LocalFailureReports::bound($outcome, 128 * 1024);
        self::assertLessThanOrEqual(128 * 1024, JsonDocument::size($bounded));
        self::assertCount(30, $bounded['attempts']);
        foreach ($bounded['attempts'] as $index => $attempt) {
            unset($attempt['message']);
            $original = $attempts[$index];
            unset($original['message']);
            self::assertSame($original, $attempt);
        }
        self::assertSame($bounded['message'], $bounded['attempts'][29]['message']);
        self::assertSame($bounded, LocalFailureReports::bound($bounded, 128 * 1024));
    }
}
