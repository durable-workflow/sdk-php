<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ActivityExecutionIdentityTest extends TestCase
{
    /** @return iterable<string, array{array<string, mixed>, ?string}> */
    public static function receivedIdentities(): iterable
    {
        yield 'canonical identity' => [['activity_execution_id' => 'logical-execution'], 'logical-execution'];
        yield 'opaque spelling' => [['activity_execution_id' => '01AbC'], '01AbC'];
        yield 'zero string' => [['activity_execution_id' => '0'], '0'];
        yield 'legacy omission' => [[], null];
        yield 'explicit null' => [['activity_execution_id' => null, 'execution_id' => 'unrelated'], null];
        yield 'empty' => [['activity_execution_id' => ''], null];
        yield 'numeric' => [['activity_execution_id' => 42], null];
        yield 'boolean' => [['activity_execution_id' => true], null];
        yield 'unrelated aliases' => [['execution_id' => 'other', 'task_id' => 'task', 'activity_attempt_id' => 'attempt'], null];
    }

    #[DataProvider('receivedIdentities')]
    public function testOnlyTheReceivedLogicalIdentityIsExposed(array $metadata, ?string $expected): void
    {
        $context = new ActivityContext(new Client('https://server.example'), 'task', 'attempt', 'owner', 'activity', 1,
            taskMetadata: $metadata);
        self::assertSame($expected, $context->activityExecutionId);
        self::assertSame('task', $context->taskId);
        self::assertSame('attempt', $context->activityAttemptId);
    }

    public function testOldConstructorsRemainValidWithoutInventingIdentity(): void
    {
        $context = new ActivityContext(new Client('https://server.example'), 'task', 'attempt', 'owner', 'activity', 1);
        self::assertNull($context->activityExecutionId);
    }

    public function testReceivedRetryClaimsKeepLogicalIdentityAndDifferentAttempts(): void
    {
        $polls = 0;
        $failures = 0;
        $completions = 0;
        $seen = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$polls, &$failures, &$completions): array {
            if (str_ends_with($uri, '/worker/register')) { return ['registered' => true]; }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                ++$polls;
                return ['task' => ['task_id' => 'task-'.$polls, 'activity_attempt_id' => 'attempt-'.$polls,
                    'activity_execution_id' => 'logical-execution', 'workflow_id' => 'original-workflow', 'run_id' => 'original-run',
                    'lease_owner' => 'owner', 'activity_type' => 'identity', 'attempt_number' => $polls, 'payload_codec' => 'avro']];
            }
            if (str_ends_with($uri, '/fail')) { ++$failures; return ['failed' => true]; }
            if (str_ends_with($uri, '/complete')) { ++$completions; return ['completed' => true]; }
            if (str_ends_with($uri, '/poll')) { return ['task' => null]; }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'owner');
        $worker->registerActivity('identity', static function (ActivityContext $context) use (&$seen): string {
            $seen[] = [$context->activityExecutionId, $context->activityAttemptId, $context->workflowId, $context->runId];
            if ($context->attemptNumber === 1) {
                throw new RuntimeException('Retry the received activity.');
            }
            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        self::assertSame([
            ['logical-execution', 'attempt-1', 'original-workflow', 'original-run'],
            ['logical-execution', 'attempt-2', 'original-workflow', 'original-run'],
        ], $seen);
        self::assertSame(1, $failures);
        self::assertSame(1, $completions);
    }
}
