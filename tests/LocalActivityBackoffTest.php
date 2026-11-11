<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * .github#219: once attempts outrun backoff_seconds, a local activity repeats
 * the final entry, the same rule server-scheduled activities follow, instead
 * of retrying with no delay.
 */
final class LocalActivityBackoffTest extends TestCase
{
    /** @return iterable<string, array{array<string, mixed>, list<int>}> */
    public static function policies(): iterable
    {
        yield 'short list repeats its final entry' => [['max_attempts' => 5, 'backoff_seconds' => [1, 2]], [1, 2, 2, 2, 0]];
        yield 'full list is used as written' => [['max_attempts' => 3, 'backoff_seconds' => [3, 4]], [3, 4, 0]];
        yield 'no list keeps immediate retries' => [['max_attempts' => 3], [0, 0, 0]];
        yield 'zero final entry stays immediate' => [['max_attempts' => 4, 'backoff_seconds' => [5, 0]], [5, 0, 0, 0]];
    }

    /**
     * @param array<string, mixed> $policy
     * @param list<int> $expected backoff recorded on each attempt; the last attempt never waits
     */
    #[DataProvider('policies')]
    public function testRetriesRepeatTheFinalBackoffEntry(array $policy, array $expected): void
    {
        $completion = null;
        $slept = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completion): array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['task' => ['task_id' => 'task', 'workflow_id' => 'workflow', 'run_id' => 'run',
                    'workflow_type' => 'flow', 'lease_owner' => 'owner', 'payload_codec' => 'avro', 'history_events' => []]];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/complete')) {
                $completion = $body;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/heartbeat')) {
                return ['task_id' => 'task', 'workflow_task_attempt' => 1, 'lease_owner' => 'owner', 'renewed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/fail')) {
                self::fail('Workflow callback failed: '.($body['failure']['message'] ?? 'missing failure message'));
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'queue',
            workerId: 'owner',
            sleeper: static function (int $microseconds) use (&$slept): void { $slept[] = $microseconds; },
        );
        $worker->registerWorkflow('flow', static fn (WorkflowContext $context): mixed
            => $context->localActivity('always-fails', [], ['retry_policy' => $policy]));
        $worker->registerActivity('always-fails', static function (ActivityContext $context): never {
            throw new RuntimeException('down');
        });

        $worker->tick(0);

        self::assertIsArray($completion);
        $recorded = $completion['commands'][0];
        self::assertSame('record_local_activity', $recorded['type']);
        self::assertSame($expected, array_map(
            static fn (array $attempt): int => $attempt['backoff_seconds'] ?? 0,
            $recorded['attempts'],
        ));
        self::assertSame(array_sum($expected) * 1_000_000, array_sum($slept));
    }
}
