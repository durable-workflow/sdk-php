<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** A runtime that routes no task to a sticky worker gets no sticky bookkeeping (#233, server#1017). */
final class StickyExecutionCapabilityTest extends TestCase
{
    /** @return iterable<string, array{mixed, bool}> */
    public static function runtimes(): iterable
    {
        yield 'native reports false' => [false, false];
        yield 'manifest entry reports unsupported' => [['supported' => false, 'minimum_protocol_version' => '1.18'], false];
        yield 'runtime that supports it' => [['supported' => true, 'minimum_protocol_version' => '1.18'], true];
        yield 'runtime that does not say' => [null, true];
    }

    #[DataProvider('runtimes')]
    public function testStickyBookkeepingFollowsTheAdvertisedCapability(mixed $sticky, bool $cached): void
    {
        $completion = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($sticky, &$completion): array {
            return match (true) {
                str_ends_with($uri, '/register') => ['server_capabilities' => array_filter(['sticky_execution' => $sticky], static fn (mixed $value): bool => $value !== null)],
                str_ends_with($uri, '/workflow-tasks/poll') => ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                    'workflow_id' => 'order-1', 'run_id' => 'run-1', 'workflow_type' => 'orders.flow', 'payload_codec' => 'avro',
                    'history_events' => [['event_type' => 'WorkflowStarted', 'payload' => ['workflow_type' => 'orders.flow']]],
                ]],
                str_ends_with($uri, '/task-1/heartbeat') => ['task_id' => 'task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true],
                str_ends_with($uri, '/task-1/complete') => (static function () use ($body, &$completion): array {
                    $completion = $body;

                    return ['completed' => true];
                })(),
                default => ['task' => null, 'poll_status' => 'empty'],
            };
        });
        $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'orders', workerId: 'worker-1');
        $worker->registerWorkflow('orders.flow', static function (WorkflowContext $context) use ($worker): string {
            $worker->requestShutdown();

            return 'done';
        });

        $worker->run(0);

        self::assertIsArray($completion);
        self::assertSame('complete_workflow', $completion['commands'][0]['type']);
        if ($cached) {
            self::assertArrayHasKey('sticky_cache', $completion);
            self::assertSame(1, $worker->stickyCacheMetrics()['miss']);
        } else {
            self::assertArrayNotHasKey('sticky_cache', $completion, 'no sticky claim for a runtime that cannot route on it');
            self::assertSame(['hit' => 0, 'miss' => 0, 'eviction' => 0, 'forced_cold_replay' => 0], $worker->stickyCacheMetrics());
        }
    }
}
