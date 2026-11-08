<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * #269 qualification witness: the @internal delivered-history observer sees
 * each workflow task attempt's complete replay input (every delivered page)
 * and its completion commands, and is inert: with no observer, a recording
 * observer or a throwing one, the Worker sends exactly the same requests.
 */
final class WorkerDeliveredHistoryObserverTest extends TestCase
{
    public function testTheObserverSeesTheReplayInputAcrossPagesAndTheCompletionCommands(): void
    {
        $seen = [];
        [$requests] = self::run(static function (string $kind, string $taskId, int $attempt, array $items) use (&$seen): void {
            $seen[] = [$kind, $taskId, $attempt, $items];
        });

        self::assertSame(['history', 'completion'], array_column($seen, 0));
        self::assertSame([['task-1', 3], ['task-1', 3]], array_map(static fn (array $s): array => [$s[1], $s[2]], $seen));
        self::assertSame([1, 2], array_column($seen[0][3], 'sequence'), 'the poll event and the continuation-page event');
        $completion = array_values(array_filter($requests, static fn (array $r): bool => str_ends_with($r['uri'], '/task-1/complete')));
        self::assertCount(1, $completion);
        self::assertSame($completion[0]['body']['commands'], $seen[1][3]);
        self::assertSame('complete_workflow', $seen[1][3][0]['type']);
    }

    public function testTheObserverIsInertAndItsFailuresAreIgnored(): void
    {
        [$without] = self::run(null);
        [$recording] = self::run(static function (): void {
        });
        [$throwing] = self::run(static function (): void {
            throw new RuntimeException('observer failure');
        });

        self::assertSame($without, $recording);
        self::assertSame($without, $throwing);
        self::assertNotSame([], array_filter($without, static fn (array $r): bool => str_ends_with($r['uri'], '/task-1/complete')));
    }

    /**
     * One workflow task delivered as a claimed poll page plus one continuation
     * page; the workflow returns the delivered signals.
     *
     * @param (\Closure(string, string, int, list<array<string, mixed>>): void)|null $observer
     * @return array{list<array{method: string, uri: string, body: ?array<string, mixed>}>}
     */
    private static function run(?\Closure $observer): array
    {
        $codec = new AvroPayloadCodec();
        $signal = static fn (int $sequence, string $value): array => ['id' => 'e'.$sequence, 'sequence' => $sequence,
            'event_type' => 'SignalReceived', 'recorded_at' => sprintf('2026-11-08T07:00:0%d.000000Z', $sequence),
            'payload' => ['signal_name' => 'go', 'arguments' => $codec->envelope([$value])]];
        $polled = false;
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$polled, $signal): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if ($polled) {
                    return ['task' => null, 'poll_status' => 'empty'];
                }
                $polled = true;

                return ['poll_status' => 'leased', 'task' => ['task_id' => 'task-1', 'workflow_task_attempt' => 3,
                    'lease_owner' => 'worker-1', 'workflow_id' => 'observer', 'run_id' => 'run-1',
                    'workflow_type' => 'observer.workflow', 'payload_codec' => 'avro',
                    'history_events' => [$signal(1, 'first')], 'next_history_page_token' => 'page-2']];
            }
            if (str_ends_with($uri, '/task-1/history')) {
                return ['history_events' => [$signal(2, 'second')], 'next_history_page_token' => ''];
            }
            if (str_ends_with($uri, '/task-1/heartbeat')) {
                return ['task_id' => 'task-1', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1', 'renewed' => true];
            }
            if (str_ends_with($uri, '/task-1/complete')) {
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            throw new RuntimeException("Unexpected request {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'observers', workerId: 'worker-1');
        $worker->registerWorkflow('observer.workflow', static fn (WorkflowContext $context): array => $context->signals('go'));
        $worker->observeDeliveredWorkflowHistory($observer);
        self::assertTrue($worker->tick(0));

        return [array_map(static fn (array $r): array => ['method' => $r['method'], 'uri' => $r['uri'], 'body' => $r['body']],
            $transport->requests)];
    }
}
