<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\ReplaySourceWorkflow;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

final class ReplaySourceLocationTest extends TestCase
{
    public function testReorderedWorkflowReportsItsAuthoredCallDuringReplayAndInTheWorkerFailure(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $workflow = new ReplaySourceWorkflow();
        $original = $replayer->replay([$workflow, 'original'], [], [], 'php-workers');
        self::assertSame('start_timer', $original->commands[0]['type']);
        $history = [
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'delay_seconds' => 5]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'delay_seconds' => 5]],
        ];
        $replayed = $replayer->replay([$workflow, 'original'], $history, [], 'php-workers');
        self::assertSame('schedule_activity', $replayed->commands[0]['type']);

        $method = new ReflectionMethod($workflow, 'greet');
        $expected = [
            'function' => ReplaySourceWorkflow::class.'::greet',
            'file' => basename((string) $method->getFileName()),
            'line' => (int) $method->getStartLine() + 2,
        ];
        $site = sprintf('%s (%s:%d)', $expected['function'], $expected['file'], $expected['line']);
        try {
            $replayer->replay([$workflow, 'reordered'], $history, [], 'php-workers');
            self::fail('Replay accepted a reordered operation.');
        } catch (NonDeterministicWorkflow $mismatch) {
            self::assertSame($expected, $mismatch->workflowSource());
            self::assertSame(1, $mismatch->sequence);
            self::assertSame('timer', $mismatch->expected);
            self::assertSame('activity', $mismatch->actual);
            self::assertStringContainsString($site, $mismatch->getMessage());
            self::assertStringNotContainsString(__DIR__, $mismatch->getMessage());
        }

        $task = [
            'task_id' => 'reorder-task',
            'workflow_task_attempt' => 1,
            'lease_owner' => 'source-worker',
            'workflow_id' => 'approval',
            'run_id' => 'approval-run',
            'workflow_type' => 'source.workflow',
            'arguments' => $codec->envelope([]),
            'history_events' => $history,
            'payload_codec' => 'avro',
        ];
        $delivered = false;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($task, &$delivered): array {
            if (str_ends_with($uri, '/workflow-tasks/poll') && !$delivered) {
                $delivered = true;

                return ['poll_status' => 'leased', 'task' => $task];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['poll_status' => 'empty', 'task' => null];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return [
                    'task_id' => $task['task_id'],
                    'workflow_task_attempt' => $task['workflow_task_attempt'],
                    'lease_owner' => $task['lease_owner'],
                    'renewed' => true,
                    'reason' => null,
                ];
            }
            if (str_ends_with($uri, '/fail')) {
                return ['failed' => true];
            }

            self::fail("Unexpected request {$method} {$uri}.");
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'php-workers', workerId: 'source-worker');
        $worker->registerWorkflow('source.workflow', [$workflow, 'reordered']);
        self::assertTrue($worker->tick(0));
        $failures = array_values(array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/workflow-tasks/reorder-task/fail'),
        ));
        self::assertCount(1, $failures);
        $failure = $failures[0]['body']['failure'];
        self::assertSame(NonDeterministicWorkflow::class, $failure['type'], $failure['message']);
        self::assertSame(1, $failure['sequence']);
        self::assertStringContainsString($site, $failure['message']);
        self::assertStringNotContainsString(__DIR__, $failure['message']);
        self::assertArrayNotHasKey('stack_trace', $failure);
    }
}
