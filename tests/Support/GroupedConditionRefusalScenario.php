<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

/**
 * sdk-php#278: a native Server without grouped_condition_waits. A new condition wait inside
 * all() fails the workflow task with a clear reason instead of sending a completion the
 * Server would refuse.
 */
final class GroupedConditionRefusalScenario
{
    /** @return array{failure_reason: ?string, completed: bool, worker_kept_polling: bool} */
    public static function run(): array
    {
        $completion = $failure = null;
        $polls = 0;
        $worker = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completion, &$failure, &$polls, &$worker): array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 30];
            }
            if (str_contains($uri, '/cluster/info')) {
                return ['implementation' => 'rust', 'version' => '3.4.0'];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if (++$polls > 1) {
                    $worker?->requestShutdown();

                    return ['task' => null, 'poll_status' => 'empty'];
                }

                return ['poll_status' => 'leased', 'task' => ['task_id' => 'task', 'workflow_task_attempt' => 1,
                    'workflow_id' => 'w', 'run_id' => 'r', 'workflow_type' => 'golden.grouped-condition', 'lease_owner' => 'corpus-worker',
                    'payload_codec' => 'avro', 'history_events' => []]];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/complete')) {
                $completion = $body;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/fail')) {
                $failure = $body['failure'] ?? $body;

                return ['failed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/heartbeat')) {
                return ['task_id' => 'task', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }

            return ['task' => null, 'poll_status' => 'empty', 'acknowledged' => true];
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport), 'corpus-queue',
            workerId: 'corpus-worker', clock: static fn (): float => 0.0);
        $worker->registerWorkflow('golden.grouped-condition', static fn (WorkflowContext $context): array => $context->all([
            $context->deferCondition(static fn (): bool => $context->signals('approve') !== [], 'approval', 300),
            $context->deferTimer(3),
        ]));
        $worker->run(0);
        $message = is_array($failure) ? (string) ($failure['message'] ?? '') : '';

        return [
            'failure_reason' => preg_match('/(grouped_condition_wait_unsupported)/', $message, $match) === 1 ? $match[1] : null,
            'completed' => $completion !== null,
            'worker_kept_polling' => $polls > 1,
        ];
    }
}
