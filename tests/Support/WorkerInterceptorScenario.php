<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Interceptor\PassthroughWorkerInterceptor;
use DurableWorkflow\Worker\Interceptor\WorkflowTaskInterceptorContext;
use DurableWorkflow\Worker\Interceptor\WorkflowTaskOutcome;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * Replays recorded history through a Worker whose workflow-task interceptor
 * tries to swap the task and forge the outcome. The submitted commands must
 * be the ones the workflow code produced.
 */
final class WorkerInterceptorScenario
{
    /**
     * @param list<array<string, mixed>> $history
     * @param list<mixed> $input
     * @return array{commands: list<array<string, mixed>>, observed_replaying: list<bool>, interceptor_order: list<string>}
     */
    public static function run(array $history, array $input): array
    {
        $codec = new AvroPayloadCodec();
        $commands = null;
        $polls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $codec, $history, $input, &$commands, &$polls,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$polls > 1 ? ['task' => null, 'poll_status' => 'empty'] : ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'interceptor-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker',
                    'workflow_id' => 'interceptor-workflow', 'run_id' => 'interceptor-run',
                    'workflow_type' => 'golden.worker-interceptor', 'payload_codec' => 'avro',
                    'arguments' => $codec->envelope($input), 'history_events' => $history,
                ]];
            }
            if (str_ends_with($uri, '/workflow-tasks/interceptor-task/complete')) {
                $commands = $body['commands'] ?? null;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['task_id' => 'interceptor-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            throw new RuntimeException('Unexpected worker interceptor fixture request '.$method.' '.$uri);
        });
        $observed = new \ArrayObject(['replaying' => [], 'order' => []]);
        $tamper = new class ($observed, 'outer') extends PassthroughWorkerInterceptor {
            /** @param \ArrayObject<string, list<mixed>> $observed */
            public function __construct(private readonly \ArrayObject $observed, private readonly string $name)
            {
            }

            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                $this->observed['replaying'] = [...$this->observed['replaying'], $context->replaying];
                $this->observed['order'] = [...$this->observed['order'], $this->name];
                $swapped = new WorkflowTaskInterceptorContext($context->workerId, $context->taskQueue, $context->namespace,
                    ['history_events' => []] + $context->task, $context->taskId, $context->workflowType, 'forged-workflow',
                    'forged-run', $context->workflowTaskAttempt, 0, false);
                $next($swapped);

                return new WorkflowTaskOutcome([['type' => 'fail_workflow', 'message' => 'forged by interceptor']]);
            }
        };
        $audit = new class ($observed) extends PassthroughWorkerInterceptor {
            /** @param \ArrayObject<string, list<mixed>> $observed */
            public function __construct(private readonly \ArrayObject $observed)
            {
            }

            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                $this->observed['order'] = [...$this->observed['order'], 'inner'];

                return $next($context);
            }
        };
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport), 'corpus-queue',
            workerId: 'corpus-worker', interceptors: [$tamper, $audit]);
        $worker->registerWorkflow('golden.worker-interceptor',
            static fn (WorkflowContext $context, string $name): string => $context->activity('greet', [$name]).' again');
        if (!$worker->tick(0) || !is_array($commands)) {
            throw new RuntimeException('The worker interceptor fixture did not complete its workflow task.');
        }

        return [
            'commands' => array_map(static fn (array $command): array => self::decode($command, $codec), $commands),
            'observed_replaying' => $observed['replaying'],
            'interceptor_order' => $observed['order'],
        ];
    }

    /**
     * @param array<string, mixed> $command
     * @return array<string, mixed>
     */
    private static function decode(array $command, AvroPayloadCodec $codec): array
    {
        if (isset($command['result']) && is_array($command['result'])) {
            $command['result'] = $codec->decodeEnvelope($command['result']);
        }

        return $command;
    }
}
