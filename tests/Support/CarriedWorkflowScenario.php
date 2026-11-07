<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/** A completion-delivered workflow fixture executed against either SDK source revision. */
final class CarriedWorkflowScenario
{
    /** @return array{commands: list<array<string, mixed>>, activity_callback_count: int, workflow_poll_requests: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $polls = $activities = $callbacks = 0;
        $commands = null;
        $clock = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $caps = ['activity_completion_workflow_poll' => true];
        $workflow = ['task' => ['task_id' => 'workflow', 'workflow_task_attempt' => 1,
            'lease_owner' => 'corpus-worker', 'workflow_id' => 'workflow-instance', 'run_id' => 'run',
            'workflow_type' => 'corpus.workflow', 'payload_codec' => 'avro', 'history_events' => [],
            'lease_expires_at' => $clock->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps,
            'server_time' => $clock->format('Y-m-d\TH:i:s.u\Z')];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $workflow, $caps, &$polls, &$activities, &$commands,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$polls === 1 ? ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps] : $workflow;
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activities === 1 ? ['task' => ['task_id' => 'activity', 'activity_attempt_id' => 'attempt',
                    'lease_owner' => 'corpus-worker', 'activity_type' => 'corpus.activity', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased', 'server_capabilities' => $caps] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity/complete')) {
                if (!isset($body['workflow_poll'])) return ['completed' => true];
                if ($body['workflow_poll']['worker_id'] !== 'corpus-worker'
                    || $body['workflow_poll']['task_queue'] !== 'corpus-queue'
                    || $body['workflow_poll']['task_kinds'] !== ['workflow']
                    || $body['workflow_poll']['timeout_seconds'] !== 0) {
                    throw new RuntimeException('The carried workflow request changed its worker-only slot binding.');
                }
                return ['completed' => true, 'workflow_poll' => ['leased' => true, 'answer' => $workflow]];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow/complete')) {
                $commands = $body['commands'];
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow/heartbeat')) {
                return ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) return ['task' => null, 'poll_status' => 'empty'];
            throw new RuntimeException('Unexpected carried workflow fixture request.');
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport), 'corpus-queue', workerId: 'corpus-worker');
        $worker->registerWorkflow('corpus.workflow', static fn (WorkflowContext $context): string => 'carried-workflow');
        $worker->registerActivity('corpus.activity', static function (ActivityContext $context) use (&$callbacks): int {
            return ++$callbacks;
        });
        $worker->tick(0);
        $worker->tick(0);
        if (!is_array($commands)) throw new RuntimeException('The carried workflow fixture did not complete its workflow.');
        foreach ($commands as &$command) {
            if (isset($command['result'])) $command['result'] = $codec->decodeEnvelope($command['result']);
        }
        unset($command);
        return ['commands' => $commands, 'activity_callback_count' => $callbacks, 'workflow_poll_requests' => $polls];
    }
}
