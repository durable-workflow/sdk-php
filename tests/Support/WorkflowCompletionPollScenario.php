<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/** The same completion/poll fixture runs against the predecessor and candidate SDK. */
final class WorkflowCompletionPollScenario
{
    /** @return array{commands: list<array<string, mixed>>, next_workflow_callback_count: int, workflow_poll_requests: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $polls = $callbacks = 0;
        $commands = null;
        $answer = static function (string $name) use ($codec): array {
            $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
            return ['task' => ['task_id' => $name, 'workflow_task_attempt' => 1,
                'lease_owner' => 'corpus-worker', 'workflow_id' => 'workflow-'.$name, 'run_id' => 'run-'.$name,
                'workflow_type' => 'corpus.workflow', 'payload_codec' => 'avro', 'arguments' => $codec->envelope([$name]),
                'history_events' => [], 'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
                'poll_status' => 'leased', 'protocol_version' => '1.20',
                'server_capabilities' => $name === 'first' ? ['workflow_task_completion_workflow_poll' => true] : [],
                'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
        };
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $answer, &$polls, &$commands,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) return $answer(++$polls === 1 ? 'first' : 'next');
            if (str_ends_with($uri, '/workflow-tasks/first/complete')) {
                $commands = $body['commands'];
                if (!isset($body['workflow_poll'])) return ['completed' => true];
                if ($body['workflow_poll']['worker_id'] !== 'corpus-worker'
                    || $body['workflow_poll']['task_queue'] !== 'corpus-queue'
                    || $body['workflow_poll']['task_kinds'] !== ['workflow']
                    || $body['workflow_poll']['timeout_seconds'] !== 0) {
                    throw new RuntimeException('The successor workflow poll changed its original worker binding.');
                }
                return ['completed' => true, 'workflow_poll' => ['leased' => true, 'answer' => $answer('next')]];
            }
            if (str_ends_with($uri, '/workflow-tasks/next/complete')) return ['completed' => true];
            if (str_ends_with($uri, '/heartbeat')) {
                $name = str_contains($uri, '/first/') ? 'first' : 'next';
                return ['task_id' => $name, 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) return ['task' => null, 'poll_status' => 'empty'];
            throw new RuntimeException('Unexpected successor workflow fixture request.');
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport), 'corpus-queue', workerId: 'corpus-worker');
        $worker->registerWorkflow('corpus.workflow', static function (WorkflowContext $context, string $name) use (&$callbacks): string {
            if ($name === 'next') ++$callbacks;
            return $name;
        });
        $worker->tick(0);
        $worker->tick(0);
        if (!is_array($commands)) throw new RuntimeException('The successor workflow fixture did not complete its parent.');
        foreach ($commands as &$command) {
            if (isset($command['result'])) $command['result'] = $codec->decodeEnvelope($command['result']);
        }
        unset($command);
        return ['commands' => $commands, 'next_workflow_callback_count' => $callbacks, 'workflow_poll_requests' => $polls];
    }
}
