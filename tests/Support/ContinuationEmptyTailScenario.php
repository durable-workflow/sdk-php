<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * Activity workflow continuation v2 (server#1181, sdk-php#500): a workflow task completion asks
 * for the tail and the Server answers an empty one at the cached boundary, as for a completion
 * that recorded no events. The cache must keep its entry, so the run's next workflow task is a
 * sticky hit that replays the cache plus its suffix without an authoritative history read.
 */
final class ContinuationEmptyTailScenario
{
    private const COMMANDS = ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'];

    /** @return array{commands: list<array<string, mixed>>, authoritative_fetches: int, tail_requests: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $caps = ['activity_workflow_continuation' => ['version' => 1,
            'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
            'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
            'supported_commands' => self::COMMANDS, 'max_history_events' => 256, 'max_commands' => 16,
            'supported_versions' => [1, 2],
            'v2' => ['version' => 2, 'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
                'supported_commands' => self::COMMANDS, 'max_commands' => 16,
                'requires_worker_capabilities' => ['sticky_execution', 'sticky_history_suffix'],
                'tail_max_events' => 64, 'tail_max_bytes' => 262144]]];
        $event = static fn (int $sequence, string $type, array $payload): array
            => ['id' => "event-{$sequence}", 'sequence' => $sequence, 'event_type' => $type, 'payload' => $payload];
        $full = [
            $event(1, 'WorkflowStarted', ['workflow_type' => 'golden.continuation-empty-tail']),
            $event(2, 'ActivityScheduled', ['sequence' => 1, 'activity_type' => 'golden.first', 'activity_execution_id' => 'a1']),
            $event(3, 'ActivityCompleted', ['sequence' => 1, 'activity_type' => 'golden.first', 'activity_execution_id' => 'a1',
                'result' => $codec->envelope('one')]),
        ];
        $tasks = [
            ['task_id' => 'first', 'sticky_replay_mode' => 'cold_replay', 'history_after_sequence' => 0,
                'history_last_sequence' => 1, 'history_events' => array_slice($full, 0, 1)],
            ['task_id' => 'second', 'sticky_replay_mode' => 'sticky_hit_expected', 'history_after_sequence' => 1,
                'history_last_sequence' => 3, 'history_events' => array_slice($full, 1)],
        ];
        $polls = $fetches = $tailRequests = 0;
        $completions = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $tasks, $full, $caps, &$polls, &$fetches, &$tailRequests, &$completions,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $task = $tasks[$polls++] ?? null;

                return $task === null ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['poll_status' => 'leased', 'server_capabilities' => $caps,
                        'task' => $task + ['workflow_task_attempt' => 1, 'workflow_id' => 'workflow', 'run_id' => 'run',
                            'workflow_type' => 'golden.continuation-empty-tail', 'lease_owner' => 'corpus-worker',
                            'payload_codec' => 'avro']];
            }
            if (preg_match('#/workflow-tasks/(first|second)/history$#', $uri) === 1) {
                ++$fetches;

                return ['history_events' => $full, 'next_history_page_token' => ''];
            }
            if (preg_match('#/workflow-tasks/(first|second)/complete$#', $uri, $match) === 1) {
                $completions[$match[1]] = $body['commands'] ?? [];
                $answer = ['task_id' => $match[1], 'run_id' => 'run', 'run_status' => 'waiting', 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => []];
                if ($match[1] === 'first') {
                    if (($body['continuation_tail'] ?? null) !== ['version' => 2]) {
                        throw new RuntimeException('The first completion must ask for the v2 tail.');
                    }
                    ++$tailRequests;
                    $answer['continuation_tail'] = ['version' => 2, 'after_sequence' => 1, 'last_sequence' => 1,
                        'history_events' => []];
                }

                return $answer;
            }
            if (preg_match('#/workflow-tasks/(first|second)/heartbeat$#', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            throw new RuntimeException("Unexpected empty-tail scenario request {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport, codec: $codec), 'corpus-queue',
            workerId: 'corpus-worker', buildId: 'corpus-build', clock: static fn (): float => 0.0,
            enableActivityContinuation: true);
        $worker->registerWorkflow('golden.continuation-empty-tail', static fn (WorkflowContext $context): mixed
            => $context->activity('golden.first'));
        // Registration is what learns the Server's sticky_history_suffix capability.
        (new \ReflectionProperty($worker, 'stickyHistorySuffixSupported'))->setValue($worker, true);
        $worker->tick(0);
        $worker->tick(0);
        if (!isset($completions['second'])) {
            throw new RuntimeException('The empty-tail scenario did not complete its second workflow task.');
        }

        return ['commands' => array_map(static function (array $command) use ($codec): array {
            if (isset($command['result']) && is_array($command['result'])) {
                $command['result'] = $codec->decodeEnvelope($command['result']);
            }

            return $command;
        }, $completions['second']), 'authoritative_fetches' => $fetches, 'tail_requests' => $tailRequests];
    }
}
