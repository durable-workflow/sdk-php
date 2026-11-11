<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * sticky_history_suffix v1 (server#1017): one worker claims the same run twice. The second
 * answer is a suffix that doesn't continue the cached prefix, so the worker must refetch full
 * history from token 0 rather than replay the cache plus the suffix.
 */
final class StickySuffixScenario
{
    /** @return array{commands: list<array<string, mixed>>, authoritative_fetches: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $event = static fn (int $sequence, string $type, array $payload): array
            => ['sequence' => $sequence, 'event_type' => $type, 'payload' => $payload];
        $full = [
            $event(1, 'WorkflowStarted', ['workflow_type' => 'golden.sticky-suffix']),
            $event(2, 'ActivityScheduled', ['sequence' => 1, 'activity_type' => 'golden.first', 'activity_execution_id' => 'a1']),
            $event(3, 'ActivityCompleted', ['sequence' => 1, 'activity_type' => 'golden.first', 'activity_execution_id' => 'a1',
                'result' => $codec->envelope('one')]),
            $event(4, 'ActivityScheduled', ['sequence' => 2, 'activity_type' => 'golden.second', 'activity_execution_id' => 'a2']),
            $event(5, 'ActivityCompleted', ['sequence' => 2, 'activity_type' => 'golden.second', 'activity_execution_id' => 'a2',
                'result' => $codec->envelope('two')]),
        ];
        $tasks = [
            ['task_id' => 'first', 'sticky_replay_mode' => 'cold_replay', 'history_after_sequence' => 0,
                'history_last_sequence' => 3, 'history_events' => array_slice($full, 0, 3)],
            // Claims to continue after 4, but this worker cached only through 3.
            ['task_id' => 'second', 'sticky_replay_mode' => 'sticky_hit_expected', 'history_after_sequence' => 4,
                'history_last_sequence' => 5, 'history_events' => [
                    $event(5, 'ActivityCompleted', ['sequence' => 2, 'activity_type' => 'golden.second',
                        'activity_execution_id' => 'a2', 'result' => $codec->envelope('stale')]),
                ]],
        ];
        $polls = $fetches = 0;
        $completions = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $tasks, $full, &$polls, &$fetches, &$completions,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $task = $tasks[$polls++] ?? null;

                return $task === null ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['poll_status' => 'leased', 'task' => $task + ['workflow_task_attempt' => 1, 'workflow_id' => 'workflow',
                        'run_id' => 'run', 'workflow_type' => 'golden.sticky-suffix', 'lease_owner' => 'corpus-worker',
                        'payload_codec' => 'avro']];
            }
            if (preg_match('#/workflow-tasks/(first|second)/history$#', $uri) === 1) {
                if (($body['next_history_page_token'] ?? null) !== base64_encode('0')) {
                    throw new RuntimeException('A rejected suffix must refetch history from token 0.');
                }
                ++$fetches;

                return ['history_events' => $full, 'next_history_page_token' => ''];
            }
            if (preg_match('#/workflow-tasks/(first|second)/complete$#', $uri, $match) === 1) {
                $completions[$match[1]] = $body['commands'] ?? [];
                if ($match[1] === 'first' && ($body['sticky_cache']['history_last_sequence'] ?? null) !== 3) {
                    throw new RuntimeException('The completion must report the last cached sequence.');
                }

                return ['completed' => true];
            }
            if (preg_match('#/workflow-tasks/(first|second)/heartbeat$#', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            throw new RuntimeException("Unexpected sticky scenario request {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport, codec: $codec), 'corpus-queue',
            workerId: 'corpus-worker', buildId: 'corpus-build', clock: static fn (): float => 0.0);
        $worker->registerWorkflow('golden.sticky-suffix', static function (WorkflowContext $context): array {
            return [$context->activity('golden.first'), $context->activity('golden.second')];
        });
        // Registration is what learns the Server's sticky_history_suffix capability.
        $registered = new \ReflectionProperty($worker, 'stickyHistorySuffixSupported');
        $registered->setValue($worker, true);
        $worker->tick(0);
        $worker->tick(0);
        if (!isset($completions['second'])) {
            throw new RuntimeException('The sticky scenario did not complete its second workflow task.');
        }

        return ['commands' => array_map(static function (array $command) use ($codec): array {
            if (isset($command['result']) && is_array($command['result'])) {
                $command['result'] = $codec->decodeEnvelope($command['result']);
            }

            return $command;
        }, $completions['second']), 'authoritative_fetches' => $fetches];
    }
}
