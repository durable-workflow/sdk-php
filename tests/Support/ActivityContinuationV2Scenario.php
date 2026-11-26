<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * Activity workflow continuation v2 (server#1181, sdk-php#500): one opted-in worker runs a run of
 * two sequential activities. The first workflow task's tail seeds the sticky cache, and each
 * activity's v2 continuation is speculated from that cache plus its own result, with no history
 * read. The second continuation's commands come from the tail the first accepted answer returned.
 */
final class ActivityContinuationV2Scenario
{
    private const COMMANDS = ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'];

    /** @return array{commands: list<array<string, mixed>>, continuation_versions: list<mixed>, history_reads: int, ordinary_completions: int, activity_callback_count: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $caps = ['workflow_task_completion_activity_poll' => true, 'activity_completion_workflow_poll' => true,
            'activity_workflow_continuation' => ['version' => 1,
                'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
                'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
                'supported_commands' => self::COMMANDS, 'max_history_events' => 256, 'max_commands' => 16,
                'supported_versions' => [1, 2],
                'v2' => ['version' => 2, 'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
                    'supported_commands' => self::COMMANDS, 'max_commands' => 16,
                    'requires_worker_capabilities' => ['sticky_execution', 'sticky_history_suffix'],
                    'tail_max_events' => 64, 'tail_max_bytes' => 262144]]];
        $time = gmdate('Y-m-d\TH:i:s.000000\Z');
        $event = static fn (int $sequence, string $type, array $payload): array => ['id' => "event-{$sequence}",
            'sequence' => $sequence, 'event_type' => $type, 'payload' => $payload, 'recorded_at' => $time];
        $activity = static fn (string $execution, int $sequence, array $extra = []): array => ['sequence' => $sequence,
            'activity_execution_id' => $execution, 'activity_type' => 'golden.continuation-step',
            'activity' => ['type' => 'golden.continuation-step']] + $extra;
        $started = static fn (int $sequence, string $execution, int $activitySequence, string $attempt): array
            => $event($sequence, 'ActivityStarted', $activity($execution, $activitySequence, ['activity_attempt_id' => $attempt]));
        $completed = static fn (int $sequence, string $execution, int $activitySequence, string $attempt): array
            => $event($sequence, 'ActivityCompleted', $activity($execution, $activitySequence,
                ['activity_attempt_id' => $attempt, 'result' => $codec->envelope(7), 'payload_codec' => 'avro']));
        $lease = static fn (string $taskId, string $execution, string $attempt): array => ['leased' => true, 'answer' => [
            'task' => ['task_id' => $taskId, 'activity_attempt_id' => $attempt, 'activity_execution_id' => $execution,
                'attempt_number' => 1, 'lease_owner' => 'corpus-worker', 'activity_type' => 'golden.continuation-step',
                'payload_codec' => 'avro', 'workflow_id' => 'workflow', 'run_id' => 'run'],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps, 'server_time' => $time]];
        $accepted = static fn (string $taskId, string $attempt, string $runStatus, array $tail, array $poll): array => [
            'task_id' => $taskId, 'activity_attempt_id' => $attempt, 'outcome' => 'completed', 'recorded' => true,
            'reason' => null, 'next_task_id' => 'next',
            'continuation' => ['version' => 2, 'outcome' => 'accepted', 'workflow_task_id' => 'next', 'workflow_task_attempt' => 1,
                'completion' => ['task_id' => 'next', 'run_id' => 'run', 'run_status' => $runStatus, 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => []]],
            'continuation_tail' => $tail, 'activity_poll' => $poll];

        $polls = 0;
        $continuations = [];
        $historyReads = $ordinary = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $caps, $time, $event, $started, $completed, $lease, $accepted, $codec, &$polls, &$continuations, &$historyReads, &$ordinary,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$polls > 1 ? ['task' => null, 'poll_status' => 'empty'] : ['task' => ['task_id' => 'first',
                    'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'workflow_id' => 'workflow', 'run_id' => 'run',
                    'workflow_type' => 'golden.activity-continuation-v2', 'payload_codec' => 'avro',
                    'arguments' => $codec->envelope([]), 'sticky_replay_mode' => 'cold_replay', 'history_after_sequence' => 0,
                    'history_last_sequence' => 1, 'history_events' => [$event(1, 'WorkflowStarted', [])],
                    'lease_expires_at' => gmdate('Y-m-d\TH:i:s.000000\Z', time() + 30)],
                    'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps, 'server_time' => $time];
            }
            if (str_ends_with($uri, '/workflow-tasks/first/complete')) {
                if (($body['continuation_tail'] ?? null) !== ['version' => 2]) {
                    throw new RuntimeException('The workflow task completion must ask for the v2 tail.');
                }

                return ['task_id' => 'first', 'run_id' => 'run', 'run_status' => 'waiting', 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => ['step-1'],
                    'continuation_tail' => ['version' => 2, 'after_sequence' => 1, 'last_sequence' => 3, 'history_events' => [
                        $event(2, 'ActivityScheduled', ['sequence' => 1, 'activity_execution_id' => 'a1',
                            'activity_type' => 'golden.continuation-step', 'activity' => ['type' => 'golden.continuation-step']]),
                        $started(3, 'a1', 1, 'attempt-1')]],
                    'activity_poll' => $lease('step-1', 'a1', 'attempt-1')];
            }
            if (preg_match('~/activity-tasks/(step-\d)/continuation/complete$~', $uri, $match) === 1) {
                $continuations[] = $body['continuation'] ?? null;

                return $match[1] === 'step-1'
                    ? $accepted('step-1', 'attempt-1', 'waiting', ['version' => 2, 'after_sequence' => 3, 'last_sequence' => 6,
                        'history_events' => [$completed(4, 'a1', 1, 'attempt-1'), $event(5, 'ActivityScheduled',
                            ['sequence' => 2, 'activity_execution_id' => 'a2', 'activity_type' => 'golden.continuation-step',
                                'activity' => ['type' => 'golden.continuation-step']]), $started(6, 'a2', 2, 'attempt-2')]],
                        $lease('step-2', 'a2', 'attempt-2'))
                    : $accepted('step-2', 'attempt-2', 'completed', ['version' => 2, 'after_sequence' => 6, 'last_sequence' => 8,
                        'history_events' => [$completed(7, 'a2', 2, 'attempt-2'), $event(8, 'WorkflowCompleted', [])]],
                        ['leased' => false]);
            }
            if (str_ends_with($uri, '/continuation/history')) {
                ++$historyReads;

                return ['continuation' => ['version' => 1, 'eligible' => false, 'reason' => 'pending_workflow_task']];
            }
            if (preg_match('~/activity-tasks/(step-\d)/complete$~', $uri, $match) === 1) {
                ++$ordinary;

                return ['task_id' => $match[1], 'outcome' => 'completed', 'recorded' => true, 'reason' => null];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['task_id' => 'first', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            throw new RuntimeException("Unexpected continuation scenario request {$method} {$uri}");
        });
        $calls = 0;
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport, codec: $codec), 'corpus-queue',
            workerId: 'corpus-worker', buildId: 'corpus-build', enableActivityContinuation: true);
        $worker->registerWorkflow('golden.activity-continuation-v2', static fn (WorkflowContext $context): int
            => $context->activity('golden.continuation-step') + $context->activity('golden.continuation-step'));
        $worker->registerActivity('golden.continuation-step', static function (ActivityContext $context) use (&$calls): int {
            ++$calls;

            return 7;
        });
        // Registration is what learns the Server's sticky_history_suffix capability.
        (new \ReflectionProperty($worker, 'stickyHistorySuffixSupported'))->setValue($worker, true);
        $worker->tick(0);
        $last = $continuations[array_key_last($continuations)] ?? null;
        $commands = is_array($last) && is_array($last['commands'] ?? null) ? $last['commands'] : [];

        return [
            'commands' => array_map(static function (array $command) use ($codec): array {
                if (isset($command['result']) && is_array($command['result'])) {
                    $command['result'] = $codec->decodeEnvelope($command['result']);
                }

                return $command;
            }, $commands),
            'continuation_versions' => array_map(static fn (mixed $continuation): mixed
                => is_array($continuation) ? ($continuation['version'] ?? null) : null, $continuations),
            'history_reads' => $historyReads,
            'ordinary_completions' => $ordinary,
            'activity_callback_count' => $calls,
        ];
    }
}
