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

/** A source-independent completion/poll fixture for the official replay consumer. */
final class CarriedActivityScenario
{
    /** @return array{commands: list<array<string, mixed>>, activity_callback_count: int, activity_poll_requests: int} */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $polls = $callbacks = 0;
        $commands = null;
        $answer = static function (array $task): array {
            $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

            return ['task' => $task + ['lease_owner' => 'corpus-worker', 'payload_codec' => 'avro',
                'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
                'poll_status' => 'leased', 'protocol_version' => '1.20',
                'server_capabilities' => ['workflow_task_completion_activity_poll' => true],
                'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
        };
        $activity = $answer(['task_id' => 'corpus-activity', 'activity_attempt_id' => 'corpus-attempt',
            'activity_type' => 'corpus.activity', 'arguments' => $codec->envelope([7])]);
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $answer, $activity, $codec, &$polls, &$commands,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return $answer(['task_id' => 'corpus-workflow', 'workflow_task_attempt' => 1,
                    'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'corpus.workflow', 'history_events' => []]);
            }
            if (str_ends_with($uri, '/workflow-tasks/corpus-workflow/complete')) {
                $commands = $body['commands'];
                if (!isset($body['activity_poll'])) {
                    return ['completed' => true];
                }
                if ($body['activity_poll']['worker_id'] !== 'corpus-worker'
                    || $body['activity_poll']['task_queue'] !== 'corpus-queue'
                    || $body['activity_poll']['timeout_seconds'] !== 0
                    || !is_string($body['activity_poll']['poll_request_id'])) {
                    throw new RuntimeException('The carried activity request changed its worker binding.');
                }

                return ['completed' => true, 'activity_poll' => ['leased' => true, 'answer' => $activity]];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                ++$polls;

                return $activity;
            }
            if (str_ends_with($uri, '/activity-tasks/corpus-activity/complete')) {
                if ($codec->decodeEnvelope($body['result']) !== 7) {
                    throw new RuntimeException('The carried activity changed its decoded result.');
                }

                return ['recorded' => true, 'outcome' => 'completed'];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }

            throw new RuntimeException('Unexpected carried activity fixture request.');
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport),
            'corpus-queue', workerId: 'corpus-worker');
        $worker->registerWorkflow('corpus.workflow', static fn (WorkflowContext $context): string => 'carried-activity');
        $worker->registerActivity('corpus.activity', static function (ActivityContext $context, int $value) use (&$callbacks): int {
            ++$callbacks;

            return $value;
        });
        $worker->tick(0);
        if (!is_array($commands)) {
            throw new RuntimeException('The carried activity fixture did not complete its workflow.');
        }
        foreach ($commands as &$command) {
            if (isset($command['result'])) {
                $command['result'] = $codec->decodeEnvelope($command['result']);
            }
        }
        unset($command);

        return ['commands' => $commands, 'activity_callback_count' => $callbacks, 'activity_poll_requests' => $polls];
    }
}
