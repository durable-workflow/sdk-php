<?php

declare(strict_types=1);

// Fixed SDK wire fixtures, with no Server implementation or durability claim.
require dirname(__DIR__, 2).'/vendor/autoload.php';

use DurableWorkflow\Codec\AvroPayloadCodec;

$file = getenv('PIPELINE_TEST_STATE');
$mutate = static function (callable $change) use ($file): array {
    $handle = fopen($file, 'c+');
    flock($handle, LOCK_EX);
    $state = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
    $change($state);
    rewind($handle);
    ftruncate($handle, 0);
    fwrite($handle, json_encode($state, JSON_THROW_ON_ERROR));
    flock($handle, LOCK_UN);
    fclose($handle);
    return $state;
};
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$body = json_decode(file_get_contents('php://input') ?: 'null', true, flags: JSON_THROW_ON_ERROR);
$state = $mutate(static function (array &$state) use ($path, $body): void {
    $state['requests'][] = ['path' => $path, 'body' => $body];
});
$options = $state['options'] ?? [];
$caps = ['workflow_task_completion_workflow_poll' => ($options['workflow_carrier'] ?? false) === true];
$envelope = static fn (array $reply): array => $reply + ['protocol_version' => '1.20', 'server_capabilities' => $caps];
if ($path === '/api/cluster/info') {
    $reply = ['namespace' => ['external_payload_storage' => ['status' => 'disabled']],
        'worker_protocol' => ['version' => '1.20', 'server_capabilities' => ['cooperative_cancellation' => ($options['mode'] ?? null) === 'cooperative']]];
} elseif ($path === '/api/worker/register') {
    $reply = ['registered' => true, 'heartbeat_interval_seconds' => 10, 'server_capabilities' => $caps];
} elseif ($path === '/api/worker/heartbeat') {
    $reply = ['acknowledged' => true];
} elseif (str_contains($path, '/api/worker/registrations/')) {
    $reply = ['worker_id' => 'pipeline-worker', 'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
} elseif ($path === '/api/worker/workflow-tasks/poll') {
    $state = $mutate(static function (array &$state) use ($options, $body): void {
        $state['workflow_polls'] = ($state['workflow_polls'] ?? 0) + 1;
        $state['empty_probe'] = ($options['empty_prefetch'] ?? false) && $state['workflow_polls'] === 3;
        if (($state['recover_poll_id'] ?? null) === ($body['poll_request_id'] ?? null)) {
            $state['next_task'] = 3;
        } elseif (!$state['empty_probe']) {
            $state['next_task'] = ($state['next_task'] ?? 0) + 1;
        }
    });
    $index = $state['next_task'];
    if ($state['empty_probe']) {
        $reply = ['task' => null, 'poll_status' => 'empty'];
    } elseif (($options['prefetch_error_status'] ?? null) !== null && $state['workflow_polls'] === 3) {
        $mutate(static function (array &$state) use ($body): void {
            $state['recover_poll_id'] = $body['poll_request_id'];
        });
        http_response_code($options['prefetch_error_status']);
        $reply = ['reason' => 'fixture_poll_failure'];
    } elseif ($index > ($options['task_count'] ?? 3)) {
        $reply = ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
    } else {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $expiry = ($options['delayed_prefetch'] ?? false) && $index === 3
            ? $now->modify('+10 seconds')->modify('+50000 microseconds') : $now->modify('+30 seconds');
        $reply = ['task' => ['task_id' => 'task-'.$index, 'workflow_task_attempt' => 1, 'lease_owner' => 'pipeline-worker',
            'workflow_id' => 'workflow-'.$index, 'run_id' => 'run-'.$index, 'workflow_type' => 'pipeline.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([$index]),
            'history_events' => [], 'lease_expires_at' => $expiry->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
        if (($options['child_bad_arguments'] ?? false) && $index === 3) {
            $reply['task']['arguments'] = (new AvroPayloadCodec())->envelope(['invalid integer argument']);
        }
        $mutate(static function (array &$state) use ($index): void {
            $state['events'][] = ['kind' => 'leased', 'task' => $index];
        });
        if (($options['delayed_prefetch'] ?? false) && $index === 3) usleep(120_000);
    }
} elseif (preg_match('~/api/worker/workflow-tasks/task-(\d+)/heartbeat$~', $path, $match) === 1) {
    $reply = ['task_id' => 'task-'.$match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'pipeline-worker', 'renewed' => true];
    $mutate(static function (array &$state) use ($match): void {
        $state['events'][] = ['kind' => 'heartbeat', 'task' => (int) $match[1]];
    });
    if (($options['lose_claim_task'] ?? null) === (int) $match[1]) {
        http_response_code(409);
        $reply = ['task_id' => 'task-'.$match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'pipeline-worker',
            'reason' => 'task_not_leased', 'task_status' => 'ready'];
    }
} elseif (preg_match('~/api/worker/workflow-tasks/task-(\d+)/complete$~', $path, $match) === 1) {
    $index = (int) $match[1];
    $state = $mutate(static function (array &$state) use ($index, $body): void {
        $state['events'][] = ['kind' => 'completion_sent', 'task' => $index];
        $state['completions'][$index] = $body;
        $state['completion_bodies'][$index][] = $body;
        $state['completion_attempts'][$index] = ($state['completion_attempts'][$index] ?? 0) + 1;
    });
    if ($index === ($options['hold_task'] ?? 2) && $state['completion_attempts'][$index] === 1) {
        $deadline = microtime(true) + 0.3;
        do {
            $current = $mutate(static function (array &$state): void {});
            if (($current['release_parent'] ?? false) === true) break;
            usleep(1000);
        } while (microtime(true) < $deadline);
    }
    $mutate(static function (array &$state) use ($index): void {
        $state['events'][] = ['kind' => 'completion_answer', 'task' => $index];
    });
    $reply = ['completed' => true];
    if ($index === ($options['hold_task'] ?? 2) && ($options['lose_parent_answer'] ?? false)
        && $state['completion_attempts'][$index] === 1) {
        http_response_code(502);
    } elseif ($index === ($options['hold_task'] ?? 2) && ($options['parent_status'] ?? 200) !== 200) {
        http_response_code($options['parent_status']);
        $reply = $options['parent_status'] === 409
            ? ['reason' => 'task_not_leased', 'task_id' => ($options['wrong_parent_binding'] ?? false) ? 'other-task' : 'task-'.$index,
                'workflow_task_attempt' => 1, 'task_status' => 'ready']
            : ['reason' => 'worker_registration_fenced'];
    }
} elseif (str_ends_with($path, '/fail')) {
    $reply = ['recorded' => true];
} elseif (str_ends_with($path, '/activity-tasks/poll') || str_ends_with($path, '/query-tasks/poll')) {
    $reply = ['task' => null, 'poll_status' => 'empty'];
    if (str_ends_with($path, '/activity-tasks/poll') && ($options['activity_poll_failure'] ?? false)
        && isset($state['completions'][2]) && !isset($state['completions'][3])) {
        http_response_code(500);
        $reply = ['reason' => 'fixture_activity_poll_failure'];
    }
} else {
    http_response_code(500);
    $reply = ['reason' => 'unexpected_fixture_request', 'path' => $path];
}
header('Content-Type: application/json');
if (http_response_code() === 502) {
    header('Content-Type: text/plain');
    echo 'The fixture upstream did not return a Server envelope.';
    return;
}
echo json_encode($envelope($reply), JSON_THROW_ON_ERROR);
