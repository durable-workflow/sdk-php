<?php

declare(strict_types=1);

// A real HTTP peer can lose a reply either before or after retaining an outcome.
[$script, $directory, $operation, $failureStatus, $committedFirst, $failureBody] = $argv;
$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
if ($server === false) {
    throw new RuntimeException($error);
}
file_put_contents($directory.'/address', stream_socket_get_name($server, false));
$state = ['requests' => [], 'outcomes' => []];
$polls = ['workflow' => 0, 'activity' => 0];
$failed = false;
$kind = $operation === 'activity_complete' ? 'activity' : 'workflow';
$target = '/api/worker/'.$kind.'-tasks/'.$kind.'-1/'.($operation === 'workflow_heartbeat' ? 'heartbeat' : 'complete');
$until = microtime(true) + 30;
while (microtime(true) < $until) {
    $peer = @stream_socket_accept($server, 1);
    if ($peer === false) {
        continue;
    }
    stream_set_timeout($peer, 3);
    $first = fgets($peer);
    $path = explode(' ', $first === false ? '' : $first)[1] ?? '';
    $length = 0;
    while (($line = fgets($peer)) !== false && $line !== "\r\n") {
        if (preg_match('/^Content-Length:\s*(\d+)/i', $line, $matches)) {
            $length = (int) $matches[1];
        }
    }
    $raw = '';
    while (strlen($raw) < $length) {
        $chunk = fread($peer, $length - strlen($raw));
        if ($chunk === false || $chunk === '') {
            break;
        }
        $raw .= $chunk;
    }
    $body = $raw === '' ? [] : json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
    $state['requests'][] = ['path' => $path, 'body' => $body, 'raw' => $raw];
    $status = 200;
    $reply = [];
    $inject = $path === $target && !$failed;
    $complete = preg_match('~/api/worker/(workflow|activity)-tasks/([^/]+)/complete$~', $path, $completion) === 1;
    if ($complete && (!$inject || $committedFirst === '1')) {
        $taskId = $completion[2];
        if (isset($state['outcomes'][$taskId])) {
            $status = 409;
            $reply = $kind === 'workflow'
                ? ['reason' => 'task_not_leased', 'task_id' => $taskId,
                    'lease_owner' => $body['lease_owner'], 'workflow_task_attempt' => $body['workflow_task_attempt'],
                    'task_status' => 'completed']
                : ['reason' => 'stale_attempt', 'recorded' => false, 'outcome' => 'completed',
                    'task_id' => $taskId, 'lease_owner' => $body['lease_owner'],
                    'activity_attempt_id' => $body['activity_attempt_id'], 'activity_status' => 'completed',
                    'attempt_status' => 'completed', 'task_status' => 'completed'];
        } else {
            $state['outcomes'][$taskId] = $body;
            $reply = ['recorded' => true, 'outcome' => 'completed'];
        }
    } elseif (str_ends_with($path, '/heartbeat')) {
        $reply = str_contains($path, '/workflow-tasks/')
            ? ['task_id' => basename(dirname($path)), ...$body, 'renewed' => true]
            : ['acknowledged' => true];
    } elseif (str_ends_with($path, '/register')) {
        $reply = ['registered' => true];
    } elseif (str_ends_with($path, '/cluster/info')) {
        $reply = ['limits' => ['max_payload_bytes' => 1048576]];
    } elseif (str_ends_with($path, '/'.$kind.'-tasks/poll')) {
        $number = ++$polls[$kind];
        $reply = ['poll_status' => 'leased', 'task' => [
            'task_id' => $kind.'-'.$number, 'lease_owner' => 'worker-1',
            'workflow_task_attempt' => 3, 'activity_attempt_id' => 'attempt-'.$number,
            'workflow_id' => 'workflow-'.$number, 'run_id' => 'run-'.$number,
            'workflow_type' => 'test.workflow', 'activity_type' => 'test.activity',
            'payload_codec' => 'avro', 'history_events' => [],
        ]];
    } elseif (str_ends_with($path, '-tasks/poll')) {
        $reply = ['task' => null, 'poll_status' => $polls[$kind] >= 2 ? 'stopped' : 'empty'];
    }
    file_put_contents($directory.'/state.json', json_encode($state, JSON_THROW_ON_ERROR));
    if ($inject) {
        $failed = true;
        $status = (int) $failureStatus;
        if ($status === 0) {
            fclose($peer);
            continue;
        }
        $response = $failureBody;
    } else {
        $response = json_encode($reply, JSON_THROW_ON_ERROR);
    }
    fwrite($peer, "HTTP/1.1 {$status} Fixture\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: ".strlen($response)."\r\n\r\n".$response);
    fclose($peer);
}
fclose($server);
