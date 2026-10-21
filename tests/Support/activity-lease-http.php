<?php

declare(strict_types=1);

// Real local HTTP transport for lease/process tests, with no application runtime.
$file = getenv('LEASE_TEST_STATE');
$handle = fopen($file, 'c+');
flock($handle, LOCK_EX);
$state = json_decode(stream_get_contents($handle), true, 32, JSON_THROW_ON_ERROR);
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$body = json_decode(file_get_contents('php://input'), true) ?? [];
$now = microtime(true);
$stamp = static fn (float $value): string => (new DateTimeImmutable('@'.sprintf('%.6F', $value)))
    ->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s.u\Z');
$state['requests'][] = ['path' => $path, 'body' => $body,
    'namespace' => $_SERVER['HTTP_X_NAMESPACE'] ?? null,
    'authorization' => $_SERVER['HTTP_AUTHORIZATION'] ?? null,
    'bootstrap' => $_SERVER['HTTP_X_MAINTENANCE_BOOTSTRAP'] ?? null];
$status = 200;
$response = [];
if (($_SERVER['HTTP_AUTHORIZATION'] ?? '') !== 'Bearer fixture-worker') {
    $status = 401;
    $response = ['reason' => 'wrong_credential'];
} elseif ($path === '/api/cluster/info') {
    $response = ['worker_protocol' => ['server_capabilities' => ['activity_lease_renewal' => true]]];
} elseif ($path === '/api/worker/heartbeat') {
    $response = ['worker_id' => 'owner', 'acknowledged' => true];
} elseif (str_ends_with($path, '/activity-tasks/poll') && !isset($state['delivered'])) {
    $state['delivered'] = true;
    $state['expiry'] = $now + 3;
    $state['progress_at'] = $now;
    $state['business_at'] = isset($state['business_seconds']) ? $now + $state['business_seconds'] : null;
    $deadlines = [];
    if ($state['business_at'] !== null) {
        $deadlines['start_to_close'] = $stamp($state['business_at']);
    }
    if (isset($state['heartbeat_seconds'])) {
        $deadlines['heartbeat'] = $stamp($now + $state['heartbeat_seconds']);
    }
    $response = ['task' => ['task_id' => 'task', 'workflow_id' => 'workflow', 'run_id' => 'run',
        'activity_execution_id' => 'execution', 'activity_attempt_id' => 'attempt', 'attempt_number' => 1,
        'activity_type' => 'slow', 'task_queue' => 'queue', 'lease_owner' => 'owner', 'payload_codec' => 'avro',
        'namespace' => 'tenant', 'lease_expires_at' => $stamp($state['expiry']), 'deadlines' => $deadlines,
        'retry_policy' => ['heartbeat_timeout' => $state['heartbeat_seconds'] ?? null]],
        'server_time' => $stamp($now), 'poll_status' => 'leased'];
} elseif (str_ends_with($path, '/poll')) {
    $response = ['task' => null, 'poll_status' => 'empty'];
} elseif (str_ends_with($path, '/renew-lease') || str_ends_with($path, '/heartbeat')) {
    $heartbeat = str_ends_with($path, '/heartbeat');
    $live = $now < $state['expiry'] && !($state['closed'] ?? false)
        && ($state['business_at'] === null || $now < $state['business_at'])
        && (!isset($state['heartbeat_seconds']) || $now < $state['progress_at'] + $state['heartbeat_seconds']);
    if ($live) {
        $state['expiry'] = $now + 3;
        if ($heartbeat) {
            $state['progress_at'] = $now;
            $state['heartbeats'] = ($state['heartbeats'] ?? 0) + 1;
        } else {
            $state['renewals'] = ($state['renewals'] ?? 0) + 1;
        }
    }
    $response = ['task_id' => 'task', 'workflow_task_id' => 'task', 'workflow_instance_id' => 'workflow',
        'workflow_run_id' => 'run', 'activity_execution_id' => 'execution', 'activity_attempt_id' => 'attempt',
        'attempt_number' => 1, 'lease_owner' => 'owner', 'can_continue' => $live, 'cancel_requested' => false,
        'lease_renewed' => $live && !$heartbeat, 'heartbeat_recorded' => $live && $heartbeat,
        'lease_expires_at' => $stamp($state['expiry']), 'server_time' => $stamp($now),
        'last_heartbeat_at' => $stamp($state['progress_at']),
        'heartbeat_deadline_at' => isset($state['heartbeat_seconds'])
            ? $stamp($state['progress_at'] + $state['heartbeat_seconds']) : null,
        'reason' => $live ? null : 'deadline_elapsed'];
    if (($state['corrupt_after'] ?? PHP_INT_MAX) <= ($state['renewals'] ?? 0)) {
        $response['activity_attempt_id'] = 'foreign-attempt';
    }
    if (!$live) {
        $status = 409;
    }
} elseif (str_ends_with($path, '/complete') || str_ends_with($path, '/fail')) {
    $live = $now < $state['expiry'] && ($state['business_at'] === null || $now < $state['business_at']);
    if ($live) {
        $state['outcomes'][] = $body;
        $state['closed'] = true;
        $response = ['recorded' => true, 'outcome' => str_ends_with($path, '/complete') ? 'completed' : 'failed'];
    } else {
        $status = 409;
        $response = ['task_id' => 'task', 'activity_attempt_id' => 'attempt', 'reason' => 'stale_attempt',
            'recorded' => false, 'outcome' => 'failed'];
    }
} else {
    $status = 404;
    $response = ['reason' => 'unexpected_fixture_route'];
}
rewind($handle);
ftruncate($handle, 0);
fwrite($handle, json_encode($state, JSON_THROW_ON_ERROR));
flock($handle, LOCK_UN);
fclose($handle);
http_response_code($status);
header('Content-Type: application/json');
echo json_encode($response, JSON_THROW_ON_ERROR);
