<?php

declare(strict_types=1);

// An inert loopback composition of the existing lease fixture, not a Server implementation.
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$file = getenv('LEASE_TEST_STATE');
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
$state = $mutate(static function (array &$state) use ($path): void {
    $state['wire_requests'][] = $path;
});
$caps = ['workflow_task_completion_activity_poll' => true, 'activity_lease_renewal' => true];
if ($path === '/api/worker/workflow-tasks/poll') {
    $state = $mutate(static function (array &$state): void { $state['workflow_polls'] = ($state['workflow_polls'] ?? 0) + 1; });
    $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $response = ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps];
    if ($state['workflow_polls'] === 2) {
        $response += ['server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
        $response['poll_status'] = 'leased';
        $response['task'] = ['task_id' => 'workflow-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'owner',
            'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'carry',
            'payload_codec' => 'avro', 'history_events' => [],
            'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')];
    }
} elseif ($path === '/api/worker/workflow-tasks/workflow-task/complete') {
    $body = json_decode(file_get_contents('php://input'), true, flags: JSON_THROW_ON_ERROR);
    $mutate(static function (array &$state) use ($body): void {
        $state['workflow_completions'][] = $body;
    });
    $response = ['completed' => true];
    if (is_array($body['activity_poll'] ?? null)) {
        // Construct the same lease fixture in-process. This is not a second wire request.
        $original = $_SERVER['REQUEST_URI'];
        $_SERVER['REQUEST_URI'] = '/api/worker/activity-tasks/poll';
        ob_start();
        require __DIR__.'/activity-lease-http.php';
        $answer = json_decode(ob_get_clean(), true, flags: JSON_THROW_ON_ERROR);
        $_SERVER['REQUEST_URI'] = $original;
        $response['activity_poll'] = ['leased' => true, 'answer' => $answer + [
            'protocol_version' => '1.20', 'server_capabilities' => $caps,
        ]];
    }
} else {
    require __DIR__.'/activity-lease-http.php';
    return;
}
header('Content-Type: application/json');
echo json_encode($response, JSON_THROW_ON_ERROR);
