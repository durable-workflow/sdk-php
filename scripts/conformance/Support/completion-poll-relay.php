<?php

declare(strict_types=1);

// A loopback test relay forwards the actual runtime answer; it never implements a Server response.
require_once __DIR__.'/CompletionPollSubject.php';
require_once __DIR__.'/CompletionPollTrace.php';

use DurableWorkflow\Qualification\CompletionPollSubject;
use DurableWorkflow\Qualification\CompletionPollTrace;

try {
    $trace = new CompletionPollTrace((string) getenv('PIPELINE_TEST_STATE'));
    $state = $trace->state();
    CompletionPollSubject::loopback($state['endpoint']);
    $path = $_SERVER['REQUEST_URI'];
    $tracePath = parse_url($path, PHP_URL_PATH);
    if (!str_starts_with($path, '/api/') || strlen($path) > 2048) {
        throw new RuntimeException('completion_relay_path');
    }
    $raw = file_get_contents('php://input', false, null, 0, 1048577);
    if ($raw === false || strlen($raw) > 1048576) {
        throw new RuntimeException('completion_relay_body');
    }
    $body = $raw === '' ? [] : json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
    if (!is_array($body)) {
        throw new RuntimeException('completion_relay_document');
    }
    $requestId = bin2hex(random_bytes(8));
    $worker = str_starts_with($path, '/api/worker/');
    if ($worker) {
        $trace->record(['kind' => 'request', 'request' => $requestId, 'path' => $tracePath,
            'body' => CompletionPollTrace::request($body, $raw)]);
    }
    $headers = [];
    foreach (getallheaders() as $key => $value) {
        if (!in_array(strtolower($key), ['host', 'content-length', 'connection', 'transfer-encoding'], true)) {
            $headers[] = $key.': '.$value;
        }
    }
    $handle = curl_init($state['endpoint'].$path);
    if ($handle === false) {
        throw new RuntimeException('completion_relay_curl');
    }
    try {
        $response = '';
        curl_setopt_array($handle, [CURLOPT_CUSTOMREQUEST => $_SERVER['REQUEST_METHOD'], CURLOPT_HTTPHEADER => $headers,
            CURLOPT_POSTFIELDS => $raw, CURLOPT_FOLLOWLOCATION => false, CURLOPT_CONNECTTIMEOUT_MS => 2000,
            CURLOPT_TIMEOUT_MS => 12000, CURLOPT_ENCODING => '',
            CURLOPT_WRITEFUNCTION => static function ($handle, string $chunk) use (&$response): int {
                if (strlen($response) + strlen($chunk) > 1048576) {
                    return 0;
                }
                $response .= $chunk;
                return strlen($chunk);
            }]);
        if (curl_exec($handle) === false) {
            throw new RuntimeException('completion_relay_upstream');
        }
        $status = curl_getinfo($handle, CURLINFO_RESPONSE_CODE);
        $contentType = curl_getinfo($handle, CURLINFO_CONTENT_TYPE);
    } finally {
        curl_close($handle);
    }
    $answer = json_decode($response, true);
    if ($worker) {
        $trace->record(['kind' => 'upstream', 'request' => $requestId, 'status' => $status,
            'answer' => is_array($answer) ? CompletionPollTrace::answer($answer) : null]);
    }
    $parent = $trace->change(static function (array &$state) use ($path, $status, $requestId): void {
        if (!in_array($state['scenario'], ['pipeline', 'pipeline-loss'], true)
            || !preg_match('~/workflow-tasks/([^/]+)/complete$~', $path, $match)) {
            return;
        }
        foreach ($state['events'] as $event) {
            if ($event['kind'] !== 'upstream') {
                continue;
            }
            $task = $event['answer']['task'] ?? null;
            foreach (['activity_poll', 'workflow_poll'] as $key) {
                $candidate = $event['answer'][$key]['task'] ?? null;
                if (($candidate['task_id'] ?? null) === rawurldecode($match[1])) {
                    $task = $candidate;
                }
            }
            if (($task['task_id'] ?? null) === rawurldecode($match[1])
                && ($task['workflow_id'] ?? null) === $state['prefix'].'-2') {
                $state['parent_attempts'] = ($state['parent_attempts'] ?? 0) + 1;
                if ($status === 200 && !isset($state['held_parent_request'])) {
                    $state['held_parent_request'] = $requestId;
                }
                $state['parent_path'] = $path;
                return;
            }
        }
    });
    // Hold only the first successful answer, never a refusal or a generated response.
    if ($status === 200 && ($parent['held_parent_request'] ?? null) === $requestId) {
        $deadline = microtime(true) + 2;
        while (!(($trace->state())['release_parent'] ?? false) && microtime(true) < $deadline) {
            usleep(1000);
        }
        if ($state['scenario'] === 'pipeline-loss') {
            $status = 502;
            $response = 'The test relay deliberately withheld the completed upstream answer.';
            $contentType = 'text/plain';
        }
    }
    if ($worker) {
        $trace->record(['kind' => 'delivered', 'request' => $requestId, 'status' => $status]);
    }
    http_response_code($status);
    header('Content-Type: '.(is_string($contentType) ? $contentType : 'application/octet-stream'));
    echo $response;
} catch (Throwable $error) {
    if (isset($trace)) {
        try {
            $trace->record(['kind' => 'relay_error']);
        } catch (Throwable) {
            // A full or unusable trace remains a runner failure, never an empty poll.
        }
    }
    http_response_code(502);
    header('Content-Type: application/json');
    // A relay failure is fatal and distinguishable from the selected lost-answer experiment.
    echo '{"reason":"completion_relay_failed"}';
}
