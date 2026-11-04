<?php

declare(strict_types=1);

require_once __DIR__.'/bootstrap.php';

use DurableWorkflow\Client;

$mode = $argv[1] ?? 'await';
if (!in_array($mode, ['await', 'abandon', 'cancel', 'terminate'], true)) {
    throw new InvalidArgumentException('Choose await, abandon, cancel or terminate.');
}
$reject = ($argv[2] ?? null) === 'reject';
$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);
$parent = $client->startWorkflow(
    'quickstart.php.child-examples',
    'php-child-example-'.bin2hex(random_bytes(12)),
    quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'),
    ['PHP', $mode, $reject],
);
$result = $parent->result(timeoutSeconds: 90, pollIntervalSeconds: 1);
$deadline = microtime(true) + 15;
do {
    $child = $client->describeWorkflow($result['identity']['workflow_id']);
    if (in_array($child->status, ['completed', 'failed', 'cancelled', 'terminated'], true)) {
        break;
    }
    usleep(200_000);
} while (microtime(true) < $deadline);

echo json_encode(['parent_result' => $result, 'child_status' => $child->status], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
