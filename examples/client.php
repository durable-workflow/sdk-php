<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Client;

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);

$workflowId = 'php-quickstart-'.bin2hex(random_bytes(16));
$handle = $client->startWorkflow(
    workflowType: 'quickstart.php.greeter',
    workflowId: $workflowId,
    taskQueue: quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'),
    input: ['PHP'],
);

$result = $handle->result(timeoutSeconds: 90, pollIntervalSeconds: 1);

echo json_encode(
    ['workflow_id' => $workflowId, 'result' => $result],
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
).PHP_EOL;
