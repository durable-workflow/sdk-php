<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Model\ServiceOperationOptions;

$client = new Client(
    getenv('DURABLE_WORKFLOW_SERVER_URL') ?: 'http://localhost:8080',
    token: getenv('DURABLE_WORKFLOW_AUTH_TOKEN') ?: 'dev-token-123',
);

$handle = $client->startWorkflow(
    workflowType: 'greeter',
    workflowId: 'php-greeter-'.bin2hex(random_bytes(4)),
    taskQueue: 'php-workers',
    input: ['world'],
);

$handle->signal('set-language', ['en']);
var_dump($handle->query('status'));
var_dump($handle->update('rename', ['Ada']));
var_dump($handle->result(timeoutSeconds: 30));

$operations = $client->withNamespace('default');
$running = $operations->listWorkflows(status: 'running', pageSize: 25);
$schedules = $operations->listSchedules(status: 'active', pageSize: 25);
$attributes = $operations->listSearchAttributes();
$cluster = $operations->clusterInfo();

$serviceCall = $operations->startServiceOperation(
    'greeter-services',
    'Greeter',
    'greet',
    ['name' => 'Ada'],
    new ServiceOperationOptions(idempotencyKey: 'php-example-greet-Ada'),
);

var_dump(
    $running->executions,
    $schedules->schedules,
    $schedules->nextPageToken,
    $attributes->customAttributes,
    $cluster->capabilities,
);
var_dump($serviceCall->describe());
