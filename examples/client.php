<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use DurableWorkflow\Client;

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
