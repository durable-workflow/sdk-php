<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

$client = new Client(
    getenv('DURABLE_WORKFLOW_SERVER_URL') ?: 'http://localhost:8080',
    token: getenv('DURABLE_WORKFLOW_AUTH_TOKEN') ?: 'dev-token-123',
);
$worker = new Worker($client, 'php-workers');

$worker->registerActivity('greet', static function (ActivityContext $context, string $name): string {
    $context->heartbeat(['phase' => 'formatting']);

    return "hello, {$name}";
});

$worker->registerWorkflow('greeter', static function (WorkflowContext $context, string $name): Generator {
    $greeting = yield $context->activity('greet', [$name], [
        'start_to_close_timeout' => 30,
        'heartbeat_timeout' => 10,
    ]);

    return ['greeting' => $greeting];
});

$worker->registerQuery('greeter', 'status', static function (QueryContext $context): array {
    return ['events' => count($context->history), 'run_id' => $context->runId];
});

$worker->registerUpdate('greeter', 'rename', static function (QueryContext $context, string $name): array {
    return ['accepted_name' => $name, 'run_id' => $context->runId];
});

$worker->run();
