<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreeterWorkflow
{
    #[Workflow('greeter')]
    public function run(WorkflowContext $context, string $name): Generator
    {
        $greeting = yield $context->activity('greet', [$name], [
            'start_to_close_timeout' => 30,
            'heartbeat_timeout' => 10,
        ]);

        return ['greeting' => $greeting];
    }

    #[Query]
    public function status(QueryContext $context): array
    {
        return ['events' => count($context->history), 'run_id' => $context->runId];
    }

    #[Signal('set-language')]
    public function setLanguage(string $language): void
    {
        // The signature declares admission; run() consumes context signals during replay.
    }

    #[Update]
    public function rename(QueryContext $context, string $name): array
    {
        return ['accepted_name' => $name, 'run_id' => $context->runId];
    }
}

final class GreetingActivities
{
    #[Activity('greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        $context->heartbeat(['phase' => 'formatting']);

        return "hello, {$name}";
    }
}

$client = new Client(
    getenv('DURABLE_WORKFLOW_SERVER_URL') ?: 'http://localhost:8080',
    token: getenv('DURABLE_WORKFLOW_AUTH_TOKEN') ?: 'dev-token-123',
);

Worker::create($client, 'php-workers')
    ->register(GreeterWorkflow::class, GreetingActivities::class)
    ->run();
