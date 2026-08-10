<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreeterWorkflow
{
    #[Workflow('quickstart.php.greeter')]
    public function run(WorkflowContext $context, string $name): Generator
    {
        $greeting = yield $context->activity('quickstart.php.greet', [$name]);

        return ['greeting' => $greeting];
    }
}

final class GreetingActivities
{
    #[Activity('quickstart.php.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN'),
);

Worker::create($client, quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'))
    ->register(GreeterWorkflow::class, GreetingActivities::class)
    ->run();
