<?php

declare(strict_types=1);

require_once __DIR__.'/bootstrap.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\WorkflowContext;

final class ChildExampleRejected extends RuntimeException
{
}

final class ChildWorkflowExamples
{
    #[Workflow('quickstart.php.child-examples')]
    public function run(WorkflowContext $context, string $name, string $mode, bool $reject): array
    {
        // Sequential calls wait for the child's value.
        $sequential = $context->childWorkflow('quickstart.php.child-greeting', [$name, false]);

        $policy = match ($mode) {
            'await', 'abandon' => ParentClosePolicy::Abandon,
            'cancel' => ParentClosePolicy::RequestCancel,
            'terminate' => ParentClosePolicy::Terminate,
            default => throw new InvalidArgumentException('Unknown child example mode.'),
        };
        $child = $context->startChildWorkflow('quickstart.php.child-greeting', [$name, $reject], [
            'parent_close_policy' => $policy,
        ]);
        $identity = $child->identity();
        $snapshot = ['call_id' => $identity->callId, 'workflow_id' => $identity->workflowId, 'initial_run_id' => $identity->initialRunId];
        $context->activity('quickstart.php.record-child', [$snapshot]);
        $result = ['sequential' => $sequential, 'identity' => $snapshot, 'parent_close_policy' => $policy->value];
        if ($mode !== 'await') {
            // Closing with an open child applies the selected parent-close policy.
            return $result;
        }

        try {
            $result['child_result'] = $child->await();
        } catch (ChildWorkflowFailed $failure) {
            $result['child_failure'] = ['type' => $failure->failureType, 'message' => $failure->getMessage()];
        }

        return $result;
    }
}

final class ChildGreetingWorkflow
{
    #[Workflow('quickstart.php.child-greeting')]
    public function greet(WorkflowContext $context, string $name, bool $reject): array
    {
        $context->sleep(3);
        if ($reject) {
            throw new ChildExampleRejected('The child declined this greeting.');
        }

        return ['greeting' => 'Hello, '.$name, 'accepted' => true];
    }
}

final class ChildExampleActivities
{
    #[Activity('quickstart.php.record-child')]
    public function record(ActivityContext $context, array $identity): array
    {
        echo json_encode(['child_started' => $identity], JSON_THROW_ON_ERROR).PHP_EOL;

        return $identity;
    }
}

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN'),
);

Worker::create($client, quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'))
    ->register(ChildWorkflowExamples::class, ChildGreetingWorkflow::class, ChildExampleActivities::class)
    ->run();
