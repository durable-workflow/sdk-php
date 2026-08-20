<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\WorkflowClientInterface;
use DurableWorkflow\WorkflowHandleInterface;

/** @internal Defers application credential resolution until a client operation is invoked. */
final class DeferredWorkflowClient implements WorkflowClientInterface
{
    private ?WorkflowClientInterface $resolved = null;

    /** @param \Closure(): WorkflowClientInterface $resolve */
    public function __construct(private readonly \Closure $resolve)
    {
    }

    public function startWorkflow(
        string $workflowType,
        string $workflowId,
        string $taskQueue,
        array $input = [],
        int $executionTimeoutSeconds = 3600,
        int $runTimeoutSeconds = 600,
        ?string $duplicatePolicy = null,
        ?array $memo = null,
        ?array $searchAttributes = null,
        ?int $priority = null,
        ?string $fairnessKey = null,
        ?int $fairnessWeight = null,
        ?string $buildId = null,
    ): WorkflowHandleInterface {
        return $this->client()->startWorkflow(
            workflowType: $workflowType,
            workflowId: $workflowId,
            taskQueue: $taskQueue,
            input: $input,
            executionTimeoutSeconds: $executionTimeoutSeconds,
            runTimeoutSeconds: $runTimeoutSeconds,
            duplicatePolicy: $duplicatePolicy,
            memo: $memo,
            searchAttributes: $searchAttributes,
            priority: $priority,
            fairnessKey: $fairnessKey,
            fairnessWeight: $fairnessWeight,
            buildId: $buildId,
        );
    }

    public function workflowHandle(string $workflowId, ?string $selectedRunId = null): WorkflowHandleInterface
    {
        return $this->client()->workflowHandle($workflowId, $selectedRunId);
    }

    private function client(): WorkflowClientInterface
    {
        return $this->resolved ??= ($this->resolve)();
    }
}
