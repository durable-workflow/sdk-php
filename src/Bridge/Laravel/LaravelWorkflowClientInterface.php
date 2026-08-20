<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\WorkflowHandleInterface;

/** Laravel-native workflow starts and handles resolved from attributed service classes. */
interface LaravelWorkflowClientInterface
{
    /**
     * @param class-string $workflowService
     * @param list<mixed> $input
     */
    public function start(
        string $workflowService,
        array $input = [],
        ?string $workflowId = null,
        ?WorkflowStartOptions $options = null,
    ): WorkflowHandleInterface;

    /** @param class-string $workflowService */
    public function handle(
        string $workflowService,
        string $workflowId,
        ?string $selectedRunId = null,
    ): WorkflowHandleInterface;
}
