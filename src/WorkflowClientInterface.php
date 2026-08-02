<?php

declare(strict_types=1);

namespace DurableWorkflow;

/** The application-facing workflow operations implemented by Client and its test fake. */
interface WorkflowClientInterface
{
    /**
     * @param list<mixed> $input
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
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
    ): WorkflowHandleInterface;

    public function workflowHandle(string $workflowId, ?string $selectedRunId = null): WorkflowHandleInterface;
}
