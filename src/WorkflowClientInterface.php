<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Model\WorkflowStreamAppendResult;
use DurableWorkflow\Model\WorkflowStreamDescription;
use DurableWorkflow\Model\WorkflowStreamItem;
use DurableWorkflow\Model\WorkflowStreamPage;

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

    /** @return list<WorkflowStreamDescription> */
    public function listWorkflowStreams(string $workflowId, string $runId): array;

    public function describeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
    ): WorkflowStreamDescription;

    /** @param callable(): bool|null $cancelled */
    public function subscribeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset = 0,
        int $maxItems = 100,
        int $waitSeconds = 0,
        ?callable $cancelled = null,
    ): WorkflowStreamPage;

    /**
     * @param callable(): bool|null $cancelled
     * @return \Generator<int, WorkflowStreamItem>
     */
    public function iterateWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset = 0,
        int $maxItems = 100,
        int $waitSeconds = 30,
        ?callable $cancelled = null,
    ): \Generator;

    /** @param list<WorkflowStreamAppendItem> $items */
    public function appendWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        array $items,
        ?int $maxPendingItems = null,
    ): WorkflowStreamAppendResult;

    public function closeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        ?string $errorReason = null,
        ?int $retentionSeconds = null,
    ): WorkflowStreamDescription;
}
