<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Model\WorkflowStreamAppendResult;
use DurableWorkflow\Model\WorkflowStreamDescription;
use DurableWorkflow\Model\WorkflowStreamPage;
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

    /** @return list<WorkflowStreamDescription> */
    public function listWorkflowStreams(string $workflowId, string $runId): array
    {
        return $this->client()->listWorkflowStreams($workflowId, $runId);
    }

    public function describeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
    ): WorkflowStreamDescription {
        return $this->client()->describeWorkflowStream($workflowId, $runId, $streamName);
    }

    public function subscribeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset = 0,
        int $maxItems = 100,
        int $waitSeconds = 0,
        ?callable $cancelled = null,
    ): WorkflowStreamPage {
        return $this->client()->subscribeWorkflowStream(
            $workflowId,
            $runId,
            $streamName,
            $fromOffset,
            $maxItems,
            $waitSeconds,
            $cancelled,
        );
    }

    /** @return \Generator<int, \DurableWorkflow\Model\WorkflowStreamItem> */
    public function iterateWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset = 0,
        int $maxItems = 100,
        int $waitSeconds = 30,
        ?callable $cancelled = null,
    ): \Generator {
        yield from $this->client()->iterateWorkflowStream(
            $workflowId,
            $runId,
            $streamName,
            $fromOffset,
            $maxItems,
            $waitSeconds,
            $cancelled,
        );
    }

    /** @param list<WorkflowStreamAppendItem> $items */
    public function appendWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        array $items,
        ?int $maxPendingItems = null,
    ): WorkflowStreamAppendResult {
        return $this->client()->appendWorkflowStream(
            $workflowId,
            $runId,
            $streamName,
            $items,
            $maxPendingItems,
        );
    }

    public function closeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        ?string $errorReason = null,
        ?int $retentionSeconds = null,
    ): WorkflowStreamDescription {
        return $this->client()->closeWorkflowStream(
            $workflowId,
            $runId,
            $streamName,
            $errorReason,
            $retentionSeconds,
        );
    }

    private function client(): WorkflowClientInterface
    {
        return $this->resolved ??= ($this->resolve)();
    }
}
