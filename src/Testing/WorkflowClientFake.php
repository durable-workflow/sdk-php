<?php

declare(strict_types=1);

namespace DurableWorkflow\Testing;

use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Model\WorkflowStreamAppendResult;
use DurableWorkflow\Model\WorkflowStreamDescription;
use DurableWorkflow\Model\WorkflowStreamItem;
use DurableWorkflow\Model\WorkflowStreamPage;
use DurableWorkflow\WorkflowClientInterface;
use DurableWorkflow\WorkflowHandleInterface;
use LogicException;

/** Records workflow client interactions and supplies configured test results. */
final class WorkflowClientFake implements WorkflowClientInterface
{
    /** @var list<array{workflow_type: string, workflow_id: string, task_queue: string, input: list<mixed>}> */
    private array $starts = [];
    /** @var list<array{workflow_id: string, name: string, arguments: list<mixed>}> */
    private array $signals = [];
    /** @var list<array{workflow_id: string, name: string, arguments: list<mixed>}> */
    private array $queries = [];
    /** @var list<array{workflow_id: string, name: string, arguments: list<mixed>}> */
    private array $updates = [];
    /** @var list<string> */
    private array $results = [];
    /** @var array<string, mixed> */
    private array $workflowResults = [];
    /** @var array<string, mixed> */
    private array $queryResults = [];
    /** @var array<string, mixed> */
    private array $updateResults = [];
    /** @var array<string, list<WorkflowStreamDescription>> */
    private array $workflowStreams = [];
    /** @var array<string, WorkflowStreamPage> */
    private array $workflowStreamPages = [];
    /** @var array<string, WorkflowStreamAppendResult> */
    private array $workflowStreamAppendResults = [];
    /** @var array<string, WorkflowStreamDescription> */
    private array $workflowStreamCloseResults = [];

    /** @param list<mixed> $input */
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
        $this->starts[] = [
            'workflow_type' => $workflowType,
            'workflow_id' => $workflowId,
            'task_queue' => $taskQueue,
            'input' => $input,
        ];

        return new TestWorkflowHandle($this, $workflowId, $workflowType);
    }

    public function workflowHandle(string $workflowId, ?string $selectedRunId = null): WorkflowHandleInterface
    {
        return new TestWorkflowHandle($this, $workflowId, selectedRunId: $selectedRunId);
    }

    /** @param list<WorkflowStreamDescription> $streams */
    public function setWorkflowStreams(string $workflowId, string $runId, array $streams): self
    {
        $this->workflowStreams[$this->streamKey($workflowId, $runId)] = $streams;

        return $this;
    }

    public function setWorkflowStreamPage(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset,
        WorkflowStreamPage $page,
    ): self {
        $this->workflowStreamPages[$this->streamKey($workflowId, $runId, $streamName, $fromOffset)] = $page;

        return $this;
    }

    public function setWorkflowStreamAppendResult(
        string $workflowId,
        string $runId,
        string $streamName,
        WorkflowStreamAppendResult $result,
    ): self {
        $this->workflowStreamAppendResults[$this->streamKey($workflowId, $runId, $streamName)] = $result;

        return $this;
    }

    public function setWorkflowStreamCloseResult(
        string $workflowId,
        string $runId,
        string $streamName,
        WorkflowStreamDescription $result,
    ): self {
        $this->workflowStreamCloseResults[$this->streamKey($workflowId, $runId, $streamName)] = $result;

        return $this;
    }

    /** @return list<WorkflowStreamDescription> */
    public function listWorkflowStreams(string $workflowId, string $runId): array
    {
        return $this->workflowStreams[$this->streamKey($workflowId, $runId)] ?? [];
    }

    public function describeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
    ): WorkflowStreamDescription {
        foreach ($this->listWorkflowStreams($workflowId, $runId) as $stream) {
            if ($stream->streamName === $streamName) {
                return $stream;
            }
        }

        throw new LogicException("No Workflow Stream is configured for {$workflowId}/{$runId}/{$streamName}.");
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
        if ($cancelled !== null && $cancelled() === true) {
            throw new \RuntimeException('Workflow Stream subscription was cancelled.');
        }

        $key = $this->streamKey($workflowId, $runId, $streamName, $fromOffset);

        return $this->workflowStreamPages[$key]
            ?? throw new LogicException("No Workflow Stream page is configured for {$key}.");
    }

    /** @return \Generator<int, WorkflowStreamItem> */
    public function iterateWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        int $fromOffset = 0,
        int $maxItems = 100,
        int $waitSeconds = 30,
        ?callable $cancelled = null,
    ): \Generator {
        $offset = $fromOffset;
        while ($cancelled === null || $cancelled() !== true) {
            $page = $this->subscribeWorkflowStream(
                $workflowId,
                $runId,
                $streamName,
                $offset,
                $maxItems,
                $waitSeconds,
                $cancelled,
            );
            foreach ($page->items as $item) {
                yield $item;
            }
            if ($page->terminal) {
                return;
            }
            $offset = $page->nextOffset;
        }
    }

    /** @param list<WorkflowStreamAppendItem> $items */
    public function appendWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        array $items,
        ?int $maxPendingItems = null,
    ): WorkflowStreamAppendResult {
        $key = $this->streamKey($workflowId, $runId, $streamName);

        return $this->workflowStreamAppendResults[$key]
            ?? throw new LogicException("No Workflow Stream append result is configured for {$key}.");
    }

    public function closeWorkflowStream(
        string $workflowId,
        string $runId,
        string $streamName,
        ?string $errorReason = null,
        ?int $retentionSeconds = null,
    ): WorkflowStreamDescription {
        $key = $this->streamKey($workflowId, $runId, $streamName);

        return $this->workflowStreamCloseResults[$key]
            ?? throw new LogicException("No Workflow Stream close result is configured for {$key}.");
    }

    public function setWorkflowResult(string $workflowId, mixed $result): self
    {
        $this->workflowResults[$workflowId] = $result;

        return $this;
    }

    public function setQueryResult(string $workflowId, string $name, mixed $result): self
    {
        $this->queryResults[$this->key($workflowId, $name)] = $result;

        return $this;
    }

    public function setUpdateResult(string $workflowId, string $name, mixed $result): self
    {
        $this->updateResults[$this->key($workflowId, $name)] = $result;

        return $this;
    }

    /** @param list<mixed> $arguments */
    public function signal(string $workflowId, string $name, array $arguments): void
    {
        $this->signals[] = ['workflow_id' => $workflowId, 'name' => $name, 'arguments' => $arguments];
    }

    /** @param list<mixed> $arguments */
    public function query(string $workflowId, string $name, array $arguments): mixed
    {
        $this->queries[] = ['workflow_id' => $workflowId, 'name' => $name, 'arguments' => $arguments];

        return $this->configured($this->queryResults, $workflowId, $name, 'query');
    }

    /** @param list<mixed> $arguments */
    public function update(string $workflowId, string $name, array $arguments): mixed
    {
        $this->updates[] = ['workflow_id' => $workflowId, 'name' => $name, 'arguments' => $arguments];

        return $this->configured($this->updateResults, $workflowId, $name, 'update');
    }

    public function result(string $workflowId): mixed
    {
        $this->results[] = $workflowId;
        if (!array_key_exists($workflowId, $this->workflowResults)) {
            throw new LogicException("No workflow result is configured for {$workflowId}.");
        }

        return $this->workflowResults[$workflowId];
    }

    /** @param list<mixed>|null $input */
    public function assertWorkflowStarted(
        string $workflowType,
        ?array $input = null,
        ?string $workflowId = null,
        ?string $taskQueue = null,
    ): void {
        foreach ($this->starts as $start) {
            if ($start['workflow_type'] === $workflowType
                && ($input === null || $start['input'] === $input)
                && ($workflowId === null || $start['workflow_id'] === $workflowId)
                && ($taskQueue === null || $start['task_queue'] === $taskQueue)
            ) {
                return;
            }
        }

        throw new AssertionFailed("No {$workflowType} workflow start matched the expected input.");
    }

    /** @param list<mixed>|null $arguments */
    public function assertSignalSent(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->assertInteraction($this->signals, 'signal', $workflowId, $name, $arguments);
    }

    /** @param list<mixed>|null $arguments */
    public function assertQueryRequested(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->assertInteraction($this->queries, 'query', $workflowId, $name, $arguments);
    }

    /** @param list<mixed>|null $arguments */
    public function assertUpdateRequested(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->assertInteraction($this->updates, 'update', $workflowId, $name, $arguments);
    }

    public function assertResultRequested(string $workflowId): void
    {
        if (!in_array($workflowId, $this->results, true)) {
            throw new AssertionFailed("No result was requested for workflow {$workflowId}.");
        }
    }

    /**
     * @param list<array{workflow_id: string, name: string, arguments: list<mixed>}> $interactions
     * @param list<mixed>|null $arguments
     */
    private function assertInteraction(
        array $interactions,
        string $kind,
        string $workflowId,
        string $name,
        ?array $arguments,
    ): void {
        foreach ($interactions as $interaction) {
            if ($interaction['workflow_id'] === $workflowId
                && $interaction['name'] === $name
                && ($arguments === null || $interaction['arguments'] === $arguments)
            ) {
                return;
            }
        }

        throw new AssertionFailed("No {$kind} interaction matched {$workflowId}.{$name}.");
    }

    /** @param array<string, mixed> $configured */
    private function configured(array $configured, string $workflowId, string $name, string $kind): mixed
    {
        $key = $this->key($workflowId, $name);
        if (!array_key_exists($key, $configured)) {
            throw new LogicException("No {$kind} result is configured for {$workflowId}.{$name}.");
        }

        return $configured[$key];
    }

    private function key(string $workflowId, string $name): string
    {
        return $workflowId."\0".$name;
    }

    private function streamKey(
        string $workflowId,
        string $runId,
        string $streamName = '',
        ?int $fromOffset = null,
    ): string {
        return implode("\0", [
            $workflowId,
            $runId,
            $streamName,
            $fromOffset === null ? '' : (string) $fromOffset,
        ]);
    }
}
