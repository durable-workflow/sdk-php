<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Model\WorkflowExecution;
use DurableWorkflow\Model\WorkflowRun;
use LogicException;

/** Operations bound to one stable workflow ID and, optionally, its originally selected run. */
final class WorkflowHandle
{
    public function __construct(
        private readonly Client $client,
        public readonly string $workflowId,
        public readonly ?string $selectedRunId = null,
        public readonly string $workflowType = '',
    ) {
    }

    public function describe(): WorkflowExecution
    {
        return $this->client->describeWorkflow($this->workflowId);
    }

    public function describeSelectedRun(): WorkflowExecution
    {
        return $this->client->describeWorkflow($this->workflowId, $this->requireSelectedRun());
    }

    /** @return list<WorkflowRun> */
    public function runs(): array
    {
        return $this->client->listWorkflowRuns($this->workflowId);
    }

    /** @param list<mixed> $arguments */
    public function signal(string $name, array $arguments = []): void
    {
        $this->client->signalWorkflow($this->workflowId, $name, $arguments);
    }

    /** @param list<mixed> $arguments */
    public function signalSelectedRun(string $name, array $arguments = []): void
    {
        $this->client->signalWorkflow($this->workflowId, $name, $arguments, $this->requireSelectedRun());
    }

    /** @param list<mixed> $arguments */
    public function query(string $name, array $arguments = []): mixed
    {
        return $this->client->queryWorkflow($this->workflowId, $name, $arguments);
    }

    /** @param list<mixed> $arguments */
    public function querySelectedRun(string $name, array $arguments = []): mixed
    {
        return $this->client->queryWorkflow($this->workflowId, $name, $arguments, $this->requireSelectedRun());
    }

    /** @param list<mixed> $arguments */
    public function update(
        string $name,
        array $arguments = [],
        string $waitFor = 'completed',
        ?int $waitTimeoutSeconds = null,
        ?string $requestId = null,
    ): mixed {
        return $this->client->updateWorkflow(
            $this->workflowId,
            $name,
            $arguments,
            $waitFor,
            $waitTimeoutSeconds,
            $requestId,
        );
    }

    /** @param list<mixed> $arguments */
    public function updateSelectedRun(
        string $name,
        array $arguments = [],
        string $waitFor = 'completed',
        ?int $waitTimeoutSeconds = null,
        ?string $requestId = null,
    ): mixed {
        return $this->client->updateWorkflow(
            $this->workflowId,
            $name,
            $arguments,
            $waitFor,
            $waitTimeoutSeconds,
            $requestId,
            $this->requireSelectedRun(),
        );
    }

    public function cancel(?string $reason = null): void
    {
        $this->client->cancelWorkflow($this->workflowId, $reason);
    }

    public function cancelSelectedRun(?string $reason = null): void
    {
        $this->client->cancelWorkflow($this->workflowId, $reason, $this->requireSelectedRun());
    }

    public function terminate(?string $reason = null): void
    {
        $this->client->terminateWorkflow($this->workflowId, $reason);
    }

    public function terminateSelectedRun(?string $reason = null): void
    {
        $this->client->terminateWorkflow($this->workflowId, $reason, $this->requireSelectedRun());
    }

    public function result(float $timeoutSeconds = 30.0, float $pollIntervalSeconds = 0.5): mixed
    {
        return $this->client->workflowResult(
            $this->workflowId,
            $this->selectedRunId,
            $timeoutSeconds,
            $pollIntervalSeconds,
            true,
        );
    }

    public function resultOfSelectedRun(float $timeoutSeconds = 30.0, float $pollIntervalSeconds = 0.5): mixed
    {
        return $this->client->workflowResult(
            $this->workflowId,
            $this->requireSelectedRun(),
            $timeoutSeconds,
            $pollIntervalSeconds,
            false,
        );
    }

    private function requireSelectedRun(): string
    {
        if ($this->selectedRunId === null || $this->selectedRunId === '') {
            throw new LogicException('This handle has no selected run ID.');
        }

        return $this->selectedRunId;
    }
}
