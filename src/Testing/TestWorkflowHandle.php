<?php

declare(strict_types=1);

namespace DurableWorkflow\Testing;

use DurableWorkflow\WorkflowHandleInterface;

/** In-memory workflow handle returned by WorkflowClientFake. */
final class TestWorkflowHandle implements WorkflowHandleInterface
{
    public function __construct(
        private readonly WorkflowClientFake $client,
        public readonly string $workflowId,
        public readonly string $workflowType = '',
        public readonly ?string $selectedRunId = null,
    ) {
    }

    /** @param list<mixed> $arguments */
    public function signal(string $name, array $arguments = []): void
    {
        $this->client->signal($this->workflowId, $name, $arguments);
    }

    /** @param list<mixed> $arguments */
    public function query(string $name, array $arguments = []): mixed
    {
        return $this->client->query($this->workflowId, $name, $arguments);
    }

    /** @param list<mixed> $arguments */
    public function update(
        string $name,
        array $arguments = [],
        string $waitFor = 'completed',
        ?int $waitTimeoutSeconds = null,
        ?string $requestId = null,
    ): mixed {
        return $this->client->update($this->workflowId, $name, $arguments);
    }

    public function result(float $timeoutSeconds = 30.0, float $pollIntervalSeconds = 0.5): mixed
    {
        return $this->client->result($this->workflowId);
    }
}
