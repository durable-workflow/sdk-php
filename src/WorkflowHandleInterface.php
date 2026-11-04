<?php

declare(strict_types=1);

namespace DurableWorkflow;

/**
 * Workflow interactions shared by live and in-memory handles.
 *
 * The SDK's handles, WorkflowHandle and TestWorkflowHandle, expose the bound
 * workflow and run as the read-only properties below. The interface does not
 * declare them as methods, so existing application implementations keep
 * loading; PHPStan does not read these tags on an interface.
 *
 * @property-read string $workflowId The workflow ID the handle is bound to, including an ID the client generated at start.
 * @property-read string|null $selectedRunId The run selected when the handle was created, or null to follow the current run.
 */
interface WorkflowHandleInterface
{
    /** @param list<mixed> $arguments */
    public function signal(string $name, array $arguments = []): void;

    /**
     * @param list<mixed> $arguments
     * @return array<string, mixed>
     */
    public function appendMessage(string $streamName, string $messageId, array $arguments = []): array;

    /** @param list<mixed> $arguments */
    public function query(string $name, array $arguments = []): mixed;

    /** @param list<mixed> $arguments */
    public function update(
        string $name,
        array $arguments = [],
        string $waitFor = 'completed',
        ?int $waitTimeoutSeconds = null,
        ?string $requestId = null,
    ): mixed;

    public function result(float $timeoutSeconds = 30.0, float $pollIntervalSeconds = 0.5): mixed;
}
