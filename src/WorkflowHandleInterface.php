<?php

declare(strict_types=1);

namespace DurableWorkflow;

/** Workflow interactions shared by live and in-memory handles. */
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
