<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

use DurableWorkflow\Worker\QueryContext;

/** Immutable context passed to query-task interceptor hooks. */
final class QueryTaskInterceptorContext
{
    /**
     * @param array<string, mixed> $task the query task as delivered by the Server
     * @param list<mixed> $arguments decoded query arguments
     */
    public function __construct(
        public readonly string $workerId,
        public readonly string $taskQueue,
        public readonly ?string $namespace,
        public readonly array $task,
        public readonly string $taskId,
        public readonly string $workflowType,
        public readonly string $queryName,
        public readonly string $workflowId,
        public readonly string $runId,
        public readonly array $arguments,
        public readonly QueryContext $queryContext,
    ) {
    }

    /** Return a copy that calls the query handler with different arguments. */
    public function withArguments(mixed ...$arguments): self
    {
        return new self($this->workerId, $this->taskQueue, $this->namespace, $this->task, $this->taskId,
            $this->workflowType, $this->queryName, $this->workflowId, $this->runId, array_values($arguments),
            $this->queryContext);
    }
}
