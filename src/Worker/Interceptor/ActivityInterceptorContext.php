<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

use DurableWorkflow\Worker\ActivityContext;

/** Immutable context passed to activity interceptor hooks. */
final class ActivityInterceptorContext
{
    /**
     * @param array<string, mixed> $task the remote activity task as delivered, or an empty array for a local activity
     * @param list<mixed> $arguments decoded activity arguments
     */
    public function __construct(
        public readonly string $workerId,
        public readonly string $taskQueue,
        public readonly ?string $namespace,
        public readonly array $task,
        public readonly string $activityType,
        public readonly array $arguments,
        public readonly int $attemptNumber,
        public readonly string $taskId,
        public readonly string $activityAttemptId,
        public readonly ?string $workflowId,
        public readonly ?string $runId,
        public readonly bool $local,
        public readonly ActivityContext $activityContext,
    ) {
    }

    /** Return a copy that calls the activity with different arguments. */
    public function withArguments(mixed ...$arguments): self
    {
        return new self($this->workerId, $this->taskQueue, $this->namespace, $this->task, $this->activityType,
            array_values($arguments), $this->attemptNumber, $this->taskId, $this->activityAttemptId,
            $this->workflowId, $this->runId, $this->local, $this->activityContext);
    }
}
