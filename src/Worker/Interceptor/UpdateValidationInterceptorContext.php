<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

use DurableWorkflow\Worker\QueryContext;

/**
 * Immutable context passed to update-validation interceptor hooks.
 *
 * The validator always runs with the Worker's own copy of this task, so a
 * hook cannot change the update's arguments or the history it validates.
 */
final class UpdateValidationInterceptorContext
{
    /**
     * @param array<string, mixed> $task the update validation task as delivered by the Server
     * @param list<mixed> $arguments decoded update arguments
     */
    public function __construct(
        public readonly string $workerId,
        public readonly string $taskQueue,
        public readonly ?string $namespace,
        public readonly array $task,
        public readonly string $taskId,
        public readonly string $workflowType,
        public readonly string $updateName,
        public readonly string $workflowId,
        public readonly string $runId,
        public readonly array $arguments,
        public readonly QueryContext $queryContext,
        public readonly bool $observabilitySupported = false,
    ) {
    }
}
