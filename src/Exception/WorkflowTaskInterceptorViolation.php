<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * A workflow-task interceptor skipped `$next` or called it more than once.
 *
 * The Worker fails the workflow task with this exception. Nothing is
 * recorded, and the Server retries the task.
 */
final class WorkflowTaskInterceptorViolation extends DurableWorkflowException
{
    public function __construct(
        public readonly string $interceptor,
        string $message,
    ) {
        parent::__construct($message);
    }
}
