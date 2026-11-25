<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An update-validation interceptor skipped `$next` or called it more than once.
 *
 * The Worker answers the validation task with `update_validation_worker_failed`.
 * The update is not accepted, and the caller can retry with the same request id.
 */
final class UpdateValidationInterceptorViolation extends DurableWorkflowException
{
    public function __construct(
        public readonly string $interceptor,
        string $message,
    ) {
        parent::__construct($message);
    }
}
