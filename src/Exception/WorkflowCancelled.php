<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\ScopedCancellationContext;

/**
 * A cooperative cancellation request delivered at an authored call, or a
 * workflow result that closed cancelled.
 *
 * Timeout cleanup delivers the {@see WorkflowTimeoutCleanupRequested}
 * subtype, so handlers that catch this class also run on timeout cleanup.
 */
class WorkflowCancelled extends DurableWorkflowException
{
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        public readonly ?string $requestId = null,
        public readonly CancellationContext|ScopedCancellationContext|null $context = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * The exception for a delivered request: the timeout cleanup subtype when
     * its context's `cause` is `timeout`.
     */
    public static function forContext(
        ?string $requestId,
        CancellationContext|ScopedCancellationContext|null $context,
        string $message = 'Workflow cancellation was requested.',
    ): self {
        return $context instanceof CancellationContext && $context->isTimeout()
            ? new WorkflowTimeoutCleanupRequested(requestId: $requestId, context: $context)
            : new self($message, requestId: $requestId, context: $context);
    }
}
