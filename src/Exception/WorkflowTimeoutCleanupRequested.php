<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\ScopedCancellationContext;

/**
 * Delivered to workflow code when its work deadline passes and the run opted
 * in to deadline-triggered timeout cleanup (workflow#863). It is a
 * cooperative cancellation request, so existing `catch (WorkflowCancelled)`
 * handlers (saga compensation) run; the run still ends timed out. Its
 * context's `cause` is `timeout`.
 *
 * @experimental
 */
final class WorkflowTimeoutCleanupRequested extends WorkflowCancelled
{
    public function __construct(
        string $message = 'Workflow work deadline passed: timeout cleanup requested.',
        int $code = 0,
        ?\Throwable $previous = null,
        ?string $requestId = null,
        CancellationContext|ScopedCancellationContext|null $context = null,
    ) {
        parent::__construct($message, $code, $previous, $requestId, $context);
    }
}
