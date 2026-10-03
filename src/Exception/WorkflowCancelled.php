<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use DurableWorkflow\Worker\CancellationContext;

final class WorkflowCancelled extends DurableWorkflowException
{
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        public readonly ?string $requestId = null,
        public readonly ?CancellationContext $context = null,
    ) {
        parent::__construct($message, $code, $previous);
    }
}
