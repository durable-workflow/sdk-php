<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

final class CodecException extends DurableWorkflowException
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
