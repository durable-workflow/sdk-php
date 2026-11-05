<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** A malformed page cannot safely continue an enumeration. */
final class PaginationException extends DurableWorkflowException
{
    public function __construct(string $message, public readonly string $reason)
    {
        parent::__construct($message);
    }
}
