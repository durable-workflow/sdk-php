<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

class ServerException extends DurableWorkflowException
{
    /** @param array<string, mixed>|list<mixed>|null $details */
    public function __construct(
        string $message,
        public readonly int $status,
        public readonly ?string $reason = null,
        public readonly ?array $details = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $status, $previous);
    }
}
