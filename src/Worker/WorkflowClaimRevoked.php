<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Throwable;

/** @internal Expected authority loss ends this claim without publishing a failure. */
final class WorkflowClaimRevoked extends WorkflowClaimAborted
{
    public function __construct(
        public readonly string $reason,
        string $message,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, previous: $previous);
    }
}
