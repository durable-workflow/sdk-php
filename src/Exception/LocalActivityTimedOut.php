<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use RuntimeException;

/** Cooperative local-activity timeout observed at an execution boundary. */
final class LocalActivityTimedOut extends RuntimeException
{
    public function __construct(
        public readonly string $timeoutKind,
        string $message,
    ) {
        parent::__construct($message);
    }
}
