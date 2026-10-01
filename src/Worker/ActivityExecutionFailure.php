<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use RuntimeException;

/** @internal Failure metadata returned by an isolated activity callback. */
final class ActivityExecutionFailure extends RuntimeException
{
    public function __construct(
        string $message,
        public readonly string $originalType,
        public readonly bool $duringEncoding = false,
    ) {
        parent::__construct($message);
    }
}
