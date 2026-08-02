<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use InvalidArgumentException;

/** A handler class or method cannot form a valid worker contract. */
final class InvalidWorkerDefinition extends InvalidArgumentException
{
    public function __construct(
        public readonly string $contract,
        public readonly string $remediation,
    ) {
        parent::__construct("Invalid worker contract {$contract}. {$remediation}");
    }
}
