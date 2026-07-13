<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

final class NonDeterministicWorkflow extends DurableWorkflowException
{
    public function __construct(
        string $message,
        public readonly ?int $sequence = null,
        public readonly ?string $expected = null,
        public readonly ?string $actual = null,
    ) {
        parent::__construct($message);
    }
}
