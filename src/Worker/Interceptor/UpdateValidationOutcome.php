<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

/**
 * Read-only view of the decision an update validator made.
 *
 * The Worker approves or rejects the update from the validator's own
 * decision, not from this object, so a hook cannot change it.
 */
final class UpdateValidationOutcome
{
    /** @param array<string, list<string>> $validationErrors */
    public function __construct(
        public readonly bool $approved,
        public readonly ?string $reason = null,
        public readonly ?string $message = null,
        public readonly ?string $failureType = null,
        public readonly array $validationErrors = [],
    ) {
    }

    public function rejected(): bool
    {
        return !$this->approved;
    }
}
