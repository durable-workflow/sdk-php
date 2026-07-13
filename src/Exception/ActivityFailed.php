<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

final class ActivityFailed extends DurableWorkflowException
{
    /** @param array<string, mixed>|null $failure */
    public function __construct(
        string $message,
        public readonly ?string $activityType = null,
        public readonly ?string $failureType = null,
        public readonly bool $nonRetryable = false,
        public readonly ?array $failure = null,
    ) {
        parent::__construct($message);
    }
}
