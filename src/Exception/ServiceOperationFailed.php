<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** A recorded service failure or cancellation thrown at the original call. */
final class ServiceOperationFailed extends DurableWorkflowException
{
    /** @param array<string, mixed> $failure */
    public function __construct(
        string $message,
        public readonly ?string $serviceCallId,
        public readonly string $failureType,
        public readonly bool $cancelled,
        public readonly array $failure,
    ) {
        parent::__construct($message);
    }
}
