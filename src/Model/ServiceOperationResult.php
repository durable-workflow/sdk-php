<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** The immutable outcome delivered at a workflow's service-operation boundary. */
final class ServiceOperationResult
{
    /** @param array<string, mixed> $serviceCall */
    public function __construct(
        public readonly string $serviceCallId,
        public readonly string $status,
        public readonly ?string $outcome,
        public readonly mixed $responsePayload,
        public readonly array $serviceCall,
    ) {
    }

    public function completed(): bool
    {
        return $this->status === 'completed';
    }
}
