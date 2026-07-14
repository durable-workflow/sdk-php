<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Current server view of a durable service operation call. */
final class ServiceOperationDescription
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $serviceCallId,
        public readonly string $endpointName,
        public readonly string $serviceName,
        public readonly string $operationName,
        public readonly ?string $status,
        public readonly mixed $outcome,
        public readonly ?bool $accepted,
        public readonly ?string $reason,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(
        array $value,
        string $endpointName,
        string $serviceName,
        string $operationName,
    ): self {
        return new self(
            (string) ($value['service_call_id'] ?? $value['id'] ?? ''),
            (string) ($value['endpoint_name'] ?? $endpointName),
            (string) ($value['service_name'] ?? $serviceName),
            (string) ($value['operation_name'] ?? $operationName),
            isset($value['status']) ? (string) $value['status'] : null,
            $value['outcome'] ?? null,
            isset($value['accepted']) ? (bool) $value['accepted'] : null,
            isset($value['reason']) ? (string) $value['reason'] : null,
            $value,
        );
    }
}
