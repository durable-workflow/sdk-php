<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class NamespaceDescription
{
    /**
     * @param array<string, mixed> $raw
     * @param array<string, mixed>|null $externalPayloadStorage
     * @param array<string, mixed>|null $deleted
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $description,
        public readonly ?int $retentionDays,
        public readonly ?string $status,
        public readonly array $raw,
        public readonly ?array $externalPayloadStorage = null,
        public readonly ?string $createdAt = null,
        public readonly ?string $updatedAt = null,
        public readonly ?array $deleted = null,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        return new self(
            (string) ($value['name'] ?? ''),
            isset($value['description']) ? (string) $value['description'] : null,
            isset($value['retention_days']) ? (int) $value['retention_days'] : null,
            isset($value['status']) ? (string) $value['status'] : null,
            $value,
            isset($value['external_payload_storage']) && is_array($value['external_payload_storage'])
                ? $value['external_payload_storage']
                : null,
            isset($value['created_at']) ? (string) $value['created_at'] : null,
            isset($value['updated_at']) ? (string) $value['updated_at'] : null,
            isset($value['deleted']) && is_array($value['deleted']) ? $value['deleted'] : null,
        );
    }
}
