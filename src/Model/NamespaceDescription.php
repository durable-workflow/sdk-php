<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class NamespaceDescription
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $name,
        public readonly ?string $description,
        public readonly ?int $retentionDays,
        public readonly ?string $status,
        public readonly array $raw,
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
        );
    }
}
