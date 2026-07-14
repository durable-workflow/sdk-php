<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Result of creating or deleting a custom search attribute. */
final class SearchAttributeDefinition
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $name,
        public readonly ?string $type,
        public readonly ?string $outcome,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value, string $fallbackName = ''): self
    {
        return new self(
            (string) ($value['name'] ?? $fallbackName),
            isset($value['type']) ? (string) $value['type'] : null,
            isset($value['outcome']) ? (string) $value['outcome'] : null,
            $value,
        );
    }
}
