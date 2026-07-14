<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Search attribute definitions available in one namespace. */
final class SearchAttributeCollection
{
    /**
     * @param array<string, string> $systemAttributes
     * @param array<string, string> $customAttributes
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly array $systemAttributes,
        public readonly array $customAttributes,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        return new self(
            self::stringMap($value['system_attributes'] ?? null),
            self::stringMap($value['custom_attributes'] ?? null),
            $value,
        );
    }

    /** @return array<string, string> */
    private static function stringMap(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }

        $result = [];
        foreach ($value as $name => $type) {
            if (is_string($name) && is_string($type)) {
                $result[$name] = $type;
            }
        }

        return $result;
    }
}
