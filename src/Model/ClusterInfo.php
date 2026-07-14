<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Public compatibility and capability manifest returned by cluster discovery. */
final class ClusterInfo
{
    /**
     * @param array<string, mixed> $namespace
     * @param array<string, mixed> $capabilities
     * @param array<string, mixed> $limits
     * @param array<string, mixed> $controlPlane
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly ?string $serverId,
        public readonly ?string $version,
        public readonly ?string $defaultNamespace,
        public readonly array $namespace,
        public readonly array $capabilities,
        public readonly array $limits,
        public readonly array $controlPlane,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        return new self(
            isset($value['server_id']) ? (string) $value['server_id'] : null,
            isset($value['version']) ? (string) $value['version'] : null,
            isset($value['default_namespace']) ? (string) $value['default_namespace'] : null,
            self::map($value['namespace'] ?? null),
            self::map($value['capabilities'] ?? null),
            self::map($value['limits'] ?? null),
            self::map($value['control_plane'] ?? null),
            $value,
        );
    }

    /** @return array<string, mixed> */
    private static function map(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }

        $result = [];
        foreach ($value as $key => $entry) {
            if (is_string($key)) {
                $result[$key] = $entry;
            }
        }

        return $result;
    }
}
