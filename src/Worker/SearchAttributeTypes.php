<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use InvalidArgumentException;

/** @internal Canonical, explicitly authored search-attribute type identity. */
final class SearchAttributeTypes
{
    private const TYPES = ['string', 'keyword', 'keyword_list', 'int', 'float', 'bool', 'datetime'];

    /**
     * Null values are deletions and need no declaration. A declaration for a
     * deletion is retained because the Server records explicitly supplied types.
     *
     * @param array<array-key, mixed> $attributes
     * @return array<string, string>
     */
    public static function canonical(array $attributes, mixed $types): array
    {
        if ($attributes === [] || count($attributes) > 100) {
            throw new InvalidArgumentException('A typed search-attribute update requires between one and 100 attributes.');
        }
        $canonical = self::typeMap($attributes, $types);
        foreach ($attributes as $key => $value) {
            if (!is_string($key) || preg_match('/\A[A-Za-z0-9_.:-]{1,64}\z/', $key) !== 1) {
                throw new InvalidArgumentException('Search attributes require portable string keys.');
            }
            if ($value !== null && !array_key_exists($key, $canonical)) {
                throw new InvalidArgumentException("Search attribute [{$key}] needs an explicit type; use null to delete it.");
            }
        }
        ksort($canonical, SORT_STRING);

        return $canonical;
    }

    /**
     * @param array<array-key, mixed> $attributes
     * @return array<string, string>
     */
    private static function typeMap(array $attributes, mixed $types): array
    {
        if (!is_array($types)) {
            throw new InvalidArgumentException('Search-attribute types must be a canonical type map.');
        }
        $canonical = [];
        foreach ($types as $key => $type) {
            if (!is_string($key) || !array_key_exists($key, $attributes)) {
                throw new InvalidArgumentException('A declared search-attribute type has no corresponding attribute.');
            }
            if (!is_string($type) || !in_array($type, self::TYPES, true)) {
                throw new InvalidArgumentException("Search attribute [{$key}] must declare a canonical type name.");
            }
            if (!self::accepts($type, $attributes[$key])) {
                throw new InvalidArgumentException("Search attribute [{$key}] has a value incompatible with its declared type.");
            }
            $canonical[$key] = $type;
        }
        return $canonical;
    }

    private static function accepts(string $type, mixed $value): bool
    {
        if ($value === null) {
            return true;
        }

        return match ($type) {
            'string' => self::stringValue($value, 2048),
            'keyword' => self::stringValue($value, 255),
            'keyword_list' => self::keywordList($value),
            'int' => is_int($value),
            'float' => is_int($value) || (is_float($value) && is_finite($value)),
            'bool' => is_bool($value),
            'datetime' => self::dateTime($value),
            default => false,
        };
    }

    private static function stringValue(mixed $value, int $limit): bool
    {
        if (!is_string($value) || trim($value) === '') {
            return false;
        }
        $length = preg_match_all('/./su', trim($value));

        return $length !== false && $length <= $limit;
    }

    private static function keywordList(mixed $value): bool
    {
        if (!is_array($value) || !array_is_list($value)) {
            return false;
        }
        foreach ($value as $item) {
            if (!is_string($item)) {
                return false;
            }
            $length = preg_match_all('/./su', trim($item));
            if ($length === false || $length > 255) {
                return false;
            }
        }

        return true;
    }

    private static function dateTime(mixed $value): bool
    {
        if (!is_string($value) || !self::stringValue($value, 2048)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z/i', trim($value)) !== 1) {
            return false;
        }
        $parsed = date_parse(trim($value));

        return $parsed['warning_count'] === 0 && $parsed['error_count'] === 0;
    }
}
