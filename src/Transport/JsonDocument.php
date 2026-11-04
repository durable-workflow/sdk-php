<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal One encoding policy for the actual document and its byte budgets. */
final class JsonDocument
{
    public static function encode(mixed $value): string
    {
        // json_encode() prints floats with the host's serialize_precision. A
        // non-default value (for example 3) would send and record 1.2345 as
        // 1.23, and a recorded search attribute would then mismatch on replay.
        // Request documents always use round-trip precision.
        $precision = ini_get('serialize_precision');
        $scoped = $precision !== '-1' && ini_set('serialize_precision', '-1') !== false;
        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION
                | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        } finally {
            if ($scoped) {
                ini_set('serialize_precision', (string) $precision);
            }
        }
    }

    public static function size(mixed $value): int
    {
        return strlen(self::encode($value));
    }
}
