<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal One encoding policy for the actual document and its byte budgets. */
final class JsonDocument
{
    public static function encode(mixed $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION
            | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    public static function size(mixed $value): int
    {
        return strlen(self::encode($value));
    }
}
