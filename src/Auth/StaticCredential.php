<?php

declare(strict_types=1);

namespace DurableWorkflow\Auth;

use InvalidArgumentException;

/** @internal Validates original static token text without changing its bytes. */
final class StaticCredential
{
    // Unicode White_Space from the frozen shared static-credentials/v1 policy.
    private const WHITE_SPACE = '[\x{0009}-\x{000D}\x{0020}\x{0085}\x{00A0}\x{1680}\x{2000}-\x{200A}'
        .'\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]';

    public static function assertValid(?string $value, string $setting): void
    {
        if ($value === null || $value === '') {
            return;
        }
        if (preg_match('//u', $value) !== 1
            || preg_match('/[\x00-\x1f\x{007f}-\x{009f}]/u', $value) === 1
            || preg_match('/\A'.self::WHITE_SPACE.'|'.self::WHITE_SPACE.'\z/u', $value) === 1) {
            throw new InvalidArgumentException('Invalid static credential setting '.$setting.'.');
        }
    }

    private function __construct()
    {
    }
}
