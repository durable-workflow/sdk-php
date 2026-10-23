<?php

declare(strict_types=1);

namespace DurableWorkflow\Failure;

use DurableWorkflow\Exception\FailureReportRefused;

/** @internal Bounds failure text without requiring an optional Unicode extension. */
final class FailureText
{
    /** @return array{value: string, changed: bool, original_bytes: int, retained_bytes: int} */
    public static function bound(string $original, int $limit, string $field, bool $forceMarker = false): array
    {
        $bytes = strlen($original);
        if (!$forceMarker && $bytes <= $limit && preg_match('//u', $original) === 1) {
            return ['value' => $original, 'changed' => false, 'original_bytes' => $bytes, 'retained_bytes' => $bytes];
        }
        $marker = "\n[truncated failure report; original {$field} {$bytes} bytes]";
        $available = $limit - strlen($marker);
        if ($available < 0) {
            throw new FailureReportRefused($limit);
        }
        $prefix = substr($original, 0, $available);
        $candidate = $prefix;
        // A cut through a valid UTF-8 code point needs at most three bytes of retreat.
        for ($retreat = 0; $retreat < 3 && preg_match('//u', $candidate) !== 1; ++$retreat) {
            $candidate = substr($candidate, 0, -1);
        }
        if (preg_match('//u', $candidate) !== 1) {
            // Repair only the bounded prefix, never allocate an escaped copy of
            // an arbitrarily large original just to retain its first64KiB.
            $candidate = json_decode(json_encode($prefix,
                JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE | JSON_UNESCAPED_UNICODE), true, flags: JSON_THROW_ON_ERROR);
            $candidate = substr($candidate, 0, $available);
            while (preg_match('//u', $candidate) !== 1) {
                $candidate = substr($candidate, 0, -1);
            }
        }

        return ['value' => $candidate.$marker, 'changed' => true,
            'original_bytes' => $bytes, 'retained_bytes' => strlen($candidate)];
    }
}
