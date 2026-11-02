<?php

declare(strict_types=1);

namespace DurableWorkflow\Failure;

use DurableWorkflow\Exception\FailureReportRefused;
use DurableWorkflow\Transport\JsonDocument;

/** @internal Normalize before workflow code observes a local durable failure. */
final class LocalFailureReports
{
    private const DIAGNOSTICS = "\n[truncation diagnostics]\n";

    /** @param array<string, mixed> $outcome
     * @param array{message?: int, attempts?: array<int, int>} $originalSizes
     * @return array<string, mixed>
     */
    public static function bound(array $outcome, int $requestLimit = FailureReportLimits::REQUEST_BYTES, array $originalSizes = []): array
    {
        $messageLimit = FailureReportLimits::MESSAGE_BYTES;
        for ($pass = 0; $pass < 18; ++$pass) {
            $bounded = self::fragment($outcome, $messageLimit, $originalSizes['message'] ?? null);
            foreach ($outcome['attempts'] ?? [] as $index => $attempt) {
                if (is_array($attempt)) {
                    $bounded['attempts'][$index] = self::fragment($attempt, $messageLimit, $originalSizes['attempts'][$index] ?? null);
                }
            }
            // A successful result keeps its independent business-payload policy.
            $diagnosticContent = $bounded;
            unset($diagnosticContent['result']);
            try {
                if (JsonDocument::size($diagnosticContent) <= $requestLimit) {
                    return $bounded;
                }
            } catch (\JsonException $error) {
                throw new FailureReportRefused($requestLimit, $error);
            }
            $messageLimit = intdiv($messageLimit, 2);
        }
        throw new FailureReportRefused($requestLimit);
    }

    /** @param array<string, mixed> $fragment
     * @return array<string, mixed>
     */
    private static function fragment(array $fragment, int $limit, ?int $sourceBytes = null): array
    {
        if (!is_string($fragment['message'] ?? null)) {
            return $fragment;
        }
        $message = trim($fragment['message']);
        $fragment['message'] = $message;
        $originalBytes = max(strlen($message), $sourceBytes ?? 0);
        if (strlen($message) <= $limit && preg_match('//u', $message) === 1 && $originalBytes === strlen($message)) {
            return $fragment;
        }
        $suffix = self::DIAGNOSTICS.JsonDocument::encode(['message' => ['original_bytes' => $originalBytes]]);
        $minimum = FailureText::minimumBytes($message, 'message') + strlen($suffix) + 24;
        $budget = max($limit, $minimum);
        $fragment['message'] = FailureText::bound($message, $budget - strlen($suffix), 'message', true, $originalBytes)['value'].$suffix;
        return $fragment;
    }

}
