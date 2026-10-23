<?php

declare(strict_types=1);

namespace DurableWorkflow\Failure;

use DurableWorkflow\Exception\FailureReportRefused;
use DurableWorkflow\Transport\JsonDocument;
use DurableWorkflow\Transport\RequestSizeLimitTransport;
use DurableWorkflow\Transport\Transport;

/** @internal Bounds reports using existing protocol carriers and exact JSON bytes. */
final class FailureReportLimiter
{
    private const METADATA = ['details', 'diagnostics', 'runtime_diagnostics', 'validation_errors'];

    public function __construct(private readonly FailureReportLimits $limits, private readonly Transport $transport)
    {
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public function request(string $path, array $body): array
    {
        $endpoint = preg_match('~^/worker/(activity|workflow|query)-tasks/[^/]+/fail$~D', $path, $matches) === 1
            && is_array($body['failure'] ?? null);
        $commands = preg_match('~^/worker/workflow-tasks/[^/]+/complete$~D', $path) === 1
            && is_array($body['commands'] ?? null)
            && array_filter($body['commands'], static fn (mixed $command): bool => is_array($command)
                && in_array($command['type'] ?? null, ['fail_workflow', 'fail_update'], true)) !== [];
        if (!$endpoint && !$commands) {
            return $body;
        }
        $limit = $this->limits->requestBytes;
        if ($this->transport instanceof RequestSizeLimitTransport) {
            try {
                $known = $this->transport->maximumRequestBytes();
            } catch (\Throwable $error) {
                throw new FailureReportRefused($limit, $error);
            }
            if ($known !== null) {
                if ($known < 1) {
                    throw new FailureReportRefused($limit);
                }
                $limit = min($limit, $known);
            }
        }
        $messageLimit = FailureReportLimits::MESSAGE_BYTES;
        $stackLimit = FailureReportLimits::STACK_BYTES;
        $dropMetadata = false;
        for ($pass = 0; $pass < 16; ++$pass) {
            $candidate = $body;
            if ($endpoint) {
                $candidate['failure'] = $this->report($body['failure'], $messageLimit, $stackLimit,
                    $dropMetadata, $matches[1] === 'activity' ? 'diagnostics' : 'stack_trace');
            }
            if ($commands) {
                foreach ($body['commands'] as $index => $command) {
                    if (is_array($command) && in_array($command['type'] ?? null, ['fail_workflow', 'fail_update'], true)) {
                        $candidate['commands'][$index] = $this->command($command, $messageLimit, $stackLimit, $dropMetadata);
                    }
                }
            }
            try {
                if (JsonDocument::size($candidate) + strlen($path) <= $limit) {
                    return $candidate;
                }
            } catch (\JsonException $error) {
                throw new FailureReportRefused($limit, $error);
            }
            if (!$dropMetadata) {
                $dropMetadata = true;
            } else {
                $messageLimit = max(96, intdiv($messageLimit, 2));
                $stackLimit = max(512, intdiv($stackLimit, 2));
            }
        }

        throw new FailureReportRefused($limit);
    }

    /** @param array<string, mixed> $original
     * @param array<string, mixed> $additionalChanges
     * @return array<string, mixed>
     */
    private function report(array $original, int $messageLimit, int $stackLimit, bool $dropMetadata, string $carrier, array $additionalChanges = []): array
    {
        $report = $original;
        $message = FailureText::bound((string) ($original['message'] ?? ''), $messageLimit, 'message');
        $stack = FailureText::bound((string) ($original['stack_trace'] ?? ''), $stackLimit, 'stack_trace');
        $changes = $additionalChanges;
        foreach (['message' => $message, 'stack_trace' => $stack] as $field => $text) {
            if ($text['changed']) {
                $changes[$field] = ['original_bytes' => $text['original_bytes']];
            }
        }
        $report['message'] = $message['value'];
        if (array_key_exists('stack_trace', $original)) {
            $report['stack_trace'] = $stack['value'];
        }
        $metadata = array_intersect_key($original, array_flip(self::METADATA));
        try {
            $metadataBytes = JsonDocument::size($metadata);
        } catch (\JsonException) {
            $metadataBytes = null;
        }
        if ($dropMetadata || $metadataBytes === null || $metadataBytes > FailureReportLimits::METADATA_BYTES) {
            $this->omitMetadata($report, $metadata, $changes);
        }
        if ($changes === []) {
            return $report;
        }
        $message = FailureText::bound((string) ($original['message'] ?? ''), $messageLimit, 'message', true);
        $report['message'] = $message['value'];
        if ($carrier === 'diagnostics') {
            $diagnostics = ['truncation' => ['fields' => $changes]];
            if (isset($report['diagnostics'])) {
                $diagnostics['original'] = $report['diagnostics'];
            }
            $report['diagnostics'] = $diagnostics;
            if (JsonDocument::size(array_intersect_key($report, array_flip(self::METADATA))) > FailureReportLimits::METADATA_BYTES) {
                $this->omitMetadata($report, $metadata, $changes);
                $report['diagnostics'] = ['truncation' => ['fields' => $changes]];
            }
        } else {
            // This endpoint admits a stack string, but no diagnostics field.
            $suffix = "\n[truncation diagnostics]\n".JsonDocument::encode(['fields' => $changes]);
            $available = $stackLimit - strlen($suffix);
            $stack = FailureText::bound((string) ($original['stack_trace'] ?? ''), $available, 'stack_trace');
            $report['stack_trace'] = $stack['value'].$suffix;
        }

        return $report;
    }

    /** @param array<string, mixed> $report
     * @param array<string, mixed> $metadata
     * @param array<string, mixed> $changes
     */
    private function omitMetadata(array &$report, array $metadata, array &$changes): void
    {
        foreach ($metadata as $field => $value) {
            try {
                $size = JsonDocument::size($value);
            } catch (\JsonException) {
                $size = null;
            }
            $changes[$field] = ['omitted' => true, 'original_serialized_bytes' => $size];
            unset($report[$field]);
        }
    }

    /** @param array<string, mixed> $command
     * @return array<string, mixed>
     */
    private function command(array $command, int $messageLimit, int $stackLimit, bool $dropMetadata): array
    {
        if (($command['type'] ?? null) === 'fail_update') {
            $originalMessage = (string) ($command['message'] ?? '');
            $bounded = FailureText::bound($originalMessage, $messageLimit, 'message');
            $command['message'] = $bounded['value'];
            if ($bounded['changed']) {
                $suffix = "\n[truncation diagnostics]\n".JsonDocument::encode(['original_message_bytes' => strlen($originalMessage)]);
                $command['message'] = FailureText::bound($originalMessage,
                    $messageLimit - strlen($suffix), 'message', true)['value'].$suffix;
            }

            return $command;
        }
        $exception = is_array($command['exception'] ?? null) ? $command['exception'] : [];
        $fixed = array_intersect_key($exception, array_flip(['class', 'type', 'code', 'line']));
        $extra = array_diff_key($exception, $fixed + array_flip(['message', 'stack_trace', 'diagnostics']));
        $report = ['message' => (string) ($command['message'] ?? '')];
        foreach (['stack_trace', 'diagnostics'] as $field) {
            if (array_key_exists($field, $exception)) {
                $report[$field] = $exception[$field];
            }
        }
        if ($extra !== []) {
            $report['details'] = $extra;
        }
        $innerMessage = array_key_exists('message', $exception)
            ? FailureText::bound((string) $exception['message'], $messageLimit, 'exception.message') : null;
        $additionalChanges = $innerMessage !== null && $innerMessage['changed']
            ? ['exception.message' => ['original_bytes' => $innerMessage['original_bytes']]] : [];
        $bounded = $this->report($report, $messageLimit, $stackLimit, $dropMetadata, 'diagnostics', $additionalChanges);
        $command['message'] = $bounded['message'];
        $result = $fixed + ($bounded['details'] ?? []);
        if ($innerMessage !== null) {
            $result['message'] = $innerMessage['value'];
        }
        foreach (['stack_trace', 'diagnostics'] as $field) {
            if (array_key_exists($field, $bounded)) {
                $result[$field] = $bounded[$field];
            }
        }
        if ($result !== [] || array_key_exists('exception', $command)) {
            $command['exception'] = $result;
        }

        return $command;
    }
}
