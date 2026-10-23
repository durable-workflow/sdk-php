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

    public function __construct(private readonly FailureReportLimits $limits, private readonly ?Transport $transport = null)
    {
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public function request(string $path, array $body, bool $enforceRequestLimit = true): array
    {
        $kind = $this->reportKind($path, $body);
        if ($kind === null) {
            return $body;
        }
        if ($kind === 'prepared') {
            return $this->preparedOutcome($path, $body);
        }
        $carrier = null;
        if ($kind === 'failure') {
            $carrier = str_contains($path, '/activity-tasks/') ? 'diagnostics' : 'stack_trace';
        }
        return $this->bound($body, $carrier, $kind === 'commands', $this->effectiveLimit(), strlen($path), $enforceRequestLimit);
    }

    /** @param array<string, mixed> $body */
    public function limitForRequest(string $path, array $body): ?int
    {
        return $this->reportKind($path, $body) === null ? null : max(1, $this->effectiveLimit() - strlen($path));
    }

    /** @param array<string, mixed> $body */
    private function reportKind(string $path, array $body): ?string
    {
        if (preg_match('~^/worker/workflow-tasks/[^/]+/local-activities/[^/]+/outcome$~D', $path) === 1
            && is_array($body['report'] ?? null) && is_string($body['report']['message'] ?? null)) {
            return 'prepared';
        }
        if (preg_match('~^/worker/(activity|workflow|query)-tasks/[^/]+/fail$~D', $path) === 1
            && is_array($body['failure'] ?? null)) {
            return 'failure';
        }
        if (preg_match('~^/worker/workflow-tasks/[^/]+/complete$~D', $path) === 1
            && is_array($body['commands'] ?? null) && $this->hasFailureCommand($body['commands'])) {
            return 'commands';
        }
        return null;
    }

    /** @param array<array-key, mixed> $commands */
    private function hasFailureCommand(array $commands): bool
    {
        foreach ($commands as $command) {
            if (is_array($command) && $this->isFailureCommand($command)) {
                return true;
            }
        }
        return false;
    }

    /** @param array<string, mixed> $command */
    private function isFailureCommand(array $command): bool
    {
        if (in_array($command['type'] ?? null, ['fail_workflow', 'fail_update'], true)) {
            return true;
        }
        if (($command['type'] ?? null) !== 'record_local_activity') {
            return false;
        }
        if (in_array($command['outcome'] ?? null, ['failed', 'timed_out', 'cancelled'], true)) {
            return true;
        }
        foreach ($command['attempts'] ?? [] as $attempt) {
            if (is_array($attempt) && in_array($attempt['outcome'] ?? null, ['failed', 'timed_out', 'cancelled'], true)) {
                return true;
            }
        }
        return false;
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private function preparedOutcome(string $path, array $body): array
    {
        $limit = $this->effectiveLimit();
        $fixed = $body;
        $fixed['report'] = [];
        $available = $limit - JsonDocument::size($fixed) - strlen($path);
        if ($available < 1) {
            throw new FailureReportRefused($limit);
        }
        $body['report'] = LocalFailureReports::bound($body['report'], $available);
        return $body;
    }

    /** @internal Known body budget shared with pre-observation local normalization. */
    public function effectiveLimit(): int
    {
        $limit = $this->limits->requestBytes;
        if (!$this->transport instanceof RequestSizeLimitTransport) {
            return $limit;
        }
        try {
            $known = $this->transport->maximumRequestBytes();
        } catch (\Throwable $error) {
            throw new FailureReportRefused($limit, $error);
        }
        if ($known !== null && $known < 1) {
            throw new FailureReportRefused($limit);
        }
        return $known === null ? $limit : min($limit, $known);
    }

    /** @internal The callback IPC carries the same bounded report before HTTP admission.
     * @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public function activityEnvelope(array $body): array
    {
        return $this->bound($body, 'diagnostics', false, $this->limits->requestBytes, 0);
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private function bound(array $body, ?string $carrier, bool $commands, int $limit, int $pathBytes, bool $enforceRequestLimit = true): array
    {
        $messageLimit = FailureReportLimits::MESSAGE_BYTES;
        $stackLimit = FailureReportLimits::STACK_BYTES;
        $dropMetadata = false;
        for ($pass = 0; $pass < 16; ++$pass) {
            $candidate = $body;
            if ($carrier !== null) {
                $candidate['failure'] = $this->report($body['failure'], $messageLimit, $stackLimit,
                    $dropMetadata, $carrier);
            }
            if ($commands) {
                $candidate['commands'] = $this->commands($body['commands'], $messageLimit, $stackLimit, $dropMetadata);
            }
            try {
                $bytes = JsonDocument::size($candidate);
                if (!$enforceRequestLimit || $bytes + $pathBytes <= $limit) {
                    return $candidate;
                }
            } catch (\JsonException $error) {
                throw new FailureReportRefused($limit, $error);
            }
            if (!$dropMetadata) {
                $dropMetadata = true;
            } else {
                $messageLimit = intdiv($messageLimit, 2);
                $stackLimit = intdiv($stackLimit, 2);
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
        $message = $this->text((string) ($original['message'] ?? ''), $messageLimit, 'message');
        $stack = $this->text((string) ($original['stack_trace'] ?? ''), $stackLimit, 'stack_trace');
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
        $changes = $this->priorChanges($original) + $changes;
        $message = $this->text((string) ($original['message'] ?? ''), $messageLimit, 'message', true);
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
            $stack = $this->text((string) ($original['stack_trace'] ?? ''), $available, 'stack_trace');
            $report['stack_trace'] = $stack['value'].$suffix;
        }

        return $report;
    }

    /** @return array{value: string, changed: bool, original_bytes: int, retained_bytes: int} */
    private function text(string $original, int $limit, string $field, bool $forceMarker = false): array
    {
        return FailureText::bound($original, max($limit, FailureText::minimumBytes($original, $field)), $field, $forceMarker);
    }

    /** Preserve bounded original-size provenance when an IPC report is narrowed again.
     * @param array<string, mixed> $original
     * @return array<string, mixed>
     */
    private function priorChanges(array $original): array
    {
        $fields = $original['diagnostics']['truncation']['fields'] ?? [];
        if (!is_array($fields)) {
            return [];
        }
        $result = [];
        foreach (['message', 'stack_trace', 'exception.message', ...self::METADATA] as $field) {
            $entry = $fields[$field] ?? null;
            $bytes = is_array($entry) ? ($entry['original_bytes'] ?? null) : null;
            if (is_int($bytes) && $bytes >= 0) {
                $result[$field] = ['original_bytes' => $bytes];
            }
        }
        return $result;
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

    /** @param array<array-key, mixed> $commands
     * @return array<array-key, mixed>
     */
    private function commands(array $commands, int $messageLimit, int $stackLimit, bool $dropMetadata): array
    {
        foreach ($commands as $index => $command) {
            if (is_array($command) && in_array($command['type'] ?? null, ['fail_workflow', 'fail_update'], true)) {
                $commands[$index] = $this->command($command, $messageLimit, $stackLimit, $dropMetadata);
            }
        }
        return $commands;
    }

    /** @param array<string, mixed> $command
     * @return array<string, mixed>
     */
    private function command(array $command, int $messageLimit, int $stackLimit, bool $dropMetadata): array
    {
        if (($command['type'] ?? null) === 'fail_update') {
            return $this->updateCommand($command, $messageLimit);
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
            ? $this->text((string) $exception['message'], $messageLimit, 'exception.message') : null;
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
    /** @param array<string, mixed> $command
     * @return array<string, mixed>
     */
    private function updateCommand(array $command, int $messageLimit): array
    {
        $originalMessage = (string) ($command['message'] ?? '');
        $bounded = $this->text($originalMessage, $messageLimit, 'message');
        $command['message'] = $bounded['value'];
        if ($bounded['changed']) {
            $suffix = "\n[truncation diagnostics]\n".JsonDocument::encode(['original_message_bytes' => strlen($originalMessage)]);
            $command['message'] = $this->text($originalMessage,
                $messageLimit - strlen($suffix), 'message', true)['value'].$suffix;
        }
        return $command;
    }

}
