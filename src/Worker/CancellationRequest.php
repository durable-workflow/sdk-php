<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use InvalidArgumentException;

/** Server-owned request identity, deadline and opaque history refresh location. */
final class CancellationRequest
{
    private function __construct(
        public readonly string $requestId,
        public readonly string $requestedAt,
        public readonly string $cleanupDeadlineAt,
        public readonly ?string $historyRefreshPageToken,
        public readonly ?CancellationContext $context = null,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromObservation(array $value): self
    {
        $requestId = self::text($value, 'request_id');
        $requestedAt = self::text($value, 'requested_at');
        $cleanupDeadlineAt = self::text($value, 'cleanup_deadline_at');
        $historyRefreshPageToken = self::text($value, 'history_refresh_page_token');

        return self::validated($requestId, $requestedAt, $cleanupDeadlineAt, $historyRefreshPageToken);
    }

    /** @param array<string, mixed> $payload */
    public static function fromHistoryPayload(array $payload, string $recordedAt): self
    {
        $context = null;
        if (array_key_exists('cancellation', $payload)) {
            $snapshot = $payload['cancellation'];
            if (!is_array($snapshot) || array_is_list($snapshot)) {
                throw new InvalidArgumentException('Canonical cancellation context must be an object.');
            }
            $context = CancellationContext::fromArray($snapshot);
            $last = $context->lineage[count($context->lineage) - 1];
            if ($context->requestId !== self::text($payload, 'workflow_command_id')
                || $last['workflow_run_id'] !== self::text($payload, 'workflow_run_id')
                || (array_key_exists('workflow_instance_id', $payload)
                    && $last['workflow_instance_id'] !== $payload['workflow_instance_id'])
                || $context->deadline() != self::timestamp(self::text($payload, 'cleanup_deadline_at'))
                || $context->requestedAt() > self::timestamp($recordedAt)
                || (array_key_exists('reason', $payload) && $context->reason !== $payload['reason'])) {
                throw new InvalidArgumentException('Canonical cancellation context does not match its request event.');
            }
        }

        return self::validated(
            self::text($payload, 'workflow_command_id'),
            $context?->requestedAt()->format('Y-m-d\TH:i:s.u\Z') ?? $recordedAt,
            self::text($payload, 'cleanup_deadline_at'), null, $context,
        );
    }

    /** Preserve the canonical snapshot while retaining the observed refresh route. */
    public function withHistoryRefreshPageToken(?string $pageToken): self
    {
        return new self($this->requestId, $this->requestedAt, $this->cleanupDeadlineAt, $pageToken, $this->context);
    }

    private static function validated(
        string $requestId,
        string $requestedAt,
        string $cleanupDeadlineAt,
        ?string $historyRefreshPageToken,
        ?CancellationContext $context = null,
    ): self {
        $requested = self::timestamp($requestedAt);
        $deadline = self::timestamp($cleanupDeadlineAt);
        if ($deadline <= $requested) {
            throw new InvalidArgumentException('Cleanup deadline must follow the original request.');
        }

        return new self(
            $requestId, $requestedAt, $cleanupDeadlineAt, $historyRefreshPageToken, $context,
        );
    }

    /** @param array<string, mixed> $value */
    private static function text(array $value, string $key): string
    {
        $text = $value[$key] ?? null;
        if (!is_string($text) || trim($text) === '') {
            throw new InvalidArgumentException("Cancellation {$key} must be a non-empty string.");
        }

        return $text;
    }

    private static function timestamp(string $value): DateTimeImmutable
    {
        if (preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Cancellation timestamps require an ISO date, time and timezone.');
        }
        try {
            $timestamp = new DateTimeImmutable($value);
        } catch (\Throwable $error) {
            throw new InvalidArgumentException('Cancellation timestamp is invalid.', previous: $error);
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Cancellation timestamp is invalid.');
        }

        return $timestamp;
    }
}
