<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Lifecycle and backlog metadata for one run-scoped Workflow Stream. */
final class WorkflowStreamDescription
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $streamName,
        public readonly string $status,
        public readonly int $lastOffset,
        public readonly int $totalItems,
        public readonly int $pendingItems,
        public readonly ?string $openedAt,
        public readonly ?string $lastAppendedAt,
        public readonly ?string $closedAt,
        public readonly ?string $errorReason,
        public readonly ?int $retentionSeconds,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        return new self(
            (string) ($value['stream_name'] ?? ''),
            (string) ($value['status'] ?? 'open'),
            (int) ($value['last_offset'] ?? -1),
            (int) ($value['total_items'] ?? 0),
            (int) ($value['pending_items'] ?? 0),
            isset($value['opened_at']) ? (string) $value['opened_at'] : null,
            isset($value['last_appended_at']) ? (string) $value['last_appended_at'] : null,
            isset($value['closed_at']) ? (string) $value['closed_at'] : null,
            isset($value['error_reason']) ? (string) $value['error_reason'] : null,
            isset($value['retention_seconds']) ? (int) $value['retention_seconds'] : null,
            $value,
        );
    }

    public function isTerminal(): bool
    {
        return $this->status === 'closed' || $this->status === 'errored';
    }
}
