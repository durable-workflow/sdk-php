<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class ScheduleDescription
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $scheduleId,
        public readonly ?string $status,
        public readonly ?string $nextFireAt,
        public readonly int $firesCount,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value, string $fallbackId = ''): self
    {
        return new self(
            (string) ($value['schedule_id'] ?? $fallbackId),
            isset($value['status']) ? (string) $value['status'] : null,
            isset($value['next_fire_at']) ? (string) $value['next_fire_at'] : null,
            (int) ($value['fires_count'] ?? 0),
            $value,
        );
    }
}
