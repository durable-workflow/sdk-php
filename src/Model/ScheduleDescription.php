<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class ScheduleDescription
{
    /**
     * @param array<string, mixed> $raw
     * @param array<string, mixed>|null $spec
     * @param array<string, mixed>|null $action
     */
    public function __construct(
        public readonly string $scheduleId,
        public readonly ?string $status,
        public readonly ?string $nextFireAt,
        public readonly int $firesCount,
        public readonly array $raw,
        public readonly ?array $spec = null,
        public readonly ?array $action = null,
        public readonly ?string $overlapPolicy = null,
        public readonly ?string $note = null,
        public readonly ?string $lastFiredAt = null,
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
            isset($value['spec']) && is_array($value['spec']) ? $value['spec'] : null,
            isset($value['action']) && is_array($value['action']) ? $value['action'] : null,
            isset($value['overlap_policy']) ? (string) $value['overlap_policy'] : null,
            isset($value['note']) ? (string) $value['note'] : null,
            isset($value['last_fired_at']) ? (string) $value['last_fired_at'] : null,
        );
    }
}
