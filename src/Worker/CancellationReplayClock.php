<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use LogicException;

/** @internal Advances only when replay consumes a committed blocking boundary. */
final class CancellationReplayClock
{
    private ?DateTimeImmutable $time = null;

    private bool $available = false;

    /** @param array<string, mixed>|null $event */
    public function observe(?array $event): void
    {
        $value = $event['timestamp'] ?? $event['recorded_at'] ?? null;
        $this->available = false;
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            return;
        }
        try {
            $time = new DateTimeImmutable($value);
        } catch (\Exception) {
            return;
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            return;
        }
        if ($this->time === null || $time > $this->time) {
            $this->time = $time;
        }
        $this->available = true;
    }

    public function time(): DateTimeImmutable
    {
        if (!$this->available || $this->time === null) {
            throw new LogicException('Cancellation remaining() requires a valid timestamp at the replayed boundary.');
        }

        return $this->time;
    }
}
