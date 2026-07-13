<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use InvalidArgumentException;

/** Portable cron and fixed-interval schedule specification. */
final class ScheduleSpec
{
    /**
     * @param list<string> $cronExpressions
     * @param list<array<string, mixed>> $intervals
     */
    public function __construct(
        public readonly array $cronExpressions = [],
        public readonly array $intervals = [],
        public readonly ?string $timezone = null,
    ) {
        if ($cronExpressions === [] && $intervals === []) {
            throw new InvalidArgumentException('A schedule needs at least one cron expression or interval.');
        }
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return array_filter([
            'cron_expressions' => $this->cronExpressions ?: null,
            'intervals' => $this->intervals ?: null,
            'timezone' => $this->timezone,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
