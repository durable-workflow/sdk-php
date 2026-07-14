<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** One namespace-scoped page from the schedule list API. */
final class SchedulePage
{
    /**
     * @param list<ScheduleDescription> $schedules
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly array $schedules,
        public readonly ?string $nextPageToken,
        public readonly array $raw,
    ) {
    }
}
