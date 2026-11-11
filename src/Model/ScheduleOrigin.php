<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use DateTimeImmutable;

/**
 * The schedule fire that started a run, read from the run's own ScheduleTriggered history event.
 *
 * occurrenceTime is the time the scheduler evaluated: the jittered fire time for a tick, or the
 * enumerated time for a backfill. It is not the run's actual start time. It is null for a fire
 * recorded without an occurrence.
 */
final class ScheduleOrigin
{
    public function __construct(
        public readonly string $scheduleId,
        public readonly ?DateTimeImmutable $occurrenceTime,
        public readonly ?int $triggerNumber,
    ) {
    }
}
