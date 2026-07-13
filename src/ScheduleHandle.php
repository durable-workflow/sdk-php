<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Model\ScheduleDescription;

/** Operations bound to a schedule ID. */
final class ScheduleHandle
{
    public function __construct(private readonly Client $client, public readonly string $scheduleId)
    {
    }

    public function describe(): ScheduleDescription
    {
        return $this->client->describeSchedule($this->scheduleId);
    }

    /** @param array<string, mixed> $changes */
    public function update(array $changes): void
    {
        $this->client->updateSchedule($this->scheduleId, $changes);
    }

    public function pause(?string $note = null): void
    {
        $this->client->pauseSchedule($this->scheduleId, $note);
    }

    public function resume(?string $note = null): void
    {
        $this->client->resumeSchedule($this->scheduleId, $note);
    }

    /** @return array<string, mixed> */
    public function trigger(?string $overlapPolicy = null): array
    {
        return $this->client->triggerSchedule($this->scheduleId, $overlapPolicy);
    }

    /** @return array<string, mixed> */
    public function backfill(string $startTime, string $endTime, ?string $overlapPolicy = null): array
    {
        return $this->client->backfillSchedule($this->scheduleId, $startTime, $endTime, $overlapPolicy);
    }

    public function delete(): void
    {
        $this->client->deleteSchedule($this->scheduleId);
    }

    /** @return array<string, mixed> */
    public function history(?int $limit = null, ?int $afterSequence = null): array
    {
        return $this->client->scheduleHistory($this->scheduleId, $limit, $afterSequence);
    }
}
