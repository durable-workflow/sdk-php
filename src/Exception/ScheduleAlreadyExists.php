<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/**
 * The Server refused to create a schedule because its id is taken (409 schedule_already_exists).
 *
 * Schedule ids are never reused, including after a delete or after max_runs runs out. Deploy tooling
 * that applies schedules from configuration can catch this and update the existing schedule instead.
 * Status, reason and details are kept from the Server's answer.
 */
final class ScheduleAlreadyExists extends ServerException
{
    /** @param array<string, mixed>|list<mixed>|null $details */
    public function __construct(
        public readonly ?string $scheduleId,
        string $message,
        int $status,
        ?string $reason,
        ?array $details,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $status, $reason, $details, $previous);
    }

    public static function fromServerException(ServerException $exception, ?string $requestedScheduleId): self
    {
        $details = $exception->details;
        $scheduleId = is_array($details) && !array_is_list($details) && is_string($details['schedule_id'] ?? null)
            ? $details['schedule_id']
            : $requestedScheduleId;

        return new self(
            $scheduleId,
            $exception->getMessage(),
            $exception->status,
            $exception->reason,
            $details,
            $exception,
        );
    }
}
