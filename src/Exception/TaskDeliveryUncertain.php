<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/** Retry only the original logical poll; this exception grants no execution. */
final class TaskDeliveryUncertain extends ServerException
{
    public function __construct(string $pollRequestId, int $status, ?Throwable $previous = null)
    {
        parent::__construct('Task delivery is not confirmed. Retry the same poll and selected offer.',
            $status, 'task_delivery_outcome_unknown', [
                'task' => null, 'poll_request_id' => $pollRequestId,
                'retry_same_poll_request_id' => true,
                ...($previous instanceof ServerException && is_int($previous->details['retry_after_seconds'] ?? null)
                    && $previous->details['retry_after_seconds'] > 0
                    ? ['retry_after_seconds' => $previous->details['retry_after_seconds']] : []),
            ], $previous);
    }
}
