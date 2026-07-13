<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityCancelled;

/** Activity attempt metadata and heartbeat/cancellation support. */
final class ActivityContext
{
    public function __construct(
        private readonly Client $client,
        public readonly string $taskId,
        public readonly string $activityAttemptId,
        public readonly string $leaseOwner,
        public readonly string $activityType,
        public readonly int $attemptNumber,
    ) {
    }

    /** @param array<string, mixed> $details */
    public function heartbeat(array $details = []): void
    {
        $response = $this->client->heartbeatActivityTask(
            $this->taskId,
            $this->activityAttemptId,
            $this->leaseOwner,
            $details,
        );
        if (($response['cancel_requested'] ?? false) === true || ($response['can_continue'] ?? true) === false) {
            throw new ActivityCancelled('The server requested activity cancellation.');
        }
    }
}
