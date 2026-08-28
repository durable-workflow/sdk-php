<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityCancelled;
use Closure;

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
        private readonly ?Closure $localHeartbeat = null,
    ) {
    }

    /** @param array<array-key, mixed> $details */
    public function heartbeat(array $details = []): void
    {
        if ($this->localHeartbeat !== null) {
            ($this->localHeartbeat)($details);

            return;
        }

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
