<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\ActivityHeartbeatResumeUnavailable;
use Closure;

/** Activity attempt metadata and heartbeat/cancellation support. */
final class ActivityContext
{
    public readonly ActivityRetryInfo $retryInfo;
    /** Original workflow_id from the public claim, or known parent local task. */
    public readonly ?string $workflowId;
    /** Original run_id from the public claim; unavailable legacy identity stays null. */
    public readonly ?string $runId;
    /** Logical execution from the claim, never inferred from task/attempt identity. */
    public readonly ?string $activityExecutionId;
    private readonly ActivityHeartbeatResume $heartbeatResume;

    /**
     * @param (Closure(array<array-key, mixed>): array<string, mixed>)|null $heartbeatRequest
     * @param array<string, mixed> $taskMetadata Received claim metadata; omitted for Server-free tests.
     * @param ActivityHeartbeatResume|null $heartbeatResume @internal Negotiated claim state; null is unavailable.
     */
    public function __construct(
        private readonly Client $client,
        public readonly string $taskId,
        public readonly string $activityAttemptId,
        public readonly string $leaseOwner,
        public readonly string $activityType,
        public readonly int $attemptNumber,
        private readonly ?Closure $localHeartbeat = null,
        private readonly ?Closure $heartbeatRequest = null,
        array $taskMetadata = [],
        ?ActivityHeartbeatResume $heartbeatResume = null,
    ) {
        $this->retryInfo = ActivityRetryInfo::fromTask($attemptNumber, $taskMetadata);
        $this->heartbeatResume = $heartbeatResume ?? ActivityHeartbeatResume::unavailable();
        // Public claim keys take precedence, including explicit null. Retain the
        // internal-name aliases used by local parents and 3.6.0 constructor callers.
        $this->workflowId = self::receivedIdentity(array_key_exists('workflow_id', $taskMetadata)
            ? $taskMetadata['workflow_id'] : ($taskMetadata['workflow_instance_id'] ?? null));
        $this->runId = self::receivedIdentity(array_key_exists('run_id', $taskMetadata)
            ? $taskMetadata['run_id'] : ($taskMetadata['workflow_run_id'] ?? null));
        $this->activityExecutionId = self::receivedIdentity($taskMetadata['activity_execution_id'] ?? null);
    }

    /** The negotiated heartbeat-resume contract version, or null when unavailable. Not a write sequence. */
    public function heartbeatResumeVersion(): ?int
    {
        return $this->heartbeatResume->available ? 1 : null;
    }

    /**
     * The acknowledged checkpoint received with this claim, or null when the
     * supported claim has none.
     *
     * Throws for an older or nonparticipating Server, local activities and
     * Server-free tests, which never invent an empty checkpoint. Heartbeats
     * sent by this attempt never change the received snapshot.
     *
     * @throws ActivityHeartbeatResumeUnavailable
     */
    public function lastHeartbeatCheckpoint(): ?ActivityHeartbeatCheckpoint
    {
        return $this->heartbeatResume->checkpoint();
    }

    /**
     * The acknowledged details from {@see lastHeartbeatCheckpoint()}, or null without a checkpoint.
     *
     * @throws ActivityHeartbeatResumeUnavailable
     */
    public function lastHeartbeatDetails(): mixed
    {
        return $this->lastHeartbeatCheckpoint()?->details();
    }

    /**
     * The original RFC 3339 acknowledgement time, or null without a checkpoint.
     *
     * @throws ActivityHeartbeatResumeUnavailable
     */
    public function lastHeartbeatAt(): ?string
    {
        return $this->lastHeartbeatCheckpoint()?->acknowledgedAt;
    }

    private static function receivedIdentity(mixed $value): ?string
    {
        return is_string($value) && $value !== '' ? $value : null;
    }

    /** @param array<array-key, mixed> $details */
    public function heartbeat(array $details = []): void
    {
        if ($this->localHeartbeat !== null) {
            ($this->localHeartbeat)($details);

            return;
        }

        $response = $this->heartbeatRequest !== null
            ? ($this->heartbeatRequest)($details)
            : $this->client->heartbeatActivityTask(
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
