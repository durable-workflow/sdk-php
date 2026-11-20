<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

final class ActivityFailed extends DurableWorkflowException
{
    /** The class the Server records when an attempt's task lease is lost. */
    public const SERVER_LEASE_LOST_TYPE = 'Workflow\\V2\\Exceptions\\ActivityLeaseExpiredException';
    /** The failure type a Worker reports when it releases a claim it can no longer maintain. */
    public const WORKER_LOST_TYPE = 'DurableWorkflow\\Worker\\ActivityWorkerLost';

    /** @param array<string, mixed>|null $failure */
    public function __construct(
        string $message,
        public readonly ?string $activityType = null,
        public readonly ?string $failureType = null,
        public readonly bool $nonRetryable = false,
        public readonly ?array $failure = null,
        public readonly ?string $historyEventType = null,
    ) {
        parent::__construct($message);
    }

    /**
     * Why the attempt lost its worker, or null when it failed for another
     * reason: `lease_expired` or `worker_deregistered` (settled by the Server),
     * or `worker_lost` (released by a Worker that could not keep its lease).
     * The attempt's callback may not have finished, so retrying on this reason
     * is the caller's choice; the default retry policy is unchanged.
     */
    public function leaseLossReason(): ?string
    {
        $types = [$this->failureType, $this->failure['exception_type'] ?? null, $this->failure['exception_class'] ?? null];
        if (in_array(self::WORKER_LOST_TYPE, $types, true)) {
            return 'worker_lost';
        }
        if (in_array(self::SERVER_LEASE_LOST_TYPE, $types, true)) {
            $reason = $this->failure['failure_reason'] ?? null;

            return is_string($reason) && $reason !== '' ? $reason : 'lease_expired';
        }

        return null;
    }
}
