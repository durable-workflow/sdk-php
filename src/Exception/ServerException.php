<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

class ServerException extends DurableWorkflowException
{
    /** @param array<string, mixed>|list<mixed>|null $details */
    public function __construct(
        string $message,
        public readonly int $status,
        public readonly ?string $reason = null,
        public readonly ?array $details = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $status, $previous);
    }

    public function isTransientConnectionFailure(): bool
    {
        $transport = $this->getPrevious();

        return $this->status === 0
            && $transport instanceof TransportException
            && $transport->status === null
            && $transport->transientConnectionFailure;
    }

    /** Whether the Server explicitly refused admission and requested a same-identity retry. */
    public function isStorageAdmissionFailure(?string $pollRequestId = null): bool
    {
        $response = $this->details;
        if ($this->status !== 503
            || !in_array($this->reason, ['storage_pressure', 'storage_admission_unavailable'], true)
            || $response === null || array_is_list($response)
            || ($response['reason'] ?? null) !== $this->reason
            || ($response['retryable'] ?? null) !== true
            || !is_int($response['retry_after_seconds'] ?? null)
            || $response['retry_after_seconds'] <= 0
            || !in_array($response['storage_state'] ?? null, ['draining', 'fenced'], true)
            || ($this->reason === 'storage_admission_unavailable' && $response['storage_state'] !== 'fenced')
            || (array_key_exists('request_admitted', $response) && $response['request_admitted'] !== false)) {
            return false;
        }

        if ($pollRequestId === null) {
            return ($response['request_admitted'] ?? null) === false;
        }

        // A refused claim does not resolve a prior request's uncertain outcome.
        // Preserve its poll identity even when pressure starts during a long poll.
        return array_key_exists('task', $response) && $response['task'] === null
            && ($response['poll_status'] ?? null) === $this->reason
            && ($response['poll_request_id'] ?? null) === $pollRequestId
            && ($response['retry_same_poll_request_id'] ?? null) === true
            && ($response['claim_admitted'] ?? null) === false;
    }
}
