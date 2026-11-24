<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;

/** The first selected packet remains immutable through uncertain replies. */
final class TaskDeliveryAttempt
{
    /**
     * @param list<string> $allowedKinds
     * @param array<string, mixed> $body
     */
    private function __construct(
        public readonly string $workerId,
        public readonly string $path,
        public readonly string $taskId,
        public readonly string $taskKind,
        private readonly array $allowedKinds,
        private readonly array $body,
        public readonly int $expiresAtMs,
        /** Monotonic milliseconds after which an unconfirmed acceptance is released. */
        public readonly int $releaseAtMs,
    ) {
    }

    /**
     * @param array<string, mixed> $body
     * @param array<string, mixed> $response
     * @param list<string> $allowedKinds
     * @param int $acceptanceTimeoutMs The negotiated offer acceptance timeout.
     */
    public static function fromResponse(string $path, array $body, array $response, array $allowedKinds,
        int $acceptanceTimeoutMs, int $wallNowMs, int $monotonicNowMs): self
    {
        $offer = $response['task_offer'] ?? null;
        if (!array_key_exists('task', $response) || $response['task'] !== null || ($response['poll_status'] ?? null) !== 'offered'
            || !is_array($offer) || array_is_list($offer)
            || ($offer['schema'] ?? null) !== TaskDelivery::SCHEMA
            || !is_string($offer['task_kind'] ?? null)
            || !in_array($offer['task_kind'], $allowedKinds, true)
            || !is_string($offer['task_id'] ?? null) || $offer['task_id'] === ''
            || !is_string($offer['token'] ?? null) || $offer['token'] === '' || strlen($offer['token']) > 8192
            || !is_int($offer['expires_at_ms'] ?? null) || $offer['expires_at_ms'] < 1) {
            throw new ServerException('The server returned an invalid task offer.', 200, 'invalid_task_delivery_offer');
        }

        // Anchor the packet's own deadline locally. The negotiated timeout caps a worker clock that runs
        // behind the Server; the grace covers one that runs ahead and a replayable committed claim.
        $remaining = max(0, min($acceptanceTimeoutMs, $offer['expires_at_ms'] - $wallNowMs));

        return new self((string) $body['worker_id'], $path, $offer['task_id'], $offer['task_kind'], $allowedKinds,
            array_replace($body, ['task_offer_token' => $offer['token'], 'timeout_seconds' => 0]),
            $offer['expires_at_ms'], $monotonicNowMs + $remaining + TaskDelivery::UNCONFIRMED_GRACE_MS);
    }

    /** @param list<string> $allowedKinds */
    public function isForKinds(array $allowedKinds): bool
    {
        return $this->allowedKinds === $allowedKinds;
    }

    /** @return array<string, mixed> */
    public function requestBody(): array
    {
        return $this->body;
    }

    /** @return array<string, string> */
    public function __debugInfo(): array
    {
        return ['worker_id' => $this->workerId, 'path' => $this->path, 'task_id' => $this->taskId];
    }
}
