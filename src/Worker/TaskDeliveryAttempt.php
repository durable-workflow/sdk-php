<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;

/** The first selected packet remains immutable through uncertain replies. */
final class TaskDeliveryAttempt
{
    /** @param array<string, mixed> $body */
    private function __construct(
        public readonly string $workerId,
        public readonly string $path,
        public readonly string $taskId,
        private readonly array $body,
    ) {
    }

    /**
     * @param array<string, mixed> $body
     * @param array<string, mixed> $response
     */
    public static function fromResponse(string $path, array $body, array $response, string $kind): self
    {
        $offer = $response['task_offer'] ?? null;
        if (!array_key_exists('task', $response) || $response['task'] !== null || ($response['poll_status'] ?? null) !== 'offered'
            || !is_array($offer) || array_is_list($offer)
            || ($offer['schema'] ?? null) !== TaskDelivery::SCHEMA
            || ($offer['task_kind'] ?? null) !== str_replace('-', '_', $kind)
            || !is_string($offer['task_id'] ?? null) || $offer['task_id'] === ''
            || !is_string($offer['token'] ?? null) || $offer['token'] === '' || strlen($offer['token']) > 8192
            || !is_int($offer['expires_at_ms'] ?? null) || $offer['expires_at_ms'] < 1) {
            throw new ServerException('The server returned an invalid task offer.', 200, 'invalid_task_delivery_offer');
        }

        return new self((string) $body['worker_id'], $path, $offer['task_id'],
            array_replace($body, ['task_offer_token' => $offer['token'], 'timeout_seconds' => 0]));
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
