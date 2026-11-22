<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** @internal Validated wire shape; Server storage authority remains a separate guarantee. */
final class TaskDeliveryRefusal
{
    private const PAIRS = [
        'task_offer_expired' => 'task_offer_expired',
        'stale_worker_registration' => 'worker_registration_lost_authority',
    ];

    private function __construct(
        public readonly int $status,
        public readonly string $pollStatus,
        public readonly string $reason,
    ) {
    }

    public static function fromJson(int $status, string $raw, bool $progress = false): ?self
    {
        $body = UniqueJson::decode($raw);
        if ($progress) {
            $frame = PollTransport::decode($raw, $status);
            $status = $frame['status'];
            $body = $frame['body'];
        }
        if ($status !== 409 || !is_array($body) || count($body) !== 3
            || !array_key_exists('task', $body) || $body['task'] !== null
            || !is_string($body['poll_status'] ?? null) || !is_string($body['reason'] ?? null)
            || (self::PAIRS[$body['poll_status']] ?? null) !== $body['reason']) {
            return null;
        }

        return new self($status, $body['poll_status'], $body['reason']);
    }

    /** @return array{task: null, poll_status: string, reason: string} */
    public function body(): array
    {
        return ['task' => null, 'poll_status' => $this->pollStatus, 'reason' => $this->reason];
    }
}
