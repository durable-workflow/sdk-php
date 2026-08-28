<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** Typed worker-session routing and lease options for activity commands. */
final class WorkerSessionOptions
{
    /** @param list<string> $requirements */
    public function __construct(
        public readonly string $sessionId,
        public readonly ?string $queue = null,
        public readonly array $requirements = [],
        public readonly int $leaseSeconds = 120,
        public readonly int $ttlSeconds = 1800,
        public readonly int $maxConcurrentActivities = 1,
        public readonly bool $createIfMissing = true,
        public readonly bool $allowReacquireAfterFailure = true,
    ) {
        if (trim($sessionId) === '') {
            throw new \InvalidArgumentException('Worker session id must be a non-empty string.');
        }
        if ($leaseSeconds < 1 || $ttlSeconds < 1 || $maxConcurrentActivities < 1) {
            throw new \InvalidArgumentException('Worker session lease, TTL, and concurrency values must be positive.');
        }
        foreach ($requirements as $requirement) {
            if (trim($requirement) === '') {
                throw new \InvalidArgumentException('Worker session requirements must be non-empty strings.');
            }
        }
    }

    /** @return array<string, mixed> */
    public function toWire(): array
    {
        return array_filter([
            'session_id' => trim($this->sessionId),
            'queue' => $this->queue,
            'requirements' => array_values(array_unique(array_map('trim', $this->requirements))),
            'lease_seconds' => $this->leaseSeconds,
            'ttl_seconds' => $this->ttlSeconds,
            'max_concurrent_activities' => $this->maxConcurrentActivities,
            'create_if_missing' => $this->createIfMissing,
            'allow_reacquire_after_failure' => $this->allowReacquireAfterFailure,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
