<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Client;

/** Typed create/use/renew/close handle for one worker-held session lease. */
final class WorkerSession
{
    private bool $closed = false;

    /** @var array<string, mixed>|null */
    private ?array $closeResponse = null;

    public function __construct(
        private readonly Client $client,
        public readonly string $workerId,
        public readonly WorkerSessionOptions $options,
    ) {
    }

    /** @return array<string, mixed> */
    public function create(): array
    {
        $this->closed = false;
        $this->closeResponse = null;

        return $this->client->createWorkerSession($this->workerId, $this->options);
    }

    /** @return array<string, mixed> */
    public function renew(?int $leaseSeconds = null): array
    {
        if ($this->closed) {
            throw new \LogicException('A closed worker session cannot be renewed.');
        }

        return $this->client->renewWorkerSession(
            $this->workerId,
            $this->options->sessionId,
            $leaseSeconds ?? $this->options->leaseSeconds,
        );
    }

    /** @return array<string, mixed> */
    public function close(string $reason = 'worker_shutdown'): array
    {
        if ($this->closed && $this->closeResponse !== null) {
            return $this->closeResponse;
        }

        $response = $this->client->closeWorkerSession(
            $this->workerId,
            $this->options->sessionId,
            $reason,
        );
        $this->closed = true;
        $this->closeResponse = $response;

        return $response;
    }

    /** @return array<string, mixed> */
    public function activityOptions(): array
    {
        return ['worker_session' => $this->options->toWire()];
    }

    public function rebuildRequiredAfterHolderLoss(): bool
    {
        return true;
    }
}
