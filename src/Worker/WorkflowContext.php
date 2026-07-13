<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\WorkflowCancelled;

/** Deterministic helpers available while a workflow generator is replayed. */
final class WorkflowContext
{
    /** @param list<array<string, mixed>> $history */
    public function __construct(
        public readonly string $workflowId,
        public readonly string $runId,
        private readonly array $history,
        private readonly PayloadCodec $codec,
        private readonly bool $cancellationRequested = false,
    ) {
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function activity(string $activityType, array $arguments = [], array $options = []): WorkflowCommand
    {
        return WorkflowCommand::activity($activityType, $arguments, $options);
    }

    public function sleep(int|float $seconds): WorkflowCommand
    {
        return WorkflowCommand::timer((int) ceil($seconds));
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function childWorkflow(string $workflowType, array $arguments = [], array $options = []): WorkflowCommand
    {
        return WorkflowCommand::childWorkflow($workflowType, $arguments, $options);
    }

    /** @param callable(): mixed $operation */
    public function sideEffect(callable $operation): WorkflowCommand
    {
        return WorkflowCommand::sideEffect($operation);
    }

    /** @param list<mixed> $arguments */
    public function continueAsNew(
        array $arguments = [],
        ?string $workflowType = null,
        ?string $taskQueue = null,
    ): WorkflowCommand {
        return WorkflowCommand::continueAsNew($arguments, $workflowType, $taskQueue);
    }

    /** @param array<string, mixed> $attributes */
    public function upsertSearchAttributes(array $attributes): WorkflowCommand
    {
        return WorkflowCommand::upsertSearchAttributes($attributes);
    }

    public function isCancellationRequested(): bool
    {
        return $this->cancellationRequested;
    }

    public function throwIfCancellationRequested(): void
    {
        if ($this->cancellationRequested) {
            throw new WorkflowCancelled('Workflow cancellation was requested.');
        }
    }

    /** @return list<list<mixed>> */
    public function signals(string $signalName): array
    {
        $signals = [];
        foreach ($this->history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SignalReceived') {
                continue;
            }
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            if (($payload['signal_name'] ?? null) !== $signalName) {
                continue;
            }
            $raw = $payload['value'] ?? $payload['input'] ?? $payload['arguments'] ?? null;
            $decoded = (is_array($raw) || is_string($raw)) ? $this->codec->decodeEnvelope($raw) : null;
            $signals[] = is_array($decoded) && array_is_list($decoded) ? $decoded : [$decoded];
        }

        return $signals;
    }

    /** @return list<list<mixed>> */
    public function updates(string $updateName): array
    {
        $updates = [];
        $seen = [];
        foreach ($this->history as $event) {
            if (!in_array($event['event_type'] ?? $event['type'] ?? null, ['UpdateAccepted', 'UpdateApplied'], true)) {
                continue;
            }
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            if (($payload['update_name'] ?? null) !== $updateName || !isset($payload['arguments'])) {
                continue;
            }
            $updateId = isset($payload['update_id']) ? (string) $payload['update_id'] : '';
            if ($updateId !== '' && isset($seen[$updateId])) {
                continue;
            }
            if ($updateId !== '') {
                $seen[$updateId] = true;
            }
            $raw = $payload['arguments'];
            $decoded = (is_array($raw) || is_string($raw)) ? $this->codec->decodeEnvelope($raw) : null;
            $updates[] = is_array($decoded) && array_is_list($decoded) ? $decoded : [$decoded];
        }

        return $updates;
    }
}
