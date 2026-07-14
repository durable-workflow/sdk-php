<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Immutable routing and waiting options for a service operation. */
final class ServiceOperationOptions
{
    /**
     * @param array<string, mixed>|null $labels
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
    public function __construct(
        public readonly ?string $modeOverride = null,
        public readonly ?string $waitFor = null,
        public readonly ?int $waitTimeoutSeconds = null,
        public readonly ?string $idempotencyKey = null,
        public readonly ?string $callerNamespace = null,
        public readonly ?string $callerWorkflowId = null,
        public readonly ?string $callerRunId = null,
        public readonly ?string $targetWorkflowId = null,
        public readonly ?string $targetRunId = null,
        public readonly ?string $connection = null,
        public readonly ?string $taskQueue = null,
        public readonly ?string $businessKey = null,
        public readonly ?array $labels = null,
        public readonly ?array $memo = null,
        public readonly ?array $searchAttributes = null,
        public readonly ?string $duplicateStartPolicy = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return array_filter([
            'mode_override' => $this->modeOverride,
            'wait_for' => $this->waitFor,
            'wait_timeout_seconds' => $this->waitTimeoutSeconds,
            'idempotency_key' => $this->idempotencyKey,
            'caller_namespace' => $this->callerNamespace,
            'caller_workflow_instance_id' => $this->callerWorkflowId,
            'caller_workflow_run_id' => $this->callerRunId,
            'target_workflow_instance_id' => $this->targetWorkflowId,
            'target_workflow_run_id' => $this->targetRunId,
            'connection' => $this->connection,
            'queue' => $this->taskQueue,
            'business_key' => $this->businessKey,
            'labels' => $this->labels,
            'memo' => $this->memo,
            'search_attributes' => $this->searchAttributes,
            'duplicate_start_policy' => $this->duplicateStartPolicy,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
