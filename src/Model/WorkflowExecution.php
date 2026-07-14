<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class WorkflowExecution
{
    /**
     * @param array<string, mixed> $raw
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
    public function __construct(
        public readonly string $workflowId,
        public readonly ?string $runId,
        public readonly string $workflowType,
        public readonly ?string $status,
        public readonly ?string $namespace,
        public readonly ?string $taskQueue,
        public readonly mixed $input,
        public readonly mixed $output,
        public readonly array $raw,
        public readonly ?string $businessKey = null,
        public readonly ?string $statusBucket = null,
        public readonly ?bool $isTerminal = null,
        public readonly ?string $startedAt = null,
        public readonly ?string $closedAt = null,
        public readonly ?array $memo = null,
        public readonly ?array $searchAttributes = null,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(
        array $value,
        string $fallbackWorkflowId = '',
        ?string $fallbackRunId = null,
        mixed $input = null,
        mixed $output = null,
    ): self {
        return new self(
            (string) ($value['workflow_id'] ?? $fallbackWorkflowId),
            isset($value['run_id']) ? (string) $value['run_id'] : $fallbackRunId,
            (string) ($value['workflow_type'] ?? ''),
            isset($value['status']) ? (string) $value['status'] : null,
            isset($value['namespace']) ? (string) $value['namespace'] : null,
            isset($value['task_queue']) ? (string) $value['task_queue'] : null,
            $input,
            $output,
            $value,
            isset($value['business_key']) ? (string) $value['business_key'] : null,
            isset($value['status_bucket']) ? (string) $value['status_bucket'] : null,
            isset($value['is_terminal']) ? (bool) $value['is_terminal'] : null,
            isset($value['started_at']) ? (string) $value['started_at'] : null,
            isset($value['closed_at']) ? (string) $value['closed_at'] : null,
            isset($value['memo']) && is_array($value['memo']) ? $value['memo'] : null,
            isset($value['search_attributes']) && is_array($value['search_attributes'])
                ? $value['search_attributes']
                : null,
        );
    }
}
