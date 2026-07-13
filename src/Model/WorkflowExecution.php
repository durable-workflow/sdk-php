<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class WorkflowExecution
{
    /** @param array<string, mixed> $raw */
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
    ) {
    }
}
