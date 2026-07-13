<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

final class WorkflowRun
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly string $workflowId,
        public readonly string $runId,
        public readonly string $workflowType,
        public readonly ?string $status,
        public readonly bool $isCurrentRun,
        public readonly array $raw,
    ) {
    }
}
