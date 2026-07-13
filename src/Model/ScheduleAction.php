<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use DurableWorkflow\Codec\PayloadCodec;

/** Workflow-start action run by a schedule. */
final class ScheduleAction
{
    /** @param list<mixed> $input */
    public function __construct(
        public readonly string $workflowType,
        public readonly ?string $taskQueue = null,
        public readonly array $input = [],
        public readonly ?int $executionTimeoutSeconds = null,
        public readonly ?int $runTimeoutSeconds = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toArray(PayloadCodec $codec): array
    {
        return array_filter([
            'workflow_type' => $this->workflowType,
            'task_queue' => $this->taskQueue,
            'input' => $codec->envelope($this->input),
            'execution_timeout_seconds' => $this->executionTimeoutSeconds,
            'run_timeout_seconds' => $this->runTimeoutSeconds,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
