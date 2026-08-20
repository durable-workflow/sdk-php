<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

/** Optional service-mode start fields beyond the Laravel bridge defaults. */
final class WorkflowStartOptions
{
    /**
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
    public function __construct(
        public readonly ?string $taskQueue = null,
        public readonly ?string $workflowType = null,
        public readonly int $executionTimeoutSeconds = 3600,
        public readonly int $runTimeoutSeconds = 600,
        public readonly ?string $duplicatePolicy = null,
        public readonly ?array $memo = null,
        public readonly ?array $searchAttributes = null,
        public readonly ?int $priority = null,
        public readonly ?string $fairnessKey = null,
        public readonly ?int $fairnessWeight = null,
        public readonly ?string $buildId = null,
    ) {
    }
}
