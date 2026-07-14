<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** One page from the workflow visibility API. */
final class WorkflowPage
{
    /**
     * @param list<WorkflowExecution> $executions
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly array $executions,
        public readonly ?string $nextPageToken,
        public readonly int $workflowCount,
        public readonly array $raw,
    ) {
    }
}
