<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** One bounded, resumable long-poll response from a Workflow Stream. */
final class WorkflowStreamPage
{
    /**
     * @param list<WorkflowStreamItem> $items
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly WorkflowStreamDescription $stream,
        public readonly array $items,
        public readonly int $nextOffset,
        public readonly bool $terminal,
        public readonly array $raw,
    ) {
    }
}
