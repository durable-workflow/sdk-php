<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Durable acceptance outcome for one Workflow Stream append batch. */
final class WorkflowStreamAppendResult
{
    /**
     * @param list<int> $acceptedOffsets
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly WorkflowStreamDescription $stream,
        public readonly array $acceptedOffsets,
        public readonly int $accepted,
        public readonly int $deduped,
        public readonly array $raw,
    ) {
    }
}
