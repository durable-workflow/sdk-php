<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** Immutable committed state supplied to query and update handlers. */
final class QueryContext
{
    /**
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    public function __construct(
        public readonly string $workflowId,
        public readonly string $runId,
        public readonly array $history,
        public readonly array $task,
    ) {
    }

    /** @return list<array<string, mixed>> */
    public function events(string $eventType): array
    {
        return array_values(array_filter(
            $this->history,
            static fn (array $event): bool => ($event['event_type'] ?? $event['type'] ?? null) === $eventType,
        ));
    }
}
