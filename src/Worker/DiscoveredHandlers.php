<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Validated handlers discovered from one class-oriented service. */
final class DiscoveredHandlers
{
    /**
     * @param array<array-key, HandlerDefinition> $workflows
     * @param array<array-key, HandlerDefinition> $activities
     * @param array<array-key, array<array-key, HandlerDefinition>> $queries
     * @param array<array-key, array<array-key, callable>> $signals
     * @param array<array-key, array<array-key, HandlerDefinition>> $updates
     */
    public function __construct(
        public readonly string $class,
        public readonly array $workflows,
        public readonly array $activities,
        public readonly array $queries,
        public readonly array $signals,
        public readonly array $updates,
    ) {
    }
}
