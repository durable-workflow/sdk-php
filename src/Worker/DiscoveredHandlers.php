<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Validated handlers discovered from one class-oriented service. */
final class DiscoveredHandlers
{
    /**
     * @param array<string, HandlerDefinition> $workflows
     * @param array<string, HandlerDefinition> $activities
     * @param array<string, array<string, HandlerDefinition>> $queries
     * @param array<string, array<string, callable>> $signals
     * @param array<string, array<string, HandlerDefinition>> $updates
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
