<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Validated handlers discovered from one class-oriented service. */
final class DiscoveredHandlers
{
    /**
     * @param array<string, callable> $workflows
     * @param array<string, callable> $activities
     * @param array<string, array<string, callable>> $queries
     * @param array<string, array<string, callable>> $signals
     * @param array<string, array<string, callable>> $updates
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
