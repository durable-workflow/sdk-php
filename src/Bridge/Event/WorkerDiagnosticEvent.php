<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Event;

/** A worker lifecycle, retry, shutdown, or handler diagnostic. */
final class WorkerDiagnosticEvent
{
    /** @param array<string, mixed> $context */
    public function __construct(
        public readonly string $name,
        public readonly array $context = [],
    ) {
    }
}
