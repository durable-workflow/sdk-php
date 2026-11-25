<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

/**
 * Read-only view of the commands one workflow task produced.
 *
 * It is a copy. The Worker submits the commands the workflow code produced,
 * not this object, so changing or replacing it cannot change history.
 */
final class WorkflowTaskOutcome
{
    /** @param list<array<string, mixed>> $commands */
    public function __construct(
        public readonly array $commands,
    ) {
    }

    /** @return list<string> */
    public function commandTypes(): array
    {
        return array_map(static fn (array $command): string => (string) ($command['type'] ?? ''), $this->commands);
    }
}
