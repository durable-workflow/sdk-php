<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal An already hydrated original poll grant, never a refreshed observation. */
final class WorkflowPrefetch
{
    /** @param array<string, mixed> $response */
    public function __construct(
        public readonly array $response,
        public readonly ?WorkflowPollGrant $grant,
        public readonly float $startedAt,
    ) {
    }
}
