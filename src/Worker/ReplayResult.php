<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

final class ReplayResult
{
    /** @param list<array<string, mixed>> $commands */
    public function __construct(public readonly array $commands)
    {
    }
}
