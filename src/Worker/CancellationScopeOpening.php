<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal An authored opening awaiting a canonical retained-claim receipt. */
final class CancellationScopeOpening
{
    public function __construct(
        public readonly int $sequence,
        public readonly string $parentScopeId,
        public readonly bool $shieldParent,
    ) {
    }
}
