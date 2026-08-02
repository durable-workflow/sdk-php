<?php

declare(strict_types=1);

namespace DurableWorkflow\Attribute;

use Attribute;

/** Marks a workflow method whose signature declares an accepted signal contract. */
#[Attribute(Attribute::TARGET_METHOD)]
final class Signal
{
    public function __construct(public readonly ?string $name = null)
    {
    }
}
