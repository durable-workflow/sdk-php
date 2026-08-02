<?php

declare(strict_types=1);

namespace DurableWorkflow\Attribute;

use Attribute;

/** Marks the public entry method of a class-oriented workflow. */
#[Attribute(Attribute::TARGET_METHOD)]
final class Workflow
{
    public function __construct(public readonly string $name)
    {
    }
}
