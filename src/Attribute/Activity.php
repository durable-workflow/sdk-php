<?php

declare(strict_types=1);

namespace DurableWorkflow\Attribute;

use Attribute;

/** Marks a public method as an activity handler. */
#[Attribute(Attribute::TARGET_METHOD)]
final class Activity
{
    public function __construct(public readonly ?string $name = null)
    {
    }
}
