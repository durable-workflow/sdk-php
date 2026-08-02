<?php

declare(strict_types=1);

namespace DurableWorkflow\Attribute;

use Attribute;

/** Marks a public workflow method as a query handler. */
#[Attribute(Attribute::TARGET_METHOD)]
final class Query
{
    public function __construct(public readonly ?string $name = null)
    {
    }
}
