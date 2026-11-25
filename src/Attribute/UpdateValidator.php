<?php

declare(strict_types=1);

namespace DurableWorkflow\Attribute;

use Attribute;

/**
 * Marks a public workflow method as the pre-acceptance validator for an update.
 *
 * The method takes a QueryContext followed by the update's arguments. The
 * Server asks the worker to run it before the update is accepted. Throwing,
 * or returning false, rejects the update with `update_validator_rejected`:
 * it is never accepted and its handler never runs. Validators read committed
 * history through the QueryContext and cannot schedule durable work.
 */
#[Attribute(Attribute::TARGET_METHOD)]
final class UpdateValidator
{
    public function __construct(public readonly string $update)
    {
    }
}
