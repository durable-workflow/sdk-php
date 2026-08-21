<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * One durable operation prepared for a {@see WorkflowContext::all()} barrier.
 *
 * @api
 */
final class DeferredWorkflowOperation
{
    /** @internal */
    public function __construct(public readonly WorkflowCommand $command)
    {
    }
}
