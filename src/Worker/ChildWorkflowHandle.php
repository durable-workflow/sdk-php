<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use LogicException;

/**
 * A child workflow whose start the runtime has committed.
 *
 * Returned by WorkflowContext::startChildWorkflow() only after a fresh parent
 * turn carries the committed start, so identity() is canonical history. The
 * child keeps running; await() returns its eventual result or throws
 * ChildWorkflowFailed, following the original call through child
 * continue-as-new and retries. The handle sends no command of its own and
 * supports no signal, query, update, cancellation or termination.
 */
final class ChildWorkflowHandle
{
    /** @internal Created by the replayer from committed history. */
    public function __construct(
        private readonly WorkflowContext $context,
        public readonly int $sequence,
        private readonly ChildWorkflowIdentity $identity,
    ) {
    }

    public function identity(): ChildWorkflowIdentity
    {
        return $this->identity;
    }

    /** Wait for the child's result in this workflow execution. */
    public function await(): mixed
    {
        return $this->context->awaitChildWorkflow($this);
    }

    /** @internal */
    public function belongsTo(WorkflowContext $context): bool
    {
        return $this->context === $context;
    }

    /** @internal */
    public function assertOwner(WorkflowContext $context): void
    {
        if (!$this->belongsTo($context)) {
            throw new LogicException('A child workflow handle can only be awaited by the workflow execution that started the child.');
        }
    }
}
