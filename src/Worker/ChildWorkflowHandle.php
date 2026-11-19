<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Model\ExternalSignalResult;
use LogicException;

/**
 * A child workflow whose start the runtime has committed.
 *
 * Returned by WorkflowContext::startChildWorkflow() only after a fresh parent
 * turn carries the committed start, so identity() is canonical history. The
 * child keeps running; await() returns its eventual result or throws
 * ChildWorkflowFailed, following the original call through child
 * continue-as-new and retries. signal() sends a durable signal to the child
 * through WorkflowContext::signalExternalWorkflow(). The handle supports no
 * query, update, cancellation or termination.
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

    /**
     * Signal the child's current run from this workflow and return the recorded outcome.
     *
     * The same durable command as WorkflowContext::signalExternalWorkflow()
     * for the child's workflow ID; it follows the child through
     * continue-as-new. Requires a runtime that advertises
     * signal_external_workflow.
     *
     * @param list<mixed> $arguments
     * @throws \DurableWorkflow\Exception\InvalidSignalName before any command when the Server would refuse the name
     */
    public function signal(string $signalName, array $arguments = []): ExternalSignalResult
    {
        return $this->context->signalExternalWorkflow($this->identity->workflowId, $signalName, $arguments);
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
