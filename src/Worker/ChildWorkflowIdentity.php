<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * The committed identity of a child started with WorkflowContext::startChildWorkflow().
 *
 * Every value comes from the parent's canonical history (ChildWorkflowScheduled
 * and ChildRunStarted), never from options or a prepared command. The values are
 * opaque: keep their spelling and case.
 */
final class ChildWorkflowIdentity
{
    /**
     * @param string $callId the parent-issued child call; stable through child continue-as-new and retries
     * @param string $workflowId the child workflow instance ID; stable through its run chain
     * @param string $initialRunId the first concrete run recorded for this call; later runs do not change it
     */
    public function __construct(
        public readonly string $callId,
        public readonly string $workflowId,
        public readonly string $initialRunId,
    ) {
    }
}
