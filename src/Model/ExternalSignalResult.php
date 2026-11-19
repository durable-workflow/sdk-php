<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/**
 * The recorded result of WorkflowContext::signalExternalWorkflow().
 *
 * Every value comes from the sender's history (ExternalSignalDelivered or
 * ExternalSignalFailed), so replay returns the same result without sending
 * the signal again. A failed outcome is a value, not an exception: workflow
 * code decides what to do with it.
 */
final class ExternalSignalResult
{
    /**
     * @param string $targetWorkflowId the target workflow instance ID
     * @param string|null $targetRunId the run that received the signal; for a failure, the run the target resolved to, or null
     * @param string|null $reason null when delivered; otherwise for example instance_not_found, run_not_found,
     *                            run_not_active, historical_run, unknown_signal, invalid_signal_arguments,
     *                            structural_limit_exceeded, runtime_reserved_signal, self_target or request_id_conflict
     * @param string|null $targetCommandId the command the target recorded (a delivered or rejected signal)
     * @param string|null $targetSignalId the target's signal record of a delivered signal
     * @param bool $retained true when the command key had already been delivered with the same intent
     */
    public function __construct(
        public readonly ExternalSignalOutcome $outcome,
        public readonly string $targetWorkflowId,
        public readonly string $signalName,
        public readonly ?string $targetRunId = null,
        public readonly ?string $reason = null,
        public readonly ?string $message = null,
        public readonly ?string $targetCommandId = null,
        public readonly ?string $targetSignalId = null,
        public readonly ?string $requestId = null,
        public readonly bool $retained = false,
    ) {
    }

    public function delivered(): bool
    {
        return $this->outcome === ExternalSignalOutcome::Delivered;
    }
}
