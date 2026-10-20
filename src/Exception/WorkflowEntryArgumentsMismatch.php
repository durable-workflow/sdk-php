<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** Entry input cannot bind, before application workflow code starts. */
final class WorkflowEntryArgumentsMismatch extends DurableWorkflowException
{
    public readonly string $reason;

    public function __construct(
        public readonly string $workflowType,
        public readonly string $bindingReason,
        public readonly ?string $parameter = null,
        public readonly ?string $expectedType = null,
        public readonly ?string $actualType = null,
    ) {
        $this->reason = 'workflow_entry_arguments_mismatch';
        parent::__construct('replay failed: '.$this->reason.' for '.$workflowType
            .' ('.$bindingReason.($parameter === null ? '' : ', parameter '.$parameter).').');
    }
}
