<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An update validator tried to schedule durable work.
 *
 * Validators run before an update is accepted and are read-only. The worker
 * refuses the attempt and rejects the update; nothing is scheduled.
 */
final class UpdateValidatorCommandRefused extends DurableWorkflowException
{
    public function __construct(
        public readonly string $workflowType,
        public readonly string $updateName,
        public readonly string $operation,
    ) {
        parent::__construct(sprintf(
            'Update validator for %s.%s cannot schedule durable work (%s); update validators are read-only.',
            $workflowType,
            $updateName,
            $operation,
        ));
    }
}
