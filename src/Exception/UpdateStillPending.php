<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An update the Server accepted that had not completed when the wait ended.
 *
 * It has no result yet. Call Client::updateWorkflow() again with the same request ID to keep
 * waiting: the Server returns the retained update instead of admitting a second one.
 */
final class UpdateStillPending extends DurableWorkflowException
{
    public function __construct(
        public readonly string $workflowId,
        public readonly ?string $runId,
        public readonly string $updateName,
        public readonly ?string $updateId,
        public readonly string $requestId,
        public readonly ?string $updateStatus,
    ) {
        parent::__construct(sprintf(
            'Update %s%s on workflow %s is still %s; it has no result yet. Call updateWorkflow() again with requestId "%s" '
            .'to keep waiting. The Server returns the retained update instead of admitting a second one.',
            $updateName,
            $updateId === null ? '' : " ({$updateId})",
            $workflowId,
            $updateStatus ?? 'pending',
            $requestId,
        ));
    }
}
