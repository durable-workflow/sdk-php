<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An update the Server accepted that had not completed when the wait ended.
 *
 * It has no result yet. Call Client::updateWorkflow() again with the same request ID to keep
 * waiting: the Server returns the retained update instead of admitting a second one. On a Server
 * that advertises update_outcome_reads, Client::updateOutcome() reads it by update ID instead.
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
        bool $fromOutcomeRead = false,
    ) {
        parent::__construct($fromOutcomeRead && $updateId !== null
            ? sprintf(
                'Update %s on workflow %s is still %s; it has no result yet. Call updateOutcome() again with update id "%s" '
                .'to keep waiting. Reading never admits another update.',
                $updateName !== '' ? "{$updateName} ({$updateId})" : $updateId,
                $workflowId,
                $updateStatus ?? 'pending',
                $updateId,
            )
            : sprintf(
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
