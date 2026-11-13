<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An update the Server accepted that had not completed when the wait ended.
 *
 * An admission-time Client::updateWorkflow() wait retains the original request ID. Calling it
 * again with that same ID re-waits on the retained update. On a Server that advertises
 * update_outcome_reads, Client::updateOutcome() reads the outcome by its stable update ID.
 *
 * A pending outcome read also throws this class. Its original request key may be unavailable:
 * requestId is then the compatible empty-string unknown representation. Keep reading by updateId;
 * an empty, cached or internal hashed token must not be substituted for the original caller key.
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
