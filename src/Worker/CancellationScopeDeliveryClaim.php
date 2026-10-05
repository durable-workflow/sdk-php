<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\RequestBudget;
use InvalidArgumentException;

/** @internal Unfrozen scalar coordination. The Worker does not enable scope execution. */
final class CancellationScopeDeliveryClaim
{
    private readonly RequestBudget $budget;
    private ?CancellationScopeDeliveryReceipt $preparation = null;
    private ?CancellationScopeDeliveryReceipt $delivery = null;

    public function __construct(
        private readonly Client $client,
        private readonly string $taskId,
        private readonly string $leaseOwner,
        private readonly int $attempt,
        private readonly CancellationScopeDeliveryIntent $intent,
        ?RequestBudget $budget = null,
    ) {
        foreach ([$taskId, $leaseOwner] as $identity) {
            if (trim($identity) === '' || strlen($identity) > 255 || preg_match('//u', $identity) !== 1) {
                throw new InvalidArgumentException('Scope coordination requires its original bounded claim identity.');
            }
        }
        if ($attempt < 1) {
            throw new InvalidArgumentException('Scope coordination requires its original positive claim attempt.');
        }
        $this->budget = $budget ?? new RequestBudget(5);
        $this->restrict($intent->authorityDeadline ?? $intent->context->deadline());
    }

    public function prepare(): CancellationScopeDeliveryReceipt
    {
        $this->budget->remainingSeconds();
        if ($this->preparation !== null) {
            return $this->preparation;
        }
        $prepared = $this->boundary('prepare');
        if ($prepared->context->toArray() !== $this->intent->context->toArray()
            || $prepared->boundary != $this->intent->boundary
            || ($this->intent->preparationHistoryEventId !== null
                && ($prepared->preparationHistoryEventId !== $this->intent->preparationHistoryEventId
                    || $prepared->authorityDeadline != $this->intent->authorityDeadline))) {
            throw new WorkflowClaimAborted('Scope preparation changes the selected original request or authority.');
        }
        $this->restrict($prepared->authorityDeadline);
        $this->budget->remainingSeconds();

        return $this->preparation = $prepared;
    }

    /** The prepared history must be replayed before dispatching its frozen operations. */
    public function deliver(CancellationScopeDeliveryIntent $replayed): CancellationScopeDeliveryReceipt
    {
        $prepared = $this->preparation;
        if ($prepared === null || $replayed->context->toArray() !== $prepared->context->toArray()
            || $replayed->boundary != $prepared->boundary
            || $replayed->preparationHistoryEventId !== $prepared->preparationHistoryEventId
            || $replayed->authorityDeadline != $prepared->authorityDeadline) {
            throw new WorkflowClaimAborted('Scope delivery requires replay of its original proved preparation.');
        }
        $this->budget->remainingSeconds();
        if ($this->delivery !== null) {
            return $this->delivery;
        }

        return $this->delivery = $this->boundary('deliver', $prepared);
    }

    private function boundary(string $phase, ?CancellationScopeDeliveryReceipt $prepared = null): CancellationScopeDeliveryReceipt
    {
        $context = $this->intent->context;

        return $this->client->cancellationScopeBoundaryOnClaim($this->taskId, $context->workflowRunId,
            $context->workflowInstanceId, $this->leaseOwner, $this->attempt, $context->scopeId,
            $this->intent->boundary, $phase, $this->budget, $prepared, enforceAuthorityDeadline: true);
    }

    private function restrict(DateTimeImmutable $deadline): void
    {
        $this->budget->restrictWallAuthorityDeadline($deadline);
    }
}
