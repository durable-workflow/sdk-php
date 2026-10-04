<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Fence one callback while its hosting claim continues supervising siblings. */
final class ScopedActivityCancellationObserved extends WorkflowClaimAborted
{
    public function __construct(
        public readonly ScopedCancellationContext $request,
        public readonly ?string $historyRefreshPageToken = null,
    )
    {
        parent::__construct('The admitted local callback observed scoped cancellation.');
    }
}
