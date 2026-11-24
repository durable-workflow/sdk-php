<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal The Server refused continuation admission while the task still owns its lease. */
final class PromotedBuildUnavailable extends WorkflowClaimAborted
{
    public const REASON = 'promoted_build_unavailable';
}
