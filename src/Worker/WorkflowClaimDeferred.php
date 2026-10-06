<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Server parked this workflow and released its current task claim. */
final class WorkflowClaimDeferred extends WorkflowClaimAborted
{
}
