<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Stop a local report until the worker reloads canonical cancellation history. */
final class CooperativeCancellationObserved extends WorkflowClaimAborted
{
}
