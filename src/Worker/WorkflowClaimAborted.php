<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Abandon an unsafe task claim without recording a workflow-code failure. */
class WorkflowClaimAborted extends \RuntimeException
{
}
