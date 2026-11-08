<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * @internal Leave the current workflow task claim without completing or
 * failing it. Either the Server parked this workflow and released the claim,
 * or this task's Server cannot accept a command the replay requires (#269: an
 * anchored condition-wait reopen without condition_wait_occurrence_deadline),
 * so the claim is left to its lease or another ordinary authority transition.
 */
final class WorkflowClaimDeferred extends WorkflowClaimAborted
{
}
