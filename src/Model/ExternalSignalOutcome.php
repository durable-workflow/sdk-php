<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** The recorded outcome of a signal sent from workflow code to another workflow. */
enum ExternalSignalOutcome: string
{
    /** The target run admitted the signal. */
    case Delivered = 'delivered';

    /** No instance with that ID, or no run with the requested run ID. */
    case TargetNotFound = 'target_not_found';

    /** The target run has closed, or the requested run was replaced by continue-as-new. */
    case TargetClosed = 'target_closed';

    /** The target refused the signal; the reason says why. */
    case Rejected = 'rejected';
}
