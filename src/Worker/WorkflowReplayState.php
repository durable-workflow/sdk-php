<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/**
 * Where workflow code is running relative to its committed history.
 *
 * A read-only observation for logging and diagnostics. Never choose durable
 * commands from it.
 */
enum WorkflowReplayState: string
{
    /** Re-running code whose commands committed history already records. */
    case Replaying = 'replaying';

    /** Running past committed history. A task that fails before it commits runs this code again. */
    case Executing = 'executing';

    /** Outside this workflow invocation, or the replayer doesn't track its position. */
    case Unavailable = 'unavailable';
}
