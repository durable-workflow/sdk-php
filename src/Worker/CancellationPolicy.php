<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** What an awaiting workflow does when cancellation reaches its child call. */
enum CancellationPolicy: string
{
    /** Request child cleanup and deliver cancellation without waiting for it. */
    case TryCancel = 'try_cancel';

    /** Deliver cancellation after the child reaches a recorded terminal outcome. */
    case WaitCancellationCompleted = 'wait_cancellation_completed';

    /** Leave the child running independently. This preserves the historical default. */
    case Abandon = 'abandon';
}
