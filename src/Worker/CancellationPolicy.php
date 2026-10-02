<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** What an awaiting workflow does when cancellation reaches its operation. */
enum CancellationPolicy: string
{
    /** Request cancellation and continue without waiting. This is the historical Activity default. */
    case TryCancel = 'try_cancel';

    /** Wait for recorded child termination or the original Activity attempt's stop acknowledgment. */
    case WaitCancellationCompleted = 'wait_cancellation_completed';

    /** Leave work independent. This is the historical child default. Remote Activities require a finite total timeout. */
    case Abandon = 'abandon';
}
