<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** What happens to an open child after its parent closes. */
enum ParentClosePolicy: string
{
    case Abandon = 'abandon';

    /** Legacy terminal cancellation, without cooperative cleanup. */
    case RequestCancel = 'request_cancel';

    /** Request cooperative cleanup with the original lineage and deadline. */
    case RequestCancellation = 'request_cancellation';

    case Terminate = 'terminate';
}
