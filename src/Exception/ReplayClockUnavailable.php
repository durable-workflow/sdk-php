<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** The current invocation has no authoritative consumed business-clock boundary. */
final class ReplayClockUnavailable extends DurableWorkflowException
{
}
