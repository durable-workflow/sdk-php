<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

use DurableWorkflow\Exception\DurableWorkflowException;

/** @internal The pinned shared SDK metrics registry is missing or malformed. */
final class InvalidMetricsRegistry extends DurableWorkflowException
{
}
