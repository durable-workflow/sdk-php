<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use RuntimeException;

/** @internal Stops local Fiber teardown, not a durable workflow failure. */
final class WorkflowFiberDiscarded extends RuntimeException
{
}
