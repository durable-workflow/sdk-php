<?php

declare(strict_types=1);

namespace DurableWorkflow\Testing;

use RuntimeException;

/** A framework-independent Durable Workflow test assertion failed. */
final class AssertionFailed extends RuntimeException
{
}
