<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

/** A fixed, value-free fixture failure; native exception text is never a report. */
final class StaticCredentialProbeFailure extends RuntimeException
{
}
