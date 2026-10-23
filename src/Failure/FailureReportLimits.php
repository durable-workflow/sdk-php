<?php

declare(strict_types=1);

namespace DurableWorkflow\Failure;

/** SDK failure-report budgets; these do not change the Server's request limit. */
final class FailureReportLimits
{
    public const MESSAGE_BYTES = 63488;
    public const STACK_BYTES = 131072;
    public const METADATA_BYTES = 65536;
    public const REQUEST_BYTES = 1048576;

    /** Applications may narrow the complete-report budget for their transport. */
    public function __construct(public readonly int $requestBytes = self::REQUEST_BYTES)
    {
        if ($requestBytes < 1 || $requestBytes > self::REQUEST_BYTES) {
            throw new \InvalidArgumentException('Failure-report request bytes must be between 1 and 1048576.');
        }
    }
}
