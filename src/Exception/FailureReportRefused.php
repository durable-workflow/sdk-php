<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** A local reporting refusal, separate from the application's original failure. */
final class FailureReportRefused extends TransportException
{
    public function __construct(int $limit, ?\Throwable $previous = null)
    {
        parent::__construct('The required failure-report identity and metadata cannot fit its configured encoded request budget.',
            0, ['reason' => 'failure_report_refused', 'limit_bytes' => $limit], $previous);
    }
}
