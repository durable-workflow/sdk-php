<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;

/** @internal Preserve an ordinary poll refusal across publication of the current completion. */
final class WorkflowPollRetry
{
    public function __construct(
        public readonly ServerException $error,
        public readonly ?CarriedPoll $carried,
    ) {
    }
}
