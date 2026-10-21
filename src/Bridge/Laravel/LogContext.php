<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use Illuminate\Log\Logger;

/** Laravel logging tap; configure once on each application channel that needs correlation. */
final class LogContext
{
    public function __invoke(Logger $logger): void
    {
        $target = $logger->getLogger();
        if (method_exists($target, 'pushProcessor')) {
            $target->pushProcessor(new \DurableWorkflow\Telemetry\LogContext());
        }
    }
}
