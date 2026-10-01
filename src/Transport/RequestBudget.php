<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;

/** @internal A monotonic budget shared by one cooperative worker request. */
final class RequestBudget
{
    private readonly float $deadline;

    public function __construct(int $seconds)
    {
        if ($seconds < 1 || $seconds > 65) {
            throw new \InvalidArgumentException('A worker request budget must be from 1 through 65 seconds.');
        }
        $this->deadline = hrtime(true) / 1e9 + $seconds;
    }

    public function remainingSeconds(): int
    {
        $remaining = $this->deadline - hrtime(true) / 1e9;
        if ($remaining <= 0) {
            throw new TransportException('The cooperative worker request time budget expired.');
        }

        // Transport timeouts use whole seconds. A final budget check rejects
        // replies that arrive during the last rounded-up fraction of a second.
        return (int) ceil($remaining);
    }
}
