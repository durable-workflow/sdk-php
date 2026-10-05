<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;
use DateTimeImmutable;

/** @internal A monotonic budget shared by one cooperative worker request. */
final class RequestBudget
{
    private float $deadline;
    private readonly float $monotonicStart;
    private readonly float $wallStart;

    public function __construct(int $seconds, private ?float $authorityDeadline = null)
    {
        if ($seconds < 1 || $seconds > 65) {
            throw new \InvalidArgumentException('A worker request budget must be from 1 through 65 seconds.');
        }
        if ($authorityDeadline !== null && !is_finite($authorityDeadline)) {
            throw new \InvalidArgumentException('An authority deadline must be a finite monotonic timestamp.');
        }
        $this->monotonicStart = hrtime(true) / 1e9;
        $this->wallStart = (float) (new DateTimeImmutable())->format('U.u');
        $this->deadline = min($this->monotonicStart + $seconds, $authorityDeadline ?? INF);
    }

    /** Retain this budget's original clock while accepting a stricter authority ceiling. */
    public function restrictAuthorityDeadline(float $deadline): void
    {
        if (!is_finite($deadline)) {
            throw new \InvalidArgumentException('An authority deadline must be a finite monotonic timestamp.');
        }
        $this->authorityDeadline = min($deadline, $this->authorityDeadline ?? INF);
        $this->deadline = min($this->deadline, $this->authorityDeadline);
    }

    /** Use the original clock mapping, including for authority learned during I/O. */
    public function restrictWallAuthorityDeadline(DateTimeImmutable $deadline): void
    {
        $this->restrictAuthorityDeadline($this->monotonicStart + (float) $deadline->format('U.u') - $this->wallStart);
    }

    public function remainingSeconds(): int
    {
        $remaining = $this->deadline - hrtime(true) / 1e9;
        if ($remaining <= 0 || ($this->authorityDeadline !== null && $remaining < 1)) {
            throw new TransportException('The cooperative worker request time budget expired.');
        }

        // Transport timeouts use whole seconds. A final budget check rejects
        // replies that arrive during the last rounded-up fraction of a second.
        return (int) ($this->authorityDeadline === null ? ceil($remaining) : floor($remaining));
    }
}
