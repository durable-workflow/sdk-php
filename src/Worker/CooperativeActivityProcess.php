<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal One owner socket and relay in a bounded concurrent callback group. */
final class CooperativeActivityProcess
{
    public string $buffer = '';
    public ?int $callbackPid = null;
    public float $nextCheck = 0.0;

    /** @param resource $socket */
    public function __construct(public readonly mixed $socket, public readonly int $relayPid)
    {
    }
}
