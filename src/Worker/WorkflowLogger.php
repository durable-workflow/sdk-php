<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Psr\Log\LoggerInterface;
use Psr\Log\LoggerTrait;
use Stringable;
use Throwable;

/**
 * Replay-aware workflow logger.
 *
 * Writes through the Worker's PSR-3 logger only while workflow code runs past
 * its committed history, so a line is written once per committed step instead
 * of once per workflow task. Each line carries the run's identity.
 *
 * Lines are written once the workflow task that ran the code has been
 * submitted: a replay that is superseded, speculative or refused writes
 * nothing, and code parked on a pending handle without recording a step logs
 * in the task that moves past it. It is not an outbox for business effects.
 * Keep handlers fast and non-blocking, because a slow handler stalls the
 * worker. Logger failures are swallowed and never change the commands.
 */
final class WorkflowLogger implements LoggerInterface
{
    use LoggerTrait;

    /** @param array<string, string|null> $identity */
    public function __construct(
        private readonly WorkflowContext $context,
        private readonly array $identity,
    ) {
    }

    /**
     * @param mixed $level
     * @param array<array-key, mixed> $context
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        if ($this->context->replayState() !== WorkflowReplayState::Executing) {
            return;
        }

        try {
            $this->context->collectWorkflowLog($level, (string) $message, array_merge($context, $this->identity));
        } catch (Throwable) {
            // Logging is an observation; it never fails workflow code.
        }
    }
}
