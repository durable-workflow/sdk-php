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
 * This is a process logger, not an exactly-once outbox: a workflow task that
 * fails before it commits runs its new code, and its log lines, again. Keep
 * handlers fast and non-blocking, because a slow handler stalls the worker.
 * Logger failures are swallowed and never change the workflow's commands.
 */
final class WorkflowLogger implements LoggerInterface
{
    use LoggerTrait;

    /** @param array<string, string|null> $identity */
    public function __construct(
        private readonly WorkflowContext $context,
        private readonly LoggerInterface $logger,
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
            $this->logger->log($level, $message, array_merge($context, $this->identity));
        } catch (Throwable) {
            // Logging is an observation; it never fails workflow code.
        }
    }
}
