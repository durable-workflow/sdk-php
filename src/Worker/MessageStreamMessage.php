<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** One server-ordered message consumed from a durable named stream. */
final class MessageStreamMessage
{
    /** @param list<mixed> $arguments */
    public function __construct(
        public readonly string $streamName,
        public readonly string $messageId,
        public readonly int $position,
        public readonly array $arguments,
    ) {
    }
}
