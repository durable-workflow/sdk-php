<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** Replay-safe receiver for repeated, ordered workflow input. */
final class MessageStream
{
    public const MAX_BATCH_SIZE = 100;

    public function __construct(
        private readonly WorkflowContext $context,
        public readonly string $name,
    ) {
    }

    /**
     * Wait for one message, then return a bounded currently-available batch.
     *
     * @return list<MessageStreamMessage>
     */
    public function receive(int $maxItems = 1): array
    {
        if ($maxItems < 1 || $maxItems > self::MAX_BATCH_SIZE) {
            throw new \InvalidArgumentException(sprintf(
                'Message stream maxItems must be between 1 and %d.',
                self::MAX_BATCH_SIZE,
            ));
        }

        do {
            $cursor = $this->context->messageStreamCursor($this->name);
            $this->context->recordMessageStreamWait($this->name, $cursor);
            $this->context->waitCondition(
                fn (): bool => $this->context->hasPendingMessageStreamMessages($this->name),
                sprintf('message-stream:%s:%d', $this->name, $cursor),
            );
        } while (!$this->context->hasPendingMessageStreamMessages($this->name));

        return $this->context->consumeMessageStreamMessages($this->name, $maxItems);
    }

    public function receiveOne(): MessageStreamMessage
    {
        return $this->receive(1)[0];
    }
}
