<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Fiber;
use LogicException;

final class DurableOperationHandle
{
    public function __construct(
        public readonly int|string $key,
        public readonly int $index,
        public readonly string $kind,
        public readonly string $identity,
        public readonly int $baseSequence,
        public readonly int $size,
        public readonly string $selectionGroupId,
        public readonly DeferredWorkflowOperation|ParallelWorkflowCommand $operation,
    ) {
    }

    public function await(): mixed
    {
        if (Fiber::getCurrent() === null) {
            throw new LogicException('A durable operation handle may only be awaited by an active workflow Fiber.');
        }

        return Fiber::suspend($this);
    }

    public function cancel(): void
    {
        if (Fiber::getCurrent() === null) {
            throw new LogicException('A durable operation handle may only be cancelled by an active workflow Fiber.');
        }

        Fiber::suspend(new CancelDurableOperationCommand($this));
    }
}
