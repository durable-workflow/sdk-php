<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

final class DurableOperationCancelled extends DurableWorkflowException
{
    public function __construct(
        public readonly string $selectionGroupId,
        public readonly int|string $memberKey,
        public readonly int $memberIndex,
        public readonly string $operationKind,
        public readonly string $operationIdentity,
    ) {
        parent::__construct(sprintf(
            'Selected %s operation %s was explicitly cancelled.',
            $operationKind,
            $operationIdentity,
        ));
    }
}
