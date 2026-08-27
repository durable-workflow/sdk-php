<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

final class CancelDurableOperationCommand
{
    public function __construct(public readonly DurableOperationHandle $handle)
    {
    }

    /** @return array<string, mixed> */
    public function toWire(): array
    {
        return [
            'type' => 'cancel_selection_operation',
            'selection_group_id' => $this->handle->selectionGroupId,
            'member_key' => $this->handle->key,
            'member_index' => $this->handle->index,
            'member_base_sequence' => $this->handle->baseSequence,
            'member_size' => $this->handle->size,
            'operation_kind' => $this->handle->kind,
            'operation_identity' => $this->handle->identity,
        ];
    }
}
