<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * The caller command key and Server refusal carried by signal and update failures.
 *
 * The key is chosen before the request is sent, so it is available after a
 * transport failure whose outcome is unknown. Retrying with the same key and
 * intent returns the retained receipt instead of admitting a second command.
 */
trait CarriesCommandRequestId
{
    private readonly ?string $requestId;

    /** The `request_id` sent with the command, or null when it was not recorded. */
    public function requestId(): ?string
    {
        return $this->requestId;
    }

    /** The Server refusal reason, or null when no protocol envelope arrived. */
    public function reason(): ?string
    {
        return $this->reason;
    }

    /**
     * The Server response fields, or an empty array when no envelope arrived.
     *
     * A `request_id_conflict` refusal carries `outcome` (`rejected_request_id_conflict`),
     * `retained_command_id`, `retained_outcome` and the mismatched `conflict` parts. Read
     * them with conflict() and retainedCommandId().
     *
     * @return array<string, mixed>
     */
    public function details(): array
    {
        return $this->details !== null && !array_is_list($this->details) ? $this->details : [];
    }

    /**
     * Whether the Server retained a command with a different intent under the same key.
     *
     * Nothing was written and the key keeps its original receipt. Don't retry this unchanged:
     * send the original input to recover the receipt, or use a new key for new input.
     */
    public function isRequestIdConflict(): bool
    {
        return $this->status === 409 && $this->reason === 'request_id_conflict';
    }

    /**
     * The parts of the refused command that differ from the retained one, for a
     * `request_id_conflict`: `name`, `arguments` or `target_or_arguments`. Otherwise empty.
     *
     * @return list<string>
     */
    public function conflict(): array
    {
        $parts = $this->isRequestIdConflict() ? ($this->details()['conflict'] ?? null) : null;

        return is_array($parts) ? array_values(array_filter($parts, is_string(...))) : [];
    }

    /** The command id retained under the key, for a `request_id_conflict`. Otherwise null. */
    public function retainedCommandId(): ?string
    {
        $id = $this->isRequestIdConflict() ? ($this->details()['retained_command_id'] ?? null) : null;

        return is_string($id) && $id !== '' ? $id : null;
    }
}
