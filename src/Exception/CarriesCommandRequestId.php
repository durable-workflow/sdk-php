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
     * A `request_id_conflict` refusal carries `request_id`, `retained_command_id`,
     * `retained_command_type`, `retained_outcome` and the mismatched `conflict` parts.
     *
     * @return array<string, mixed>
     */
    public function details(): array
    {
        return $this->details !== null && !array_is_list($this->details) ? $this->details : [];
    }

    /** Whether the Server retained a command with a different intent under the same key. */
    public function isRequestIdConflict(): bool
    {
        return $this->status === 409 && $this->reason === 'request_id_conflict';
    }
}
