<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/**
 * A signal-with-start intake the Server refused, or whose request did not complete.
 *
 * Every refusal is atomic: no run was started, no signal was recorded and no command key was
 * reserved. reason() names the cause, for example `run_not_active` (the current run is closed
 * and the closed run policy is `reject`), `instance_already_started` (an open run under the
 * `fail` duplicate policy), `workflow_type_conflict`, `task_queue_conflict`,
 * `workflow_id_reserved_in_namespace`, `unknown_signal`, `invalid_signal_arguments` or
 * `request_id_conflict`.
 */
final class SignalWithStartFailed extends ServerException
{
    use CarriesCommandRequestId;

    /** @param array<string, mixed>|list<mixed>|null $details */
    public function __construct(
        string $message,
        int $status,
        ?string $reason = null,
        ?array $details = null,
        ?Throwable $previous = null,
        ?string $requestId = null,
    ) {
        parent::__construct($message, $status, $reason, $details, $previous);
        $this->requestId = $requestId;
    }
}
