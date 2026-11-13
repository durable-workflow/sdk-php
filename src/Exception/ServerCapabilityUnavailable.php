<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/**
 * The connected Server has no route for an additive capability the call needs.
 *
 * Raised with status 501 and reason `backend_capability_unavailable` when an older Server answers
 * a capability's route with a plain 404. Nothing was read or changed. `capability` names the
 * cluster info capability to check before offering the call, and `details` carries the Server's
 * original status and answer. The original error is kept as the previous exception.
 */
final class ServerCapabilityUnavailable extends ServerException
{
    /** @param array<string, mixed> $details */
    public function __construct(
        public readonly string $capability,
        string $message,
        array $details = [],
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, 501, 'backend_capability_unavailable', ['capability' => $capability] + $details, $previous);
    }

    /**
     * A plain route 404 means the Server predates the capability. A 404 with any other reason,
     * such as `namespace_not_found`, is the Server's own answer and is kept as it is.
     */
    public static function isMissingRoute(ServerException $exception): bool
    {
        return $exception->status === 404 && in_array($exception->reason, [null, 'not_found'], true);
    }

    public static function fromMissingRoute(ServerException $exception, string $capability, string $message): self
    {
        return new self($capability, $message, [
            'server_status' => $exception->status,
            'server_reason' => $exception->reason,
            'server_response' => $exception->details,
        ], $exception);
    }
}
