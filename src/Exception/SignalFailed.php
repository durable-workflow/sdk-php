<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/** A signal the Server refused, or whose request did not complete. */
final class SignalFailed extends ServerException
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
