<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/** An update the Server refused, or whose request did not complete. */
final class UpdateFailed extends ServerException
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

    /**
     * True when the workflow's update validator refused the update before it
     * was accepted (`update_validator_rejected`). The update was never applied.
     */
    public function rejectedByValidator(): bool
    {
        return $this->reason === 'update_validator_rejected';
    }

    /**
     * True when the Server could not get a validator decision, for example
     * because no capable worker answered before the acceptance deadline. The
     * update was not accepted; retrying with the same request id is safe.
     */
    public function validationFailed(): bool
    {
        return ($this->details()['outcome'] ?? null) === 'update_validation_failed';
    }

    /**
     * Field errors the validator reported, keyed by field.
     *
     * @return array<string, list<string>>
     */
    public function validationErrors(): array
    {
        $errors = $this->details()['validation_errors'] ?? null;
        if (!is_array($errors)) {
            return [];
        }
        $normalized = [];
        foreach ($errors as $field => $messages) {
            if (!is_string($field) || !is_array($messages)) {
                continue;
            }
            $kept = array_values(array_filter($messages, static fn (mixed $message): bool => is_string($message)));
            if ($kept !== []) {
                $normalized[$field] = $kept;
            }
        }

        return $normalized;
    }
}
