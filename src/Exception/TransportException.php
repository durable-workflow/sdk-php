<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

class TransportException extends DurableWorkflowException
{
    /** @param array<string, mixed>|list<mixed>|null $response */
    public function __construct(
        string $message,
        public readonly ?int $status = null,
        public readonly ?array $response = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $status ?? 0, $previous);
    }

    /** @param array<string, mixed>|list<mixed>|null $response */
    public static function fromResponse(int $status, ?array $response, string $rawBody): self
    {
        $message = is_array($response)
            ? (string) ($response['message'] ?? $response['error'] ?? "Server returned HTTP {$status}.")
            : ($rawBody !== '' ? $rawBody : "Server returned HTTP {$status}.");

        return new self($message, $status, $response);
    }
}
