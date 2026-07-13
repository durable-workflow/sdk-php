<?php

declare(strict_types=1);

namespace DurableWorkflow\Auth;

/** Authentication adapter for API keys, signed headers, and gateway identities. */
final class StaticHeadersAuthentication implements Authentication
{
    /** @param array<string, string> $headers */
    public function __construct(private readonly array $headers)
    {
    }

    /** @return array<string, string> */
    public function headers(bool $workerRequest): array
    {
        return $this->headers;
    }
}
