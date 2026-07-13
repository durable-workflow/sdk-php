<?php

declare(strict_types=1);

namespace DurableWorkflow\Auth;

use InvalidArgumentException;

/** Bearer authentication with optional independent control and worker tokens. */
final class TokenAuthentication implements Authentication
{
    public function __construct(
        private readonly ?string $token = null,
        private readonly ?string $controlToken = null,
        private readonly ?string $workerToken = null,
    ) {
        if ($this->resolvedToken(false) === null || $this->resolvedToken(true) === null) {
            throw new InvalidArgumentException('At least one non-empty authentication token is required.');
        }
    }

    /** @return array<string, string> */
    public function headers(bool $workerRequest): array
    {
        return ['Authorization' => 'Bearer '.(string) $this->resolvedToken($workerRequest)];
    }

    private function resolvedToken(bool $workerRequest): ?string
    {
        $candidates = $workerRequest
            ? [$this->workerToken, $this->token, $this->controlToken]
            : [$this->controlToken, $this->token, $this->workerToken];

        foreach ($candidates as $candidate) {
            if ($candidate !== null && trim($candidate) !== '') {
                return trim($candidate);
            }
        }

        return null;
    }
}
