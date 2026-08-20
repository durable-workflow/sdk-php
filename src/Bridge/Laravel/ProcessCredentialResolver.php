<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\Transport;
use InvalidArgumentException;

/** Resolves role credentials only when a Laravel client is constructed. */
final class ProcessCredentialResolver
{
    private const SHARED_TOKEN = 'DURABLE_WORKFLOW_TOKEN';
    private const CLIENT_TOKEN = 'DURABLE_WORKFLOW_CLIENT_TOKEN';
    private const WORKER_TOKEN = 'DURABLE_WORKFLOW_WORKER_TOKEN';

    public static function applicationClient(
        ServiceConfiguration $configuration,
        ?Transport $transport = null,
    ): Client {
        [$token, $clientToken, $workerToken] = self::credentials();
        self::assertAuthenticationMode($token, $clientToken, $workerToken);
        if ($token === null && $clientToken === null && $workerToken !== null) {
            throw new InvalidArgumentException(
                'A client credential is required for the Laravel application client. Configure the self-hosted shared token or scoped client token.',
            );
        }

        return new Client(
            $configuration->endpoint,
            namespace: $configuration->namespace,
            transport: $transport,
            token: $token,
            controlToken: $clientToken,
        );
    }

    public static function workerClient(
        ServiceConfiguration $configuration,
        ?Transport $transport = null,
    ): Client {
        [$token, $clientToken, $workerToken] = self::credentials();
        self::assertAuthenticationMode($token, $clientToken, $workerToken);
        if ($token === null && $workerToken === null && $clientToken !== null) {
            throw new InvalidArgumentException(
                'A worker credential is required for the Laravel worker client. Configure the self-hosted shared token or scoped worker token.',
            );
        }

        return new Client(
            $configuration->endpoint,
            namespace: $configuration->namespace,
            transport: $transport,
            token: $token,
            workerToken: $workerToken,
        );
    }

    public static function workerCredentialRole(): string
    {
        [$token, $clientToken, $workerToken] = self::credentials();
        self::assertAuthenticationMode($token, $clientToken, $workerToken);

        return $token === null ? 'worker' : 'shared';
    }

    /** @return array{?string, ?string, ?string} */
    private static function credentials(): array
    {
        return [
            self::credential(self::SHARED_TOKEN),
            self::credential(self::CLIENT_TOKEN),
            self::credential(self::WORKER_TOKEN),
        ];
    }

    private static function credential(string $name): ?string
    {
        $value = getenv($name);
        if ($value === false || $value === '') {
            return null;
        }

        return $value;
    }

    private static function assertAuthenticationMode(
        ?string $token,
        ?string $clientToken,
        ?string $workerToken,
    ): void {
        if ($token !== null && ($clientToken !== null || $workerToken !== null)) {
            throw new InvalidArgumentException(
                'Configure either the self-hosted shared Durable Workflow token or scoped client and worker tokens, not both.',
            );
        }
    }

    private function __construct()
    {
    }
}
