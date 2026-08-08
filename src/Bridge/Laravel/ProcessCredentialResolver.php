<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\Transport;
use Illuminate\Support\Env;
use InvalidArgumentException;

/** Resolves role credentials only when a Laravel client is constructed. */
final class ProcessCredentialResolver
{
    private const SHARED_TOKEN = 'DURABLE_WORKFLOW_TOKEN';
    private const CONTROL_TOKEN = 'DURABLE_WORKFLOW_CONTROL_TOKEN';
    private const WORKER_TOKEN = 'DURABLE_WORKFLOW_WORKER_TOKEN';

    public static function controlClient(
        ServiceConfiguration $configuration,
        ?Transport $transport = null,
    ): Client {
        [$token, $controlToken, $workerToken] = self::credentials();
        self::assertAuthenticationMode($token, $controlToken, $workerToken);
        if ($token === null && $controlToken === null && $workerToken !== null) {
            throw new InvalidArgumentException(
                'A control credential is required for the application client. Configure the shared token or scoped control token.',
            );
        }

        return new Client(
            $configuration->endpoint,
            namespace: $configuration->namespace,
            transport: $transport,
            token: $token,
            controlToken: $controlToken,
        );
    }

    public static function workerClient(
        ServiceConfiguration $configuration,
        ?Transport $transport = null,
    ): Client {
        [$token, $controlToken, $workerToken] = self::credentials();
        self::assertAuthenticationMode($token, $controlToken, $workerToken);
        if ($token === null && $workerToken === null && $controlToken !== null) {
            throw new InvalidArgumentException(
                'A worker credential is required for the worker client. Configure the shared token or scoped worker token.',
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

    /** @return array{?string, ?string, ?string} */
    private static function credentials(): array
    {
        return [
            self::credential(self::SHARED_TOKEN),
            self::credential(self::CONTROL_TOKEN),
            self::credential(self::WORKER_TOKEN),
        ];
    }

    private static function credential(string $name): ?string
    {
        $value = Env::get($name);
        if ($value === null || $value === '') {
            return null;
        }
        if (!is_string($value)) {
            throw new InvalidArgumentException("Durable Workflow process credential {$name} must be a string.");
        }

        return $value;
    }

    private static function assertAuthenticationMode(
        ?string $token,
        ?string $controlToken,
        ?string $workerToken,
    ): void {
        if ($token !== null && ($controlToken !== null || $workerToken !== null)) {
            throw new InvalidArgumentException(
                'Configure either the shared Durable Workflow token or scoped control and worker tokens, not both.',
            );
        }
    }

    private function __construct()
    {
    }
}
