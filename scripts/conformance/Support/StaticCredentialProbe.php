<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Auth\TokenAuthentication;
use DurableWorkflow\Bridge\Laravel\ProcessCredentialResolver;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\Transport;
use ReflectionProperty;

require_once __DIR__.'/StaticCredentialProbeFailure.php';

/** Construction/header inspection only; no client request or worker loop. */
final class StaticCredentialProbe
{
    public const ENDPOINT = 'https://fixture.invalid';
    public const ENVIRONMENT = ['shared' => 'DURABLE_WORKFLOW_TOKEN', 'control' => 'DURABLE_WORKFLOW_CLIENT_TOKEN',
        'worker' => 'DURABLE_WORKFLOW_WORKER_TOKEN'];
    private const CONFIGURATION = ['shared' => 'token', 'control' => 'control_token', 'worker' => 'worker_token'];

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $process
     * @return array<string, string>
     */
    public static function headers(string $target, array $options, string $plane, Transport $transport, array $process = []): array
    {
        if (!in_array($plane, ['control', 'worker'], true)
            || array_diff(array_keys($options), ['shared', 'control', 'worker']) !== []) {
            throw new StaticCredentialProbeFailure('Unsupported credential fixture role.');
        }
        if ($target === 'TokenAuthentication') {
            $authentication = new TokenAuthentication(token: $options['shared'] ?? null,
                controlToken: $options['control'] ?? null, workerToken: $options['worker'] ?? null);
        } else {
            $client = match ($target) {
                'Client' => new Client(self::ENDPOINT, transport: $transport,
                    token: $options['shared'] ?? null, controlToken: $options['control'] ?? null, workerToken: $options['worker'] ?? null),
                'ServiceConfiguration', 'ServiceConfigurationAlias' =>
                    self::configured($target, $options, $plane, $transport),
                'ProcessCredentialResolver' => self::process($options, $process, $plane, $transport),
                default => throw new StaticCredentialProbeFailure('Unsupported credential target.'),
            };
            $authentication = self::authentication($client);
        }
        return $authentication?->headers($plane === 'worker') ?? [];
    }

    public static function authentication(Client $client): ?Authentication
    {
        $authentication = (new ReflectionProperty(Client::class, 'authentication'))->getValue($client);
        if ($authentication !== null && !$authentication instanceof Authentication) {
            throw new StaticCredentialProbeFailure('Unsupported constructed authentication.');
        }
        return $authentication;
    }

    /**
     * @param array<string, ?string> $options
     * @return array<string, ?string>
     */
    public static function constructor(array $options): array
    {
        $configuration = new ServiceConfiguration(self::ENDPOINT, 'fixture', 'fixture', [],
            $options['shared'] ?? null, $options['control'] ?? null, $options['worker'] ?? null);
        return ['token' => $configuration->token, 'controlToken' => $configuration->controlToken,
            'workerToken' => $configuration->workerToken];
    }

    /**
     * @template T
     * @param array<string, string> $values
     * @param callable(): T $build
     * @return T
     */
    public static function environment(array $values, callable $build): mixed
    {
        $names = [...array_values(self::ENVIRONMENT), 'DURABLE_WORKFLOW_PROCESS_ROLE', 'DURABLE_WORKFLOW_PROCESS_TOKEN'];
        if (array_diff(array_keys($values), $names) !== []) {
            throw new StaticCredentialProbeFailure('Unsupported fixture environment setting.');
        }
        $previous = [];
        foreach ($names as $name) {
            $previous[$name] = getenv($name);
        }
        try {
            foreach ($names as $name) {
                self::put($name);
            }
            foreach ($values as $name => $value) {
                self::put($name.'='.$value);
                if (getenv($name) !== $value) {
                    throw new StaticCredentialProbeFailure('Fixture environment bytes differ.');
                }
            }
            return $build();
        } finally {
            foreach ($previous as $name => $value) {
                self::put($value === false ? $name : $name.'='.$value);
            }
        }
    }

    /** @param array<string, ?string> $options */
    private static function configured(string $target, array $options, string $plane, Transport $transport): Client
    {
        $credentials = [];
        foreach ($options as $role => $value) {
            $credentials[self::CONFIGURATION[$role]] = $value;
        }
        $configuration = ServiceConfiguration::fromArray(['endpoint' => self::ENDPOINT,
            'namespace' => 'fixture', 'task_queue' => 'fixture', 'handlers' => [], 'credentials' => $credentials]);
        if ($target === 'ServiceConfigurationAlias') {
            if ($plane !== 'control') {
                throw new StaticCredentialProbeFailure('The configuration alias is a control client.');
            }
            return $configuration->client($transport);
        }
        return $plane === 'worker' ? $configuration->workerClient($transport) : $configuration->controlClient($transport);
    }

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $values
     */
    private static function process(array $options, array $values, string $plane, Transport $transport): Client
    {
        $values = self::processValues($options, $values);
        $configuration = ServiceConfiguration::fromArray(['endpoint' => self::ENDPOINT, 'task_queue' => 'fixture']);
        return self::environment($values, static fn (): Client => $plane === 'worker'
            ? ProcessCredentialResolver::workerClient($configuration, $transport)
            : ProcessCredentialResolver::applicationClient($configuration, $transport));
    }

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $values
     * @return array<string, string>
     */
    public static function processValues(array $options, array $values = []): array
    {
        foreach ($options as $role => $value) {
            if (!isset(self::ENVIRONMENT[$role])) {
                throw new StaticCredentialProbeFailure('Unsupported fixture credential role.');
            }
            if ($value !== null) {
                $values[self::ENVIRONMENT[$role]] = $value;
            }
        }
        return $values;
    }

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $process
     */
    public static function workerRole(array $options, array $process = []): string
    {
        return self::environment(self::processValues($options, $process),
            static fn (): string => ProcessCredentialResolver::workerCredentialRole());
    }

    private static function put(string $setting): void
    {
        if (!putenv($setting)) {
            throw new StaticCredentialProbeFailure('Fixture environment update failed.');
        }
    }
}
