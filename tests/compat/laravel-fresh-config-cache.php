<?php

declare(strict_types=1);

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\ProcessCredentialResolver;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Foundation\Application;
use Illuminate\Support\Env;

const LARAVEL_FRESH_CLIENT_TOKEN = 'fresh-cache-client-secret';
const LARAVEL_FRESH_WORKER_TOKEN = 'fresh-cache-worker-secret';

final class LaravelFreshClientOperationWorkflow
{
    #[Workflow('laravel.client-credential-probe')]
    public function run(): void
    {
    }
}

/** @return array{getenv: bool, _ENV: bool, _SERVER: bool, laravel_env: bool} */
function laravelFreshEnvironmentPresence(string $name): array
{
    return [
        'getenv' => getenv($name) !== false,
        '_ENV' => array_key_exists($name, $_ENV),
        '_SERVER' => array_key_exists($name, $_SERVER),
        'laravel_env' => Env::get($name) !== null,
    ];
}

/** @return array<string, array{getenv: bool, _ENV: bool, _SERVER: bool, laravel_env: bool}> */
function laravelFreshCredentialPresence(): array
{
    $presence = [];
    foreach ([
        'DURABLE_WORKFLOW_TOKEN',
        'DURABLE_WORKFLOW_CLIENT_TOKEN',
        'DURABLE_WORKFLOW_WORKER_TOKEN',
        'DURABLE_WORKFLOW_PROCESS_ROLE',
        'DURABLE_WORKFLOW_PROCESS_TOKEN',
    ] as $name) {
        $presence[$name] = laravelFreshEnvironmentPresence($name);
    }

    return $presence;
}

function laravelFreshAuthentication(Client $client): Authentication
{
    $property = (new ReflectionClass($client))->getProperty('authentication');
    $authentication = $property->getValue($client);
    if (!$authentication instanceof Authentication) {
        throw new RuntimeException('Fresh Laravel did not construct authenticated role clients.');
    }

    return $authentication;
}

function laravelFreshWorkerClient(WorkerFactory $factory): Client
{
    $property = (new ReflectionClass($factory))->getProperty('client');
    $client = $property->getValue($factory);
    if (!$client instanceof Client) {
        throw new RuntimeException('Fresh Laravel did not inject the worker client.');
    }

    return $client;
}

function laravelFreshAssertOppositeRoleFails(callable $operation, string $role): void
{
    try {
        $operation();
    } catch (InvalidArgumentException $exception) {
        if (!str_contains($exception->getMessage(), "{$role} credential is required")) {
            throw $exception;
        }

        return;
    }

    throw new RuntimeException("Fresh Laravel accepted the opposite {$role} credential role.");
}

function laravelFreshBootstrap(string $basePath): Application
{
    $application = require $basePath.'/bootstrap/app.php';
    if (!$application instanceof Application) {
        throw new RuntimeException('Fresh Laravel did not bootstrap an application.');
    }
    if (!$application->configurationIsCached()) {
        throw new RuntimeException('Fresh Laravel did not boot its cached configuration.');
    }

    $application->make(Kernel::class)->bootstrap();

    return $application;
}

/** @param array<string, array{getenv: bool, _ENV: bool, _SERVER: bool, laravel_env: bool}> $presence */
function laravelFreshAssertActualProcessRole(array $presence, string $role): void
{
    if ($presence['DURABLE_WORKFLOW_TOKEN']['getenv']
        || $presence['DURABLE_WORKFLOW_CLIENT_TOKEN']['getenv']
        || $presence['DURABLE_WORKFLOW_WORKER_TOKEN']['getenv']
        || !$presence['DURABLE_WORKFLOW_PROCESS_ROLE']['getenv']
        || !$presence['DURABLE_WORKFLOW_PROCESS_TOKEN']['getenv']
        || getenv('DURABLE_WORKFLOW_PROCESS_ROLE') !== $role
        || getenv('DURABLE_WORKFLOW_PROCESS_TOKEN') !== ($role === 'client'
            ? LARAVEL_FRESH_CLIENT_TOKEN
            : LARAVEL_FRESH_WORKER_TOKEN)
    ) {
        throw new RuntimeException("Fresh Laravel {$role} process did not receive an isolated role handoff.");
    }
}

function laravelFreshRunChild(string $basePath, string $role): void
{
    $application = laravelFreshBootstrap($basePath);

    if ($role === 'worker') {
        if ($application->resolved(WorkerFactory::class)) {
            throw new RuntimeException('Fresh Laravel resolved the worker before the role reproduction.');
        }
    }

    $presence = laravelFreshCredentialPresence();
    laravelFreshAssertActualProcessRole($presence, $role);
    fwrite(STDOUT, json_encode(['role' => $role, 'presence' => $presence], JSON_THROW_ON_ERROR).PHP_EOL);

    if ($role === 'worker') {
        if (!$presence['DURABLE_WORKFLOW_PROCESS_ROLE']['laravel_env']
            || !$presence['DURABLE_WORKFLOW_PROCESS_TOKEN']['laravel_env']
        ) {
            throw new RuntimeException('Fresh Laravel lost the explicit worker credential handoff during bootstrap.');
        }
        $application->make('config')->set(
            'durable-workflow.handlers',
            [LaravelFreshClientOperationWorkflow::class],
        );
        $applicationClient = $application->make(LaravelWorkflowClientInterface::class);
        $factory = $application->make(WorkerFactory::class);
        $client = laravelFreshWorkerClient($factory);
        if (!array_key_exists('Authorization', laravelFreshAuthentication($client)->headers(true))) {
            throw new RuntimeException('Fresh Laravel worker did not receive its process credential.');
        }
        laravelFreshAssertOppositeRoleFails(
            static fn () => $applicationClient->handle(
                LaravelFreshClientOperationWorkflow::class,
                'client-credential-probe',
            ),
            'client',
        );

        return;
    }

    if ($role === 'client') {
        $client = $application->make(WorkflowClientInterface::class);
        if (!$client instanceof Client
            || !array_key_exists('Authorization', laravelFreshAuthentication($client)->headers(false))
        ) {
            throw new RuntimeException('Fresh Laravel application did not receive its process credential.');
        }
        laravelFreshAssertOppositeRoleFails(
            static fn () => $application->make(WorkerFactory::class),
            'worker',
        );

        return;
    }

    throw new RuntimeException('Unknown fresh Laravel role process.');
}

function laravelFreshAssertContainsNoCredentials(string $cachePath): void
{
    $contents = file_get_contents($cachePath);
    if (!is_string($contents)) {
        throw new RuntimeException('Fresh Laravel did not produce a readable configuration cache.');
    }
    foreach ([
        'DURABLE_WORKFLOW_TOKEN',
        'DURABLE_WORKFLOW_CLIENT_TOKEN',
        'DURABLE_WORKFLOW_WORKER_TOKEN',
        'DURABLE_WORKFLOW_PROCESS_ROLE',
        'DURABLE_WORKFLOW_PROCESS_TOKEN',
        LARAVEL_FRESH_CLIENT_TOKEN,
        LARAVEL_FRESH_WORKER_TOKEN,
    ] as $credentialBytes) {
        if (str_contains($contents, $credentialBytes)) {
            throw new RuntimeException('Fresh Laravel serialized credential bytes into its configuration cache.');
        }
    }

    $configuration = require $cachePath;
    if (!is_array($configuration)
        || isset($configuration['durable-workflow']['credentials'])
    ) {
        throw new RuntimeException('Fresh Laravel cached Durable Workflow credentials.');
    }
}

$basePath = $argv[1] ?? null;
if (!is_string($basePath) || !is_file($basePath.'/artisan')) {
    throw new InvalidArgumentException('Pass the path to a fresh Laravel application.');
}
require $basePath.'/vendor/autoload.php';

if (($argv[2] ?? null) === '--assert-probes') {
    $probePath = $argv[3] ?? null;
    $lines = is_string($probePath)
        ? file($probePath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES)
        : false;
    if (!is_array($lines)) {
        throw new RuntimeException('Fresh Laravel role probes were not recorded.');
    }
    $observed = [];
    foreach ($lines as $line) {
        $entry = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($entry)) {
            throw new RuntimeException('Fresh Laravel recorded an invalid role probe.');
        }
        fwrite(STDOUT, json_encode($entry, JSON_THROW_ON_ERROR).PHP_EOL);
        $clientRole = ($entry['role_is_client'] ?? null) === true;
        $workerRole = ($entry['role_is_worker'] ?? null) === true;
        if ($clientRole === $workerRole) {
            throw new RuntimeException('Fresh Laravel role probe did not identify exactly one process role.');
        }
        $role = $clientRole ? 'client' : 'worker';
        $stage = $entry['stage'] ?? null;
        $presence = $entry['presence'] ?? null;
        if (!is_string($stage) || !is_array($presence)) {
            throw new RuntimeException('Fresh Laravel role probe omitted its stage or presence map.');
        }
        foreach (['DURABLE_WORKFLOW_TOKEN', 'DURABLE_WORKFLOW_CLIENT_TOKEN', 'DURABLE_WORKFLOW_WORKER_TOKEN'] as $name) {
            if (($presence[$name]['getenv'] ?? null) !== false) {
                throw new RuntimeException("Fresh Laravel {$role} {$stage} retained ambient {$name}.");
            }
        }
        foreach (['DURABLE_WORKFLOW_PROCESS_ROLE', 'DURABLE_WORKFLOW_PROCESS_TOKEN'] as $name) {
            if (($presence[$name]['getenv'] ?? null) !== true
                || ($stage !== 'shell-entry' && ($presence[$name]['laravel_env'] ?? null) !== true)
            ) {
                throw new RuntimeException("Fresh Laravel {$role} {$stage} lost its explicit {$name} handoff.");
            }
        }
        $observed["{$role}:{$stage}"] = true;
    }
    foreach (['client', 'worker'] as $role) {
        foreach (['shell-entry', 'before-bootstrap', 'after-bootstrap'] as $stage) {
            if (!isset($observed["{$role}:{$stage}"])) {
                throw new RuntimeException("Fresh Laravel did not record {$role} credential presence at {$stage}.");
            }
        }
    }
    exit(0);
}
if (($argv[2] ?? null) === '--assert-invalid-handoff') {
    $expected = $argv[3] ?? '';
    try {
        ProcessCredentialResolver::workerClient(new ServiceConfiguration(
            'http://127.0.0.1:8080',
            'default',
            'fresh-cache-workers',
            [],
        ));
    } catch (InvalidArgumentException $exception) {
        if ($expected !== '' && str_contains($exception->getMessage(), $expected)) {
            exit(0);
        }
        throw $exception;
    }
    throw new RuntimeException('Fresh Laravel accepted an invalid explicit process credential handoff.');
}
if (($argv[2] ?? null) === '--assert-cache') {
    laravelFreshAssertContainsNoCredentials($basePath.'/bootstrap/cache/config.php');
    exit(0);
}
if (($argv[2] ?? null) === '--child') {
    try {
        laravelFreshRunChild($basePath, $argv[3] ?? '');
    } catch (Throwable $exception) {
        fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
        exit(1);
    }
    exit(0);
}

throw new InvalidArgumentException('Pass --assert-cache or --child with a Laravel process role.');
