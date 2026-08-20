<?php

declare(strict_types=1);

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Client;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Foundation\Application;
use Illuminate\Support\Env;
use Symfony\Component\Process\Process;

const LARAVEL_FRESH_CLIENT_TOKEN = 'fresh-cache-client-secret';
const LARAVEL_FRESH_WORKER_TOKEN = 'fresh-cache-worker-secret';

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
    $clientPresent = $presence['DURABLE_WORKFLOW_CLIENT_TOKEN']['getenv'];
    $workerPresent = $presence['DURABLE_WORKFLOW_WORKER_TOKEN']['getenv'];
    if ($presence['DURABLE_WORKFLOW_TOKEN']['getenv']
        || $clientPresent !== ($role === 'client')
        || $workerPresent !== ($role === 'worker')
    ) {
        throw new RuntimeException("Fresh Laravel {$role} process did not receive only its role credential.");
    }
}

function laravelFreshRunChild(string $basePath, string $role): void
{
    $application = laravelFreshBootstrap($basePath);

    if ($role === 'worker') {
        if ($application->resolved(WorkerFactory::class)) {
            throw new RuntimeException('Fresh Laravel resolved the worker before the role reproduction.');
        }
        // Reproduce the published failure's conflicting Laravel environment view:
        // the process environment is worker-only while Laravel's repository reports
        // only the opposite scoped credential. Runtime credentials must follow the
        // operating-system process environment, not this framework repository.
        unset($_ENV['DURABLE_WORKFLOW_WORKER_TOKEN'], $_SERVER['DURABLE_WORKFLOW_WORKER_TOKEN']);
        $_SERVER['DURABLE_WORKFLOW_CLIENT_TOKEN'] = LARAVEL_FRESH_CLIENT_TOKEN;
        Env::disablePutenv();
    }

    $presence = laravelFreshCredentialPresence();
    laravelFreshAssertActualProcessRole($presence, $role);
    fwrite(STDOUT, json_encode(['role' => $role, 'presence' => $presence], JSON_THROW_ON_ERROR).PHP_EOL);

    if ($role === 'worker') {
        if (!$presence['DURABLE_WORKFLOW_CLIENT_TOKEN']['laravel_env']
            || $presence['DURABLE_WORKFLOW_WORKER_TOKEN']['laravel_env']
        ) {
            throw new RuntimeException('Fresh Laravel did not reproduce the conflicting cached environment view.');
        }
        $factory = $application->make(WorkerFactory::class);
        $client = laravelFreshWorkerClient($factory);
        if (!array_key_exists('Authorization', laravelFreshAuthentication($client)->headers(true))) {
            throw new RuntimeException('Fresh Laravel worker did not receive its process credential.');
        }
        laravelFreshAssertOppositeRoleFails(
            static fn () => $application->make(WorkflowClientInterface::class),
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

if (($argv[2] ?? null) === '--child') {
    try {
        laravelFreshRunChild($basePath, $argv[3] ?? '');
    } catch (Throwable $exception) {
        fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
        exit(1);
    }
    exit(0);
}

$baseEnvironment = [
    'DURABLE_WORKFLOW_TOKEN' => false,
    'DURABLE_WORKFLOW_CLIENT_TOKEN' => false,
    'DURABLE_WORKFLOW_WORKER_TOKEN' => false,
];
$publish = new Process(
    [PHP_BINARY, $basePath.'/artisan', 'vendor:publish', '--tag=durable-workflow-config', '--force'],
    $basePath,
    $baseEnvironment,
);
$publish->mustRun();

$cache = new Process(
    [PHP_BINARY, $basePath.'/artisan', 'config:cache'],
    $basePath,
    array_merge($baseEnvironment, [
        'DURABLE_WORKFLOW_CLIENT_TOKEN' => LARAVEL_FRESH_CLIENT_TOKEN,
        'DURABLE_WORKFLOW_WORKER_TOKEN' => LARAVEL_FRESH_WORKER_TOKEN,
    ]),
);
$cache->mustRun();
laravelFreshAssertContainsNoCredentials($basePath.'/bootstrap/cache/config.php');

$roles = [
    'worker' => ['DURABLE_WORKFLOW_WORKER_TOKEN' => LARAVEL_FRESH_WORKER_TOKEN],
    'client' => ['DURABLE_WORKFLOW_CLIENT_TOKEN' => LARAVEL_FRESH_CLIENT_TOKEN],
];
foreach ($roles as $role => $environment) {
    $process = new Process(
        [PHP_BINARY, __FILE__, $basePath, '--child', $role],
        env: array_merge($baseEnvironment, $environment),
    );
    $process->mustRun();
    fwrite(STDOUT, $process->getOutput());
}

fwrite(STDOUT, 'Fresh Laravel cached role isolation passed.'.PHP_EOL);
