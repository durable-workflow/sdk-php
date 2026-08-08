<?php

declare(strict_types=1);

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Bridge\Laravel\ProcessCredentialResolver;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Contracts\Console\Kernel;
use Symfony\Component\Process\Process;

require $argv[1];

const LARAVEL_CACHE_CONTROL_TOKEN = 'cache-control-secret';
const LARAVEL_CACHE_WORKER_TOKEN = 'cache-worker-secret';
const LARAVEL_CACHE_SHARED_TOKEN = 'cache-shared-secret';

final class LaravelCacheTransport implements Transport
{
    /** @var list<array{method: string, uri: string, headers: array<string, string>}> */
    public array $requests = [];

    /** @param list<array<string, mixed>|list<mixed>|null> $responses */
    public function __construct(private array $responses)
    {
    }

    /** @return array<string, mixed>|list<mixed>|null */
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->requests[] = ['method' => $method, 'uri' => $uri, 'headers' => $headers];

        return array_shift($this->responses);
    }
}

function laravelCacheAuthentication(Client $client): Authentication
{
    $property = (new ReflectionClass($client))->getProperty('authentication');
    $authentication = $property->getValue($client);
    if (!$authentication instanceof Authentication) {
        throw new RuntimeException('Cached Laravel client did not retain process authentication.');
    }

    return $authentication;
}

function laravelCacheWorkerClient(WorkerFactory $factory): Client
{
    $property = (new ReflectionClass($factory))->getProperty('client');
    $client = $property->getValue($factory);
    if (!$client instanceof Client) {
        throw new RuntimeException('Cached Laravel worker factory did not receive a client.');
    }

    return $client;
}

function laravelCacheAssertFailsBeforeTransport(callable $operation, string $role): void
{
    try {
        $operation();
    } catch (InvalidArgumentException $exception) {
        if (!str_contains($exception->getMessage(), "{$role} credential is required")) {
            throw $exception;
        }

        return;
    }

    throw new RuntimeException("The cached Laravel {$role} boundary accepted the opposite role credential.");
}

function laravelCacheBootstrap(string $basePath): Illuminate\Foundation\Application
{
    $application = require $basePath.'/bootstrap/app.php';
    if (!$application instanceof Illuminate\Foundation\Application) {
        throw new RuntimeException('The cached Laravel fixture did not bootstrap an application.');
    }

    $application->make(Kernel::class)->bootstrap();

    return $application;
}

function laravelCacheAssertContainsNoCredentials(string $cachePath): void
{
    $contents = file_get_contents($cachePath);
    if (!is_string($contents)) {
        throw new RuntimeException('Laravel did not produce a readable configuration cache.');
    }
    foreach ([
        LARAVEL_CACHE_CONTROL_TOKEN,
        LARAVEL_CACHE_WORKER_TOKEN,
        LARAVEL_CACHE_SHARED_TOKEN,
    ] as $credential) {
        if (str_contains($contents, $credential)) {
            throw new RuntimeException('Laravel serialized a Durable Workflow credential into its configuration cache.');
        }
    }

    $configuration = require $cachePath;
    if (!is_array($configuration)) {
        throw new RuntimeException('Laravel produced an invalid configuration cache.');
    }
    $credentials = $configuration['durable-workflow']['credentials'] ?? [];
    if (!is_array($credentials)) {
        throw new RuntimeException('Cached Durable Workflow credentials must not be configured.');
    }
    foreach (['token', 'control_token', 'worker_token'] as $name) {
        if (($credentials[$name] ?? null) !== null && ($credentials[$name] ?? null) !== '') {
            throw new RuntimeException("Laravel cached the Durable Workflow {$name} credential.");
        }
    }
}

function laravelCacheRunChild(string $basePath, string $role): void
{
    $application = laravelCacheBootstrap($basePath);
    if (!$application->configurationIsCached()) {
        throw new RuntimeException('The Laravel role process did not boot the cached configuration.');
    }
    $configuration = $application->make('config')->get('durable-workflow', []);
    if (!is_array($configuration) || isset($configuration['credentials'])) {
        throw new RuntimeException('The Laravel role process loaded credentials from cached configuration.');
    }

    if ($role === 'control') {
        $client = $application->make(WorkflowClientInterface::class);
        if (!$client instanceof Client) {
            throw new RuntimeException('The control-only process did not resolve the Laravel application client.');
        }
        $authentication = laravelCacheAuthentication($client);
        if (($authentication->headers(false)['Authorization'] ?? null) !== 'Bearer '.LARAVEL_CACHE_CONTROL_TOKEN) {
            throw new RuntimeException('The control-only process did not resolve its runtime credential.');
        }
        laravelCacheAssertFailsBeforeTransport(
            static fn () => $client->registerWorker('opposite-role', 'cache-workers', [], []),
            'worker',
        );
        laravelCacheAssertFailsBeforeTransport(
            static fn () => $application->make(WorkerFactory::class),
            'worker',
        );

        return;
    }

    if ($role === 'worker') {
        $factory = $application->make(WorkerFactory::class);
        $client = laravelCacheWorkerClient($factory);
        $authentication = laravelCacheAuthentication($client);
        if (($authentication->headers(true)['Authorization'] ?? null) !== 'Bearer '.LARAVEL_CACHE_WORKER_TOKEN) {
            throw new RuntimeException('The worker-only process did not resolve its runtime credential.');
        }
        laravelCacheAssertFailsBeforeTransport(static fn () => $client->health(), 'control');
        laravelCacheAssertFailsBeforeTransport(
            static fn () => $application->make(WorkflowClientInterface::class),
            'control',
        );

        $transport = new LaravelCacheTransport([[
            'worker_id' => 'cache-worker',
            'outcome' => 'deregistered',
            'recovered_workflow_task_count' => 0,
        ]]);
        $runtimeClient = ProcessCredentialResolver::workerClient(
            $application->make(ServiceConfiguration::class),
            $transport,
        );
        $deregistration = $runtimeClient->deregisterWorkerRegistration('cache-worker');
        if ($deregistration['outcome'] !== 'deregistered'
            || count($transport->requests) !== 1
            || $transport->requests[0]['method'] !== 'DELETE'
            || !str_ends_with($transport->requests[0]['uri'], '/api/worker/registrations/cache-worker')
            || ($transport->requests[0]['headers']['Authorization'] ?? null)
                !== 'Bearer '.LARAVEL_CACHE_WORKER_TOKEN
        ) {
            throw new RuntimeException('The worker-only process did not complete graceful deregistration.');
        }

        return;
    }

    if ($role === 'shared') {
        $controlClient = $application->make(WorkflowClientInterface::class);
        $workerClient = laravelCacheWorkerClient($application->make(WorkerFactory::class));
        if (!$controlClient instanceof Client
            || (laravelCacheAuthentication($controlClient)->headers(false)['Authorization'] ?? null)
                !== 'Bearer '.LARAVEL_CACHE_SHARED_TOKEN
            || (laravelCacheAuthentication($workerClient)->headers(true)['Authorization'] ?? null)
                !== 'Bearer '.LARAVEL_CACHE_SHARED_TOKEN
        ) {
            throw new RuntimeException('The cached Laravel application did not preserve shared-token authentication.');
        }

        return;
    }

    throw new RuntimeException('Unknown cached Laravel role process.');
}

function laravelCacheRemoveFixture(string $path): void
{
    if (!is_dir($path)) {
        return;
    }

    $iterator = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($path, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST,
    );
    foreach ($iterator as $entry) {
        if ($entry->isLink() || $entry->isFile()) {
            unlink($entry->getPathname());
        } else {
            rmdir($entry->getPathname());
        }
    }
    rmdir($path);
}

$mode = $argv[2] ?? null;
$basePath = $argv[3] ?? null;
if ($mode === '--cache' && is_string($basePath)) {
    $application = require $basePath.'/bootstrap/app.php';
    if (!$application instanceof Illuminate\Foundation\Application) {
        throw new RuntimeException('The Laravel cache builder did not bootstrap an application.');
    }
    $kernel = $application->make(Kernel::class);
    if ($kernel->call('config:cache') !== 0) {
        throw new RuntimeException('Laravel config:cache failed: '.$kernel->output());
    }
    exit(0);
}
if ($mode === '--child' && is_string($basePath)) {
    laravelCacheRunChild($basePath, $argv[4] ?? '');
    exit(0);
}

$autoloadPath = realpath($argv[1]);
if (!is_string($autoloadPath)) {
    throw new RuntimeException('The Laravel compatibility autoloader does not exist.');
}
$fixturePath = sys_get_temp_dir().'/durable-workflow-laravel-cache-'.bin2hex(random_bytes(8));
$cachePath = $fixturePath.'/bootstrap/cache/config.php';

try {
    foreach (['bootstrap/cache', 'config', 'storage/framework/cache', 'storage/framework/sessions', 'storage/framework/views'] as $directory) {
        if (!mkdir($fixturePath.'/'.$directory, 0777, true) && !is_dir($fixturePath.'/'.$directory)) {
            throw new RuntimeException("Could not create Laravel fixture directory {$directory}.");
        }
    }
    if (!symlink(dirname($autoloadPath), $fixturePath.'/vendor')) {
        throw new RuntimeException('Could not expose the compatibility dependency graph to the Laravel fixture.');
    }
    if (!copy(dirname(__DIR__, 2).'/resources/laravel/durable-workflow.php', $fixturePath.'/config/durable-workflow.php')) {
        throw new RuntimeException('Could not publish the Durable Workflow Laravel configuration.');
    }
    $bootstrap = <<<'PHP'
<?php

declare(strict_types=1);

use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use Illuminate\Foundation\Application;

return Application::configure(basePath: dirname(__DIR__))
    ->withProviders([DurableWorkflowServiceProvider::class])
    ->create();
PHP;
    if (file_put_contents($fixturePath.'/bootstrap/app.php', $bootstrap.PHP_EOL) === false) {
        throw new RuntimeException('Could not create the Laravel fixture bootstrap.');
    }

    $baseEnvironment = [
        'DURABLE_WORKFLOW_TOKEN' => false,
        'DURABLE_WORKFLOW_CONTROL_TOKEN' => false,
        'DURABLE_WORKFLOW_WORKER_TOKEN' => false,
    ];
    $cache = new Process([PHP_BINARY, __FILE__, $autoloadPath, '--cache', $fixturePath], env: $baseEnvironment);
    $cache->mustRun();
    laravelCacheAssertContainsNoCredentials($cachePath);

    $roles = [
        'control' => ['DURABLE_WORKFLOW_CONTROL_TOKEN' => LARAVEL_CACHE_CONTROL_TOKEN],
        'worker' => ['DURABLE_WORKFLOW_WORKER_TOKEN' => LARAVEL_CACHE_WORKER_TOKEN],
        'shared' => ['DURABLE_WORKFLOW_TOKEN' => LARAVEL_CACHE_SHARED_TOKEN],
    ];
    foreach ($roles as $role => $environment) {
        $process = new Process(
            [PHP_BINARY, __FILE__, $autoloadPath, '--child', $fixturePath, $role],
            env: array_merge($baseEnvironment, $environment),
        );
        $process->mustRun();
    }
} finally {
    laravelCacheRemoveFixture($fixturePath);
}

fwrite(STDOUT, 'Laravel cached configuration compatibility passed.'.PHP_EOL);
