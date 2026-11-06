<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow as WorkflowFacade;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClient;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\WorkerFactory as LaravelWorkerFactory;
use DurableWorkflow\Bridge\Symfony\DurableWorkflowBundle;
use DurableWorkflow\Bridge\Symfony\DependencyInjection\DurableWorkflowExtension;
use DurableWorkflow\Bridge\Symfony\WorkerFactory as SymfonyWorkerFactory;
use DurableWorkflow\Client;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Foundation\Application;
use Illuminate\Foundation\Bootstrap\LoadConfiguration;
use Illuminate\Support\Facades\Facade;
use ReflectionObject;
use ReflectionProperty;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;

require_once __DIR__.'/StaticCredentialProbe.php';
require_once __DIR__.'/StaticCredentialTransportSpy.php';

/** Real provider/bundle construction; no handler execution, worker make/run or runtime request. */
final class StaticCredentialFrameworkProbe
{
    private const BOOTSTRAP_DIRECTORY = '/bootstrap';
    /** @return array<string, array{framework: string, plane: string, surface: string, cached: bool}> */
    public static function targets(): array
    {
        return [
            'LaravelClient' => ['framework' => 'laravel', 'plane' => 'control', 'surface' => 'client', 'cached' => false],
            'LaravelInterfaces' => ['framework' => 'laravel', 'plane' => 'control', 'surface' => 'interfaces', 'cached' => false],
            'LaravelFacade' => ['framework' => 'laravel', 'plane' => 'control', 'surface' => 'facade', 'cached' => false],
            'LaravelWorkerFactory' => ['framework' => 'laravel', 'plane' => 'worker', 'surface' => 'worker', 'cached' => false],
            'LaravelCachedClient' => ['framework' => 'laravel', 'plane' => 'control', 'surface' => 'client', 'cached' => true],
            'LaravelCachedWorkerFactory' => ['framework' => 'laravel', 'plane' => 'worker', 'surface' => 'worker', 'cached' => true],
            'SymfonyClient' => ['framework' => 'symfony', 'plane' => 'control', 'surface' => 'client', 'cached' => false],
            'SymfonyWorkerFactory' => ['framework' => 'symfony', 'plane' => 'worker', 'surface' => 'worker', 'cached' => false],
        ];
    }

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $process
     * @return array<string, string>
     */
    public static function headers(string $target, array $options, string $plane, StaticCredentialTransportSpy $spy, array $process = []): array
    {
        $metadata = self::targets()[$target] ?? null;
        if ($metadata === null || $metadata['plane'] !== $plane) {
            throw new StaticCredentialProbeFailure('Unsupported framework credential target.');
        }
        if ($metadata['framework'] === 'laravel') {
            if (!class_exists(Application::class)) {
                throw new StaticCredentialProbeFailure('The proposed Laravel framework is unavailable.');
            }
            return StaticCredentialProbe::environment(StaticCredentialProbe::processValues($options, $process),
                static fn (): array => self::laravel($metadata, $spy));
        }
        if ($process !== [] || !class_exists(FrameworkBundle::class)) {
            throw new StaticCredentialProbeFailure('The proposed Symfony framework is unavailable or unsupported.');
        }
        return self::symfony($metadata['surface'], $options, $plane, $spy);
    }

    /**
     * @param array{framework: string, plane: string, surface: string, cached: bool} $metadata
     * @return array<string, string>
     */
    private static function laravel(array $metadata, StaticCredentialTransportSpy $spy): array
    {
        $previousContainer = Container::getInstance();
        $previousFacade = Facade::getFacadeApplication();
        $timezone = date_default_timezone_get();
        $encoding = function_exists('mb_internal_encoding') ? mb_internal_encoding() : null;
        $base = self::directory();
        $app = null;
        try {
            $app = new Application($base);
            $values = ['runtime_url' => 'https://fixture.invalid', 'namespace' => 'fixture',
                'task_queue' => 'fixture', 'handlers' => []];
            $app->instance('config', new Repository(['durable-workflow' => $values]));
            if ($metadata['cached']) {
                self::cachedConfiguration($app, $base, $values);
            }
            $app->afterResolving(Client::class, static function (Client $client) use ($spy): void {
                $spy->attach($client);
            });
            (new DurableWorkflowServiceProvider($app))->register();
            Facade::clearResolvedInstances();
            Facade::setFacadeApplication($app);
            $client = self::laravelClient($app, $metadata['surface']);
            $spy->attach($client);
            return StaticCredentialProbe::authentication($client)?->headers($metadata['plane'] === 'worker') ?? [];
        } finally {
            $app?->flush();
            Facade::clearResolvedInstances();
            Facade::setFacadeApplication($previousFacade);
            Container::setInstance($previousContainer);
            date_default_timezone_set($timezone);
            if ($encoding !== null) {
                mb_internal_encoding($encoding);
            }
            self::removeDirectory($base);
        }
    }

    private static function laravelClient(Application $app, string $surface): Client
    {
        if ($surface === 'worker') {
            return self::factoryClient($app->make(LaravelWorkerFactory::class));
        }
        if ($surface === 'client') {
            return $app->make(Client::class);
        }
        $lazy = $app->make(WorkflowClientInterface::class);
        if ($app->resolved(Client::class) || $lazy instanceof Client) {
            throw new StaticCredentialProbeFailure('Laravel interface construction was not credential-lazy.');
        }
        if ($surface === 'facade') {
            $root = WorkflowFacade::getFacadeRoot();
            if (!$root instanceof LaravelWorkflowClientInterface
                || (new ReflectionProperty(LaravelWorkflowClient::class, 'client'))->getValue($root) !== $lazy
                || $app->resolved(Client::class)) {
                throw new StaticCredentialProbeFailure('Laravel facade lost its lazy workflow client.');
            }
        }
        $handle = $lazy->workflowHandle('fixture');
        $client = $app->make(Client::class);
        if ((new ReflectionObject($handle))->getProperty('client')->getValue($handle) !== $client) {
            throw new StaticCredentialProbeFailure('Laravel local handle used a different client.');
        }
        return $client;
    }

    /** @param array<string, mixed> $values */
    private static function cachedConfiguration(Application $app, string $base, array $values): void
    {
        $app->useBootstrapPath($base.self::BOOTSTRAP_DIRECTORY);
        $path = $app->getCachedConfigPath();
        if ($path !== $base.'/bootstrap/cache/config.php' || array_key_exists('credentials', $values)) {
            throw new StaticCredentialProbeFailure('Unexpected fixture configuration cache boundary.');
        }
        if (!mkdir($base.self::BOOTSTRAP_DIRECTORY, 0700) || !mkdir($base.'/bootstrap/cache', 0700)) {
            throw new StaticCredentialProbeFailure('Fixture cache directory creation failed.');
        }
        $bytes = '<?php return '.var_export(['durable-workflow' => $values,
            'app' => ['env' => 'production', 'timezone' => 'UTC']], true).";\n";
        if (file_put_contents($path, $bytes) !== strlen($bytes) || !chmod($path, 0600)) {
            throw new StaticCredentialProbeFailure('Fixture configuration cache creation failed.');
        }
        (new LoadConfiguration())->bootstrap($app);
        if (!$app->configurationIsCached() || $app->make('config')->get('durable-workflow.credentials') !== null) {
            throw new StaticCredentialProbeFailure('Fixture cached configuration retained credential settings.');
        }
    }

    /**
     * @param array<string, ?string> $options
     * @return array<string, string>
     */
    private static function symfony(string $surface, array $options, string $plane, StaticCredentialTransportSpy $spy): array
    {
        $credentials = [];
        foreach (['shared' => 'token', 'control' => 'control_token', 'worker' => 'worker_token'] as $role => $name) {
            if (array_key_exists($role, $options)) {
                $credentials[$name] = $options[$role];
            }
        }
        $container = new ContainerBuilder();
        $bundle = new DurableWorkflowBundle();
        $bundle->build($container);
        $extension = $bundle->getContainerExtension();
        if (!$extension instanceof DurableWorkflowExtension) {
            throw new StaticCredentialProbeFailure('Unexpected Symfony bundle extension.');
        }
        $extension->load([['endpoint' => 'https://fixture.invalid', 'namespace' => 'fixture',
            'task_queue' => 'fixture', 'handlers' => [], 'credentials' => $credentials]], $container);
        $container->compile();
        $client = $surface === 'worker'
            ? self::factoryClient($container->get(SymfonyWorkerFactory::class)) : $container->get(Client::class);
        if (!$client instanceof Client) {
            throw new StaticCredentialProbeFailure('Symfony did not construct the selected role client.');
        }
        if ($surface === 'client' && $container->get(WorkflowClientInterface::class) !== $client) {
            throw new StaticCredentialProbeFailure('Symfony workflow interface used a different application client.');
        }
        $spy->attach($client);
        return StaticCredentialProbe::authentication($client)?->headers($plane === 'worker') ?? [];
    }

    private static function factoryClient(object $factory): Client
    {
        $client = (new ReflectionObject($factory))->getProperty('client')->getValue($factory);
        if (!$client instanceof Client) {
            throw new StaticCredentialProbeFailure('Framework worker factory did not retain a client.');
        }
        return $client;
    }

    private static function directory(): string
    {
        $path = tempnam(sys_get_temp_dir(), 'sdk-php-credential-cache-');
        if ($path === false || !unlink($path) || !mkdir($path, 0700)) {
            throw new StaticCredentialProbeFailure('Fixture private directory creation failed.');
        }
        return $path;
    }

    private static function removeDirectory(string $base): void
    {
        foreach ([$base.'/bootstrap/cache/config.php', $base.'/bootstrap/cache', $base.self::BOOTSTRAP_DIRECTORY, $base] as $path) {
            if (is_file($path)) {
                unlink($path);
            } elseif (is_dir($path)) {
                rmdir($path);
            }
        }
    }
}
