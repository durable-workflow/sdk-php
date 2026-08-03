<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/** Auto-discovered Laravel registration for the optional service-mode bridge. */
final class DurableWorkflowServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom($this->configPath(), 'durable-workflow');

        $this->app->singleton(ServiceConfiguration::class, function (): ServiceConfiguration {
            $values = $this->app->make('config')->get('durable-workflow', []);

            return ServiceConfiguration::fromArray(is_array($values) ? $values : []);
        });
        $this->app->singleton(Client::class, static fn ($app): Client => $app->make(ServiceConfiguration::class)->client());
        $this->app->alias(Client::class, WorkflowClientInterface::class);
        $this->app->singleton(WorkerFactory::class, function ($app): WorkerFactory {
            $logger = $app->bound(LoggerInterface::class)
                ? $app->make(LoggerInterface::class)
                : new NullLogger();
            $events = $app->bound(Dispatcher::class) ? $app->make(Dispatcher::class) : null;

            return new WorkerFactory(
                $app,
                $app->make(ServiceConfiguration::class),
                $app->make(Client::class),
                $logger,
                $events,
            );
        });
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                $this->configPath() => $this->app->configPath('durable-workflow.php'),
            ], 'durable-workflow-config');
            $this->commands([WorkerCommand::class]);
        }
    }

    private function configPath(): string
    {
        return dirname(__DIR__, 3).'/resources/laravel/durable-workflow.php';
    }
}
