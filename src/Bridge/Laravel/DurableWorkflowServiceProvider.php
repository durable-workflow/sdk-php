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

            // Laravel configuration is serializable and may be cached. Credentials
            // belong to the process and are resolved by each role-specific factory.
            if (is_array($values)) {
                unset($values['credentials']);
                $runtimeUrl = $values['runtime_url'] ?? null;
                if (!is_string($runtimeUrl)) {
                    throw new \InvalidArgumentException(
                        'Durable Workflow Laravel runtime_url must be a string. Set DURABLE_WORKFLOW_RUNTIME_URL to the Server origin or complete Cloud namespace runtime URL.',
                    );
                }
                $values['endpoint'] = $runtimeUrl;
                unset($values['runtime_url']);
            }

            return ServiceConfiguration::fromArray(is_array($values) ? $values : []);
        });
        $this->app->singleton(
            Client::class,
            static fn ($app): Client => ProcessCredentialResolver::applicationClient(
                $app->make(ServiceConfiguration::class),
            ),
        );
        $this->app->alias(Client::class, WorkflowClientInterface::class);
        $this->app->singleton(
            WorkflowServiceResolver::class,
            static fn ($app): WorkflowServiceResolver => new WorkflowServiceResolver(
                $app->make(ServiceConfiguration::class),
            ),
        );
        $this->app->bind(
            LaravelWorkflowClient::class,
            static fn ($app): LaravelWorkflowClient => new LaravelWorkflowClient(
                new DeferredWorkflowClient(
                    static fn (): WorkflowClientInterface => $app->make(WorkflowClientInterface::class),
                ),
                $app->make(ServiceConfiguration::class),
                $app->make(WorkflowServiceResolver::class),
            ),
        );
        $this->app->alias(LaravelWorkflowClient::class, LaravelWorkflowClientInterface::class);
        $this->app->singleton(WorkerFactory::class, function ($app): WorkerFactory {
            $logger = $app->bound(LoggerInterface::class)
                ? $app->make(LoggerInterface::class)
                : new NullLogger();
            $events = $app->bound(Dispatcher::class) ? $app->make(Dispatcher::class) : null;

            return new WorkerFactory(
                $app,
                $app->make(ServiceConfiguration::class),
                ProcessCredentialResolver::workerClient($app->make(ServiceConfiguration::class)),
                $logger,
                $events,
                ProcessCredentialResolver::workerCredentialRole(),
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
