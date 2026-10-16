<?php

namespace App\Providers;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Client;
use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        // Octane HTTP workers bootstrap through CLI. Preparation has no credentials.
        if (getenv('EXAMPLE_PROCESS_ROLE') === 'http' && config('example.eager')) {
            $this->app->make(Client::class);
            $this->app->make(LaravelWorkflowClientInterface::class);
        }
        $this->app->instance('example.sdk_boot_resolved', $this->app->resolved(Client::class));
    }
}
