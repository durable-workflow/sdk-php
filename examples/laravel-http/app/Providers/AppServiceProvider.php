<?php

namespace App\Providers;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Client;
use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if (! $this->app->runningInConsole() && config('example.eager')) {
            $this->app->make(Client::class);
            $this->app->make(LaravelWorkflowClientInterface::class);
        }
    }
}
