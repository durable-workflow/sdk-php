<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;

$application = Application::configure(basePath: dirname(__DIR__))
    ->withRouting(
        api: __DIR__.'/../routes/api.php',
        commands: __DIR__.'/../routes/console.php',
        health: '/up',
    )
    ->withMiddleware(function (Middleware $middleware): void {
        $middleware->alias(['example' => \App\Http\Middleware\ExampleContext::class]);
    })
    ->withExceptions(function (Exceptions $exceptions): void {
    })->create();

$application->useStoragePath(getenv('EXAMPLE_STORAGE_PATH') ?: dirname(__DIR__).'/storage');

return $application;
