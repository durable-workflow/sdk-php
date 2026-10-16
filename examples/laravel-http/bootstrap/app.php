<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;
use Illuminate\Http\Request;
use DurableWorkflow\Exception\ServerException;

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
        $exceptions->render(function (ServerException $exception, Request $request) {
            if ($request->is('api/*')) {
                $status = $exception->status >= 400 && $exception->status <= 599 ? $exception->status : 502;

                return response()->json([
                    'error' => 'runtime_operation_refused', 'reason' => $exception->reason,
                ], $status);
            }
        });
    })->create();

$application->useStoragePath(getenv('EXAMPLE_STORAGE_PATH') ?: dirname(__DIR__).'/storage');

return $application;
