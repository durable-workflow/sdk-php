<?php

declare(strict_types=1);

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;

final class ExampleContext
{
    public function handle(Request $request, Closure $next)
    {
        $user = config('example.users')[$request->bearerToken() ?? ''] ?? null;
        abort_unless(is_string($user), 401);
        $namespace = config('durable-workflow.namespace');
        abort_unless($request->header('X-Namespace', $namespace) === $namespace, 403);
        $requestId = $request->header('X-Request-Id');
        abort_unless(is_string($requestId) && preg_match('/^[a-zA-Z0-9_-]{1,80}$/', $requestId) === 1, 422);
        $request->attributes->set('example_user', $user);
        $context = [
            'request_id' => $requestId,
            'user' => $user,
            'namespace' => $namespace,
            'optional_context' => $request->header('X-Optional-Context'),
        ];
        Log::withContext($context);
        $started = hrtime(true);
        $status = 500;

        try {
            $response = $next($request);
            $status = $response->getStatusCode();
            $response->headers->add([
                'X-Request-Id' => $requestId,
                'X-Worker-Pid' => (string) getmypid(),
                'X-Example-User' => $user,
                'X-Example-Namespace' => $namespace,
                'X-Optional-Context' => (string) ($context['optional_context'] ?? ''),
            ]);

            return $response;
        } finally {
            Log::info('example.request', [
                'pid' => getmypid(), 'path' => $request->path(), 'status' => $status,
                'elapsed_ms' => (hrtime(true) - $started) / 1e6,
            ]);
            Log::withoutContext(array_keys($context));
        }
    }
}
