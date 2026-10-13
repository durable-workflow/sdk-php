<?php

declare(strict_types=1);

return [
    // Server origin or complete Cloud runtime base URI. Do not append /api.
    'runtime_url' => env('DURABLE_WORKFLOW_RUNTIME_URL', 'http://localhost:8080'),
    'namespace' => env('DURABLE_WORKFLOW_NAMESPACE', 'default'),
    'task_queue' => env('DURABLE_WORKFLOW_TASK_QUEUE', 'php-workers'),

    // Credentials intentionally are not configuration values. The service provider
    // reads them from the process environment when it constructs each role client,
    // keeping secrets out of the artifact produced by `php artisan config:cache`.

    'handlers' => [
        // App\Workflows\OrderWorkflow::class,
        // App\Activities\OrderActivities::class,
    ],

    'poll_timeout_seconds' => 5,

    'worker' => [
        // Process-local durable history only; misses/expiry still replay authoritative history.
        'sticky_cache' => [
            'capacity' => 100,
            'ttl_seconds' => 300, // Positive integer, at most 3600 seconds.
        ],
    ],
];
