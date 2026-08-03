<?php

declare(strict_types=1);

return [
    // Server origin or complete Cloud runtime base URI. Do not append /api.
    'endpoint' => env('DURABLE_WORKFLOW_ENDPOINT', 'http://localhost:8080'),
    'namespace' => env('DURABLE_WORKFLOW_NAMESPACE', 'default'),
    'task_queue' => env('DURABLE_WORKFLOW_TASK_QUEUE', 'php-workers'),

    'credentials' => [
        // Secrets remain in the environment; vendor:publish never writes their values.
        // With scoped credentials, inject control_token only into web processes and
        // worker_token only into the process running durable-workflow:worker.
        'token' => env('DURABLE_WORKFLOW_TOKEN'),
        'control_token' => env('DURABLE_WORKFLOW_CONTROL_TOKEN'),
        'worker_token' => env('DURABLE_WORKFLOW_WORKER_TOKEN'),
    ],

    'handlers' => [
        // App\Workflows\OrderWorkflow::class,
        // App\Activities\OrderActivities::class,
    ],

    'poll_timeout_seconds' => 5,
];
