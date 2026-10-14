<?php

return [
    'runtime_url' => env('DURABLE_WORKFLOW_RUNTIME_URL', 'http://localhost:8080'),
    'namespace' => env('DURABLE_WORKFLOW_NAMESPACE', 'service-http-example'),
    'task_queue' => env('DURABLE_WORKFLOW_TASK_QUEUE', 'service-http-example'),
    'handlers' => [App\Workflows\HttpWorkflow::class, App\Workflows\EchoActivity::class],
    'poll_timeout_seconds' => 5,
    'worker' => [
        'sticky_cache' => [
            'capacity' => filter_var(env('EXAMPLE_HISTORY_CAPACITY', 100), FILTER_VALIDATE_INT),
            'ttl_seconds' => filter_var(env('EXAMPLE_HISTORY_TTL', 300), FILTER_VALIDATE_INT),
        ],
    ],
    // Runtime credentials are read from the process environment by the SDK.
];
