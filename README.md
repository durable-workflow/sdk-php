# Durable Workflow PHP SDK

<p align="center">
  <a href="https://github.com/durable-workflow/sdk-php/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://github.com/durable-workflow/sdk-php/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI status"></a>
  <a href="https://packagist.org/packages/durable-workflow/sdk"><img src="https://img.shields.io/packagist/v/durable-workflow/sdk.svg" alt="Packagist version"></a>
  <a href="https://packagist.org/packages/durable-workflow/sdk"><img src="https://img.shields.io/packagist/php-v/durable-workflow/sdk.svg" alt="Supported PHP versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/durable-workflow/sdk-php" alt="MIT license"></a>
</p>

The first-party PHP client and worker SDK for
[Durable Workflow Cloud](https://cloud.durable-workflow.com/early-access) and
self-hosted [Durable Workflow Server](https://github.com/durable-workflow/server).
Use it from plain PHP, Laravel, or Symfony to run durable workflows outside the
application process while keeping framework-native configuration, dependency
injection, commands, logging, and tests.

## Install

```bash
composer require durable-workflow/sdk:^2.0
```

The SDK requires PHP 8.1 or newer. It uses the official `apache/avro` package
for portable payloads and accepts any PSR-18 HTTP client.

## Choose Your Path

| Starting point | Recommended guide |
| --- | --- |
| Plain PHP client or worker | [First workflow](https://php.durable-workflow.com/getting-started/first-workflow/) |
| Laravel v1 or v2 embedded | [Choose a Laravel ownership model](https://php.durable-workflow.com/frameworks/laravel/) |
| Laravel service mode | [Laravel service-mode bridge](https://php.durable-workflow.com/frameworks/laravel/#service-mode) |
| Symfony service mode | [Symfony bundle](https://php.durable-workflow.com/frameworks/symfony/) |
| Runtime authentication | [Authentication guide](https://php.durable-workflow.com/operate/authentication/) |

Laravel applications can keep Durable Workflow embedded or move execution to
Cloud or Server. The service-mode bridge supplies container bindings, config,
an Artisan worker command, diagnostics, a facade, and a test fake. Symfony gets
the same application-shaped experience through its bundle and Console worker.

## Author a Workflow

Handlers can be ordinary attributed classes resolved by the worker container:

```php
<?php

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreetingWorkflow
{
    #[Workflow('example.greeting')]
    public function run(WorkflowContext $context, string $name): array
    {
        $greeting = $context->activity('example.greet', [$name]);

        return ['greeting' => $greeting];
    }
}

final class GreetingActivities
{
    #[Activity('example.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "Hello, {$name}";
    }
}
```

Register the classes on a task queue and start polling:

```php
use DurableWorkflow\Client;
use DurableWorkflow\Worker;

$client = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: getenv('DURABLE_WORKFLOW_WORKER_TOKEN'),
);

Worker::create($client, 'greetings')
    ->register(GreetingWorkflow::class, GreetingActivities::class)
    ->run();
```

Start the workflow from a client process using a client-role credential:

```php
$client = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);

$handle = $client->startWorkflow(
    workflowType: 'example.greeting',
    workflowId: 'greeting-'.bin2hex(random_bytes(12)),
    taskQueue: 'greetings',
    input: ['PHP'],
);

var_dump($handle->result());
```

For Cloud, use the complete namespace runtime URL exactly as provisioned. For
self-hosted Server, use its origin such as `http://localhost:8080`. Keep client
and worker credentials in separate processes.

The complete [plain PHP quickstart](https://php.durable-workflow.com/getting-started/first-workflow/)
includes the three runnable files, environment setup, expected output, and
common failure diagnostics.

## Capabilities

- Workflows, activities, child workflows, timers, retries, and heartbeats
- Signals, queries, updates, condition waits, and message streams
- Parallel work, sagas, cancellation, continue-as-new, and version markers
- Schedules, search attributes, memo, external payloads, and worker versioning
- Replay testing, in-memory client fakes, Laravel and Symfony test helpers
- Stable workflow handles and machine-readable runtime diagnostics

See the [complete SDK reference](docs/sdk-reference.md) for control-plane APIs,
worker configuration, Message Streams, framework setup, and testing examples.
The generated [API reference](https://php.durable-workflow.com/api/) documents
every public class and method.

## Development

```bash
composer install
composer test
composer analyse
composer benchmark-avro-value
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and validation details.

## License

MIT
