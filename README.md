# Durable Workflow PHP SDK

The first-party, framework-neutral PHP SDK for applications and remote workers
that connect to a standalone [Durable Workflow server](https://github.com/durable-workflow/server).
It targets PHP 8.1 or newer and does not require Laravel or the embedded
`durable-workflow/workflow` engine.

## Install

Install the package from Packagist:

```bash
composer require durable-workflow/sdk:^0.1
```

To install directly from the source repository before a tagged release:

```bash
composer config repositories.durable-workflow-sdk vcs https://github.com/durable-workflow/sdk-php
composer require durable-workflow/sdk:dev-main
```

The SDK uses the official [`apache/avro`](https://packagist.org/packages/apache/avro)
package for schema parsing and binary payload encoding. Guzzle is included as
the default PSR-18 transport; any PSR-18 client and PSR-17 factories can be
injected instead.

## Start and inspect a workflow

```php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Auth\TokenAuthentication;

$client = new Client(
    'http://localhost:8080',
    new TokenAuthentication('dev-token-123'),
    namespace: 'default',
);

$handle = $client->startWorkflow(
    workflowType: 'greeter',
    workflowId: 'greeting-1',
    taskQueue: 'php-workers',
    input: ['world'],
);

$handle->signal('set-language', ['en']);
var_dump($handle->query('status'));
var_dump($handle->result(timeoutSeconds: 30));
```

`WorkflowHandle` distinguishes the stable workflow instance from a selected
run. Its ordinary operations follow whichever run is current after a
continue-as-new transition. The `*SelectedRun()` methods retain the original
run guard and fail rather than silently targeting a successor.

## Control-plane administration and discovery

`Client::withNamespace()` returns an immutable namespace selection that keeps
the configured authentication, transport, and payload codec. Workflow
visibility results and the newly covered administrative surfaces use SDK model
types while retaining the complete server payload in each model's `raw`
property.

```php
use DurableWorkflow\Model\ServiceOperationOptions;

$orders = $client->withNamespace('orders-prod');

$page = $orders->listWorkflows(
    workflowType: 'orders.process',
    status: 'running',
    query: 'CustomerId = "42"',
    pageSize: 25,
);
$schedulePage = $orders->listSchedules(
    status: 'paused',
    workflowType: 'reports.rollup',
    query: 'Region = "eu-west"',
    pageSize: 25,
);

$attributes = $orders->listSearchAttributes();
$orders->createSearchAttribute('OrderTotal', 'double');
$orders->deleteSearchAttribute('TemporaryField');

$orders->setNamespaceExternalStorage(
    'orders-prod',
    's3',
    thresholdBytes: 2 * 1024 * 1024,
    config: ['bucket' => 'workflow-payloads'],
);

$operation = $orders->startServiceOperation(
    'payments',
    'Cards',
    'authorize',
    ['amount' => 4200, 'currency' => 'USD'],
    new ServiceOperationOptions(idempotencyKey: 'order-42-authorization'),
);

$call = $operation->describe();
$operation->cancel('customer request');
$cluster = $orders->clusterInfo();
```

`listSchedules()` returns a typed page with `schedules`, `nextPageToken`, and
the original response in `raw`. It supports server-side `status`, workflow
type, visibility-query, page-size, and continuation-token filtering. Pass a
non-null `nextPageToken` back unchanged with the same namespace and filters to
read the next page. See the protocol guide for the paging and error contract.

`startServiceOperation()` explicitly starts an asynchronous call and returns a
`ServiceOperationHandle`. `executeServiceOperation()` honors the catalog mode,
waits for completion by default, and returns a `ServiceOperationDescription`.
Arguments use the client's payload codec; the default is the official Apache
Avro implementation.

## Run a remote PHP worker

Workflow handlers may be ordinary callables or generators. Yielding a command
from `WorkflowContext` creates a durable step; replay sends the recorded value
back into the generator without repeating the external work.

```php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

$client = new Client('http://server:8080', token: 'dev-token-123');
$worker = new Worker($client, 'php-workers');

$worker->registerActivity(
    'greet',
    static fn (ActivityContext $context, string $name): string => "hello, {$name}",
);

$worker->registerWorkflow(
    'greeter',
    static function (WorkflowContext $context, string $name): Generator {
        $greeting = yield $context->activity('greet', [$name]);

        return ['greeting' => $greeting];
    },
);

$worker->run();
```

Call `$context->heartbeat($details)` from a long-running activity. It throws
`ActivityCancelled` when the server requests cancellation. `Worker::run()`
installs SIGINT/SIGTERM handlers when `pcntl` is available, stops accepting new
tasks, and lets the active synchronous task settle before returning.

See [`examples/`](examples), the generated
[PHP API reference](https://php.durable-workflow.com/), and
[`docs/protocol.md`](docs/protocol.md) for the complete client, schedule,
namespace, visibility, search-attribute, service-operation, discovery,
authentication, worker, query, and update surfaces.

## Development

```bash
composer install
composer validate --strict
composer test
composer analyse
composer docs
```

The dependency-boundary check rejects Laravel, Illuminate, the embedded
workflow package, and the standalone server package in both declared and
resolved production dependencies.
