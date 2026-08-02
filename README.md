# Durable Workflow PHP SDK

The first-party, framework-neutral PHP SDK for applications and remote workers
that connect to a standalone [Durable Workflow server](https://github.com/durable-workflow/server).
It targets PHP 8.1 or newer and does not require Laravel or the embedded
`durable-workflow/workflow` engine.

## Install

Install the package from Packagist:

```bash
composer require durable-workflow/sdk:2.0.0-rc.8@RC
```

This exact package is PHP SDK `2.0.0-rc.8` and is qualified with Server
`2.0.0-rc.12`. SDK and Server prerelease counters are independent. Earlier 2.0
prereleases and pre-1.0 SDK releases remain historical rather than alternate
supported baselines.

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

Pass the Server origin or the complete Cloud runtime base URI to `Client`
without a terminal `/api`; the SDK owns and appends that path segment. Keep any
other Cloud runtime path prefix exactly as provided.

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

The preferred service-mode API discovers handler contracts from ordinary PHP
classes. Attributes name the server contract while method signatures describe
its arguments. Yielding a command from `WorkflowContext` creates a durable
step; replay sends the recorded value back into the generator without repeating
the external work.

```php
<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreeterWorkflow
{
    #[Workflow('greeter')]
    public function run(WorkflowContext $context, string $name): Generator
    {
        $greeting = yield $context->activity('greet', [$name]);

        return ['greeting' => $greeting];
    }

    #[Query]
    public function status(QueryContext $context): array
    {
        return ['events' => count($context->history)];
    }

    #[Signal('set-language')]
    public function setLanguage(string $language): void
    {
        // This declaration is reflected for admission; run() consumes signals.
    }

    #[Update]
    public function rename(QueryContext $context, string $name): string
    {
        return $name;
    }
}

final class GreetingActivities
{
    #[Activity]
    public function greet(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

$client = new Client('http://server:8080', token: 'dev-token-123');

Worker::create($client, 'php-workers')
    ->register(GreeterWorkflow::class, GreetingActivities::class)
    ->run();
```

`register()` resolves class names once and validates every attributed method
before registration or polling. With no container, concrete classes with no
required constructor arguments are instantiated automatically. Pass any PSR-11
`ContainerInterface` as the third argument to `Worker::create()` when handlers
have application dependencies. Pass a PSR-3 `LoggerInterface` with the named
`logger` argument; lifecycle, retry, shutdown, and handler failures then use the
application's normal logging pipeline. The optional `diagnosticListener`
receives the same event names and structured context.

Signal methods are signature declarations for server admission and are not
invoked. The workflow reads their committed values with
`$context->signals('set-language')` during replay. Query and update methods are
executed with immutable `QueryContext` state.

The callable registration methods remain the intentional low-level escape
hatch. For example, replay-consumed signals can be declared directly:

```php
$worker->declareSignal(
    'counter',
    'increment',
    static fn (int $amount): mixed => null,
);
```

Call `$context->heartbeat($details)` from a long-running activity. It throws
`ActivityCancelled` when the server requests cancellation. `Worker::run()`
installs SIGINT/SIGTERM handlers when `pcntl` is available, stops accepting new
tasks, and lets the active synchronous task settle before returning. The
managed worker also returns when any task poll reports a terminal typed outcome
such as `stale_worker_registration`, `draining`, or `stopped`; empty and timeout
polls remain idle. Registration also negotiates the worker heartbeat cadence.
Managed long polls are bounded by that cadence and heartbeat checks run between
workflow, activity, and query polls, so an idle polling cycle cannot silently
consume the server's registration freshness window. Invalid advertised cadence
values leave the worker's configured safe fallback in effect.

Low-level worker integrations can call `pollWorkflowTaskResponse()`,
`pollActivityTaskResponse()`, and `pollQueryTaskResponse()` to receive the
complete server envelope, including `poll_status`, `reason`, protocol metadata,
and any future fields. The existing task-only poll methods delegate to these
response methods and still return the leased task or `null`. Use
`DurableWorkflow\Worker\PollResponse::isTerminal()` to apply the same typed
terminal-outcome classification as the managed worker.

When a poll fails with a complete worker-protocol envelope explicitly marked as
transient, the managed worker retries that same poll with capped backoff while
keeping heartbeats and graceful shutdown responsive. Pass a
`transientPollRetryObserver` callback to the `Worker` constructor to record the
task kind, consecutive attempt, selected delay, and typed server exception.
Authentication failures, malformed responses, and generic server errors remain
fatal.

## Test workflow code and interactions

The testing namespace has no PHPUnit dependency. Its assertions throw
`DurableWorkflow\Testing\AssertionFailed`, so they work with PHPUnit, Pest, or
plain PHP. Application services can type their dependency as
`WorkflowClientInterface`; both the network `Client` and `WorkflowClientFake`
implement that interface and return handles with the same interaction methods.

```php
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Testing\WorkflowClientFake;

$worker = Worker::create($client, 'php-workers')
    ->register(GreeterWorkflow::class, GreetingActivities::class);
$handlers = new WorkerTestHarness($worker);
$handlers->assertWorkflowEmits('greeter', 'schedule_activity', ['Ada']);
$handlers->assertActivityResult('greet', 'hello, Ada', ['Ada']);
$handlers->assertQueryResult('greeter', 'status', ['events' => 0]);
$handlers->assertUpdateResult('greeter', 'rename', 'Grace', ['Grace']);
$handlers->assertRegistered('signal', 'set-language', 'greeter');

$workflows = (new WorkflowClientFake())
    ->setQueryResult('greeting-1', 'status', 'running')
    ->setUpdateResult('greeting-1', 'rename', 'accepted')
    ->setWorkflowResult('greeting-1', ['greeting' => 'hello, Ada']);
$handle = $workflows->startWorkflow('greeter', 'greeting-1', 'php-workers', ['Ada']);
$handle->signal('set-language', ['en']);
$handle->query('status');
$handle->update('rename', ['Grace']);
$handle->result();

$workflows->assertWorkflowStarted('greeter', ['Ada']);
$workflows->assertSignalSent('greeting-1', 'set-language', ['en']);
$workflows->assertQueryRequested('greeting-1', 'status');
$workflows->assertUpdateRequested('greeting-1', 'rename', ['Grace']);
$workflows->assertResultRequested('greeting-1');
```

Workflow tasks additionally require an acknowledged lease renewal before user
code runs. Typed transient renewal pressure is retried with the original task
ID, attempt, and lease owner; shutdown or a terminal/lost lease prevents task
execution and completion.

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
