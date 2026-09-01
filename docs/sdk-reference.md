# PHP SDK Reference

This is the detailed repository reference for the first-party PHP client and
worker SDK. Start with the [root README](../README.md) or the
[PHP developer portal](https://php.durable-workflow.com/) for the shortest
runnable path.

## Choose the PHP execution model

- **Laravel adoption:** start with the [ownership-first transition
  guide](https://php.durable-workflow.com/frameworks/laravel/) when moving a
  Laravel Workflow v1 application to v2 embedded or service mode, or when
  moving embedded v2 to service mode. Laravel 9 through 13 are supported on
  both v2 destinations.
- **Plain PHP service mode:** follow the quickstart below when a framework-neutral
  application and remote worker connect to Durable Workflow Cloud or a
  self-hosted Server.
- **Symfony service mode:** use the [Symfony bridge](#symfony-service-mode) from
  this SDK for autowired remote handlers and a managed console worker.
- **Embedded Laravel workflows:** use
  [`durable-workflow/workflow`](https://php.durable-workflow.com/frameworks/laravel/)
  when the Laravel application itself should own durable state and execute
  through Laravel queues. That is a different deployment model, not a
  prerequisite for this SDK.

## Plain PHP quickstart

Create an empty Composer project and install the current published package:

```bash
mkdir durable-php-quickstart
cd durable-php-quickstart
composer init --name=acme/durable-php-quickstart --no-interaction
composer require 'durable-workflow/sdk:^2.0'
```

The package declares its supported Server baseline in Composer metadata.

Contributors testing the current source branch can install it directly:

```bash
composer config repositories.durable-workflow-sdk vcs https://github.com/durable-workflow/sdk-php
composer require durable-workflow/sdk:dev-main
```

The SDK uses the official [`apache/avro`](https://packagist.org/packages/apache/avro)
package for schema parsing and binary payload encoding. Guzzle is included as
the default PSR-18 transport; any PSR-18 client and PSR-17 factories can be
injected instead.

### Choose Cloud or Server without rewriting the URL

Set one runtime URI exactly as provisioned:

```bash
# Self-hosted Server: pass the bare origin. The SDK appends one /api segment.
export DURABLE_WORKFLOW_RUNTIME_URL='http://localhost:8080'
export DURABLE_WORKFLOW_NAMESPACE='default'

# Durable Workflow Cloud: instead use both values returned by provisioning.
# export DURABLE_WORKFLOW_RUNTIME_URL='https://cloud.example/api/runtime/v1/namespaces/<runtime-id>'
# export DURABLE_WORKFLOW_NAMESPACE='<provisioned-namespace>'

export DURABLE_WORKFLOW_TASK_QUEUE="php-quickstart-$(php -r 'echo bin2hex(random_bytes(8));')"
```

The Cloud URL already contains `/api/runtime/v1/namespaces/...`; do not trim
that prefix or replace it with the Cloud control-plane URL. Keep the separately
provisioned Cloud namespace value unchanged as well. The SDK appends its
endpoint `/api` after the namespace runtime URI. For Server, pass an origin such
as `http://localhost:8080`, not `http://localhost:8080/api`, so the request path
contains one `/api` segment rather than two.

Inject credentials through the process environment or a secret manager. Client
operations and worker polling are separate roles, so keep their variables
separate even when a development Server is configured with one shared token:

```bash
read -rsp 'Client credential: ' DURABLE_WORKFLOW_CLIENT_TOKEN; echo
export DURABLE_WORKFLOW_CLIENT_TOKEN
read -rsp 'Worker credential: ' DURABLE_WORKFLOW_WORKER_TOKEN; echo
export DURABLE_WORKFLOW_WORKER_TOKEN
```

The prompts do not echo values. Do not put these exports in source files,
commit an `.env` file, or print either value in diagnostics.

### Create the three example files

`bootstrap.php` resolves Composer consistently when the example is in a clean
project, this SDK checkout, an installed SDK package, or a Sample App
playground/container that copies the files beside its own `vendor/` directory.

<!-- docs-example id="php.quickstart.bootstrap" -->
```php
<?php

declare(strict_types=1);

(static function (): void {
    $candidates = array_unique([
        __DIR__.'/vendor/autoload.php',
        dirname(__DIR__).'/vendor/autoload.php',
        dirname(__DIR__, 2).'/vendor/autoload.php',
        dirname(__DIR__, 3).'/autoload.php',
        getcwd().'/vendor/autoload.php',
    ]);

    foreach ($candidates as $candidate) {
        if (is_file($candidate)) {
            require $candidate;

            return;
        }
    }

    throw new RuntimeException(
        'Composer autoload.php was not found. Run the example from a Composer project or copy all three example files beside vendor/.',
    );
})();

function quickstartEnvironment(string $name): string
{
    $value = getenv($name);
    if (!is_string($value) || trim($value) === '') {
        throw new RuntimeException("Set the {$name} environment variable before running this example.");
    }

    return trim($value);
}
```

`worker.php` keeps `#[Workflow]`, `#[Activity]`, and the `register()` call in one
visible path. The attributes provide the public type names that Server admits.

<!-- docs-example id="php.quickstart.worker" -->
```php
<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreeterWorkflow
{
    #[Workflow('quickstart.php.greeter')]
    public function run(WorkflowContext $context, string $name): array
    {
        $greeting = $context->activity('quickstart.php.greet', [$name]);

        return ['greeting' => $greeting];
    }
}

final class GreetingActivities
{
    #[Activity('quickstart.php.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN'),
);

Worker::create($client, quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'))
    ->register(GreeterWorkflow::class, GreetingActivities::class)
    ->run();
```

`client.php` starts a unique workflow and waits for its completed result.

<!-- docs-example id="php.quickstart.client" -->
```php
<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Client;

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);

$workflowId = 'php-quickstart-'.bin2hex(random_bytes(16));
$handle = $client->startWorkflow(
    workflowType: 'quickstart.php.greeter',
    workflowId: $workflowId,
    taskQueue: quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'),
    input: ['PHP'],
);

$result = $handle->result(timeoutSeconds: 90, pollIntervalSeconds: 1);

echo json_encode(
    ['workflow_id' => $workflowId, 'result' => $result],
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
).PHP_EOL;
```

The files above ship under [`examples/`](../examples/). Copy all three into the
new project if you prefer not to create them from the visible blocks.

### Start the worker, workflow, and result read

Run the worker in the first terminal with only its role credential:

```bash
env -u DURABLE_WORKFLOW_CLIENT_TOKEN php worker.php
```

In a second terminal with the same runtime, namespace, and task queue, start the
workflow and read the result with only the client credential:

```bash
env -u DURABLE_WORKFLOW_WORKER_TOKEN php client.php
```

The output contains a new workflow ID and this result shape:

```json
{"workflow_id":"php-quickstart-…","result":{"greeting":"hello, PHP"}}
```

Stop the worker with `Ctrl+C` after the client completes.

`register()` is the preferred class-oriented API: it discovers every attributed
handler in the supplied classes before polling. For generated handlers or other
callable-first code, the direct alternative is explicit and does not use
attributes:

```php
$worker = Worker::create($client, $taskQueue)
    ->registerWorkflow('quickstart.php.greeter', $workflowHandler)
    ->registerActivity('quickstart.php.greet', $activityHandler);
```

Both callables receive the same `WorkflowContext` and `ActivityContext` values
shown in the class-oriented example. Do not call `register()` with un-attributed
classes; use one complete registration style or the other.

The machine-readable [quickstart contract](quickstart-contract.json) records
the supported runtime URL forms, role-specific environment variables,
package-owned source files, and expected result. Documentation builds verify
that the visible examples still match those runnable files.

## Workflow handles and control-plane APIs

`WorkflowHandle` distinguishes the stable workflow instance from a selected
run. Its ordinary operations follow whichever run is current after a
continue-as-new transition. The `*SelectedRun()` methods retain the original
run guard and fail rather than silently targeting a successor.

## Control-plane administration and discovery

`Client::withNamespace()` returns an immutable namespace selection that keeps
the configured authentication and transport. Workflow payload encoding remains
Apache Avro. Workflow
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
Arguments use the official Apache Avro payload codec.

## Run a remote PHP worker

The preferred service-mode API discovers handler contracts from ordinary PHP
classes. Attributes name the server contract while method signatures describe
its arguments. Calls on `WorkflowContext` suspend the workflow's Fiber at each
durable step. Replay resumes that call with its recorded value, or throws its
recorded failure there, without repeating external work.

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
    public function run(WorkflowContext $context, string $name): array
    {
        $greeting = $context->activity('greet', [$name]);

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
have application dependencies.

Attributed workflow classes have a replay-scoped lifecycle. Registration
captures a clean handler template, then each workflow task replay, query, and
update runs on a fresh shallow clone. Mutable properties on the workflow object
therefore cannot cross workflow IDs, runs, or replay attempts, while
constructor-injected collaborators retain their configured identity. Keep
workflow-local mutable state directly on the handler; injected collaborators
are shared services and must not be used to hold execution-local state.
Workflow handler classes must remain cloneable.

Activity services have worker-scoped lifetimes instead: their resolved instance
is reused for activity tasks, so they can retain clients and other service
resources. The explicit `registerWorkflow()`, `registerQuery()`, and
`registerUpdate()` low-level methods also invoke the supplied callable as-is;
state captured by such a callable remains owned by the application. Use
attribute-based workflow registration when the SDK should provide replay-state
isolation.

Pass a PSR-3 `LoggerInterface` with the named `logger` argument; lifecycle,
retry, shutdown, and handler failures then use the application's normal logging
pipeline. The optional `diagnosticListener` receives the same event names and
structured context.

Signal methods are signature declarations for server admission and are not
invoked. The workflow reads their committed values with
`$context->signals('set-language')` during replay. Query and update methods are
executed with immutable `QueryContext` state.

The PHP SDK does not expose update-validator authoring. Worker registration
therefore declares an empty `update_validators` list for every workflow type;
capability discovery and Server admission must not infer validator parity from
the presence of ordinary update handlers.

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

## Receive repeated input with Message Streams

Inbound Message Streams deliver repeated, ordered application input to a stable
workflow instance. The application appends a stable message identity through
`WorkflowHandle::appendMessage()`, while workflow code consumes one message or
a bounded ordered batch through `messageStream()`. Runtime-owned cursors survive
replay, worker replacement, server restart, and continue-as-new.

See the task-oriented [Message Streams guide](https://php.durable-workflow.com/build/message-streams/)
and the shipped [client](../examples/message-stream-client.php) and
[worker](../examples/message-stream-worker.php) examples.

## Laravel service mode

Laravel 9 through 13 auto-discover the service provider from the same SDK package.
Publish the environment-backed configuration, add attributed handler services,
and start the supervised Artisan command:

```bash
composer require 'durable-workflow/sdk:^2.0'
php artisan vendor:publish --tag=durable-workflow-config
php artisan config:cache
php artisan durable-workflow:worker
```

Set `DURABLE_WORKFLOW_RUNTIME_URL` to a self-hosted Server origin or the complete
Cloud runtime base URI. Set `DURABLE_WORKFLOW_NAMESPACE` and
`DURABLE_WORKFLOW_TASK_QUEUE`, then choose shared-token or scoped authentication.
For scoped Cloud authentication, inject credentials at the process boundary:

| Laravel process | Inject | Do not inject |
| --- | --- | --- |
| Web, queue, or other application process | `DURABLE_WORKFLOW_CLIENT_TOKEN` | `DURABLE_WORKFLOW_WORKER_TOKEN` |
| `php artisan durable-workflow:worker` | `DURABLE_WORKFLOW_WORKER_TOKEN` | `DURABLE_WORKFLOW_CLIENT_TOKEN` |

The service provider gives the injectable application interfaces only the client
credential and creates a separate worker client only for the worker factory.
For a self-hosted deployment that uses one credential for both roles, inject
`DURABLE_WORKFLOW_TOKEN` instead. Supply secret values through the deployment
platform's process environment or secret store, not a generated configuration
file or a committed `.env` file.

The published configuration deliberately has no credential entries. Build and
deploy one cached configuration without any Durable Workflow credential in the
cache-building environment, then inject only the required role credential when
each application or worker process starts. Resolving either documented client
interface is credential-lazy; the provider resolves the client credential when
the interface performs its first application-client operation, after Laravel has
loaded the cache. Resolving `Client` directly is the explicit eager low-level
path and validates the application credential immediately.
Applications upgrading an older published configuration should republish it or
remove its `credentials` block before rebuilding the cache. List handler classes
in `config/durable-workflow.php`:

```php
'handlers' => [
    App\Workflows\GreeterWorkflow::class,
    App\Activities\GreetingActivities::class,
],
```

Laravel resolves every handler through its container, so ordinary constructor
injection works. Inject `LaravelWorkflowClientInterface` to start an attributed
workflow service class on the configured default queue; explicit IDs and
`WorkflowStartOptions` remain available. Inject `WorkflowClientInterface` for
low-level cross-language string contracts; like the Laravel-shaped interface,
it is safe to constructor-inject into services that may be discovered by a
worker-only Artisan process. Inject `Client` directly only when eager application
credential validation and its broader concrete API are intentional.
Worker diagnostics use Laravel's PSR logger and dispatch
`WorkerDiagnosticEvent` through Laravel events. The event name is available in
its `name` property and includes lifecycle, retry, handler-failure, and shutdown
events. After server registration, Artisan prints a registered-and-polling line
with the runtime host, namespace, queue, workflow and activity types, and
credential role; it never includes credential values.

In a Laravel test, `DurableWorkflow::fake()` replaces the class-shaped Laravel
client and its low-level transport. It returns `LaravelWorkflowClientFake` for
result setup and service-class interaction assertions:

```php
$workflows = DurableWorkflow::fake()
    ->setWorkflowResult('greeting-1', ['greeting' => 'hello, Ada']);

// Exercise application code, then use the framework-independent assertions.
$workflows->assertWorkflowStarted(GreeterWorkflow::class, ['Ada']);
```

## Symfony service mode

Symfony 6.4, 7, and 8 applications register the Bundle from the SDK package in
`config/bundles.php`:

```php
return [
    // ...
    DurableWorkflow\Bridge\Symfony\DurableWorkflowBundle::class => ['all' => true],
];
```

Configure Server or Cloud through environment processors. Attributed services
under Symfony's normal autoconfigured imports are registered as handlers. The
optional `handlers` list also registers classes outside those imports as
autowired services:

```yaml
# config/packages/durable_workflow.yaml
durable_workflow:
  endpoint: '%env(DURABLE_WORKFLOW_RUNTIME_URL)%'
  namespace: '%env(DURABLE_WORKFLOW_NAMESPACE)%'
  task_queue: '%env(DURABLE_WORKFLOW_TASK_QUEUE)%'
  credentials:
    control_token: '%env(default::DURABLE_WORKFLOW_CLIENT_TOKEN)%'
    worker_token: '%env(default::DURABLE_WORKFLOW_WORKER_TOKEN)%'
  handlers:
    - App\Workflow\GreeterWorkflow
    - App\Activity\GreetingActivities
```

Inject `DURABLE_WORKFLOW_CLIENT_TOKEN` only into web and other application
processes. Inject `DURABLE_WORKFLOW_WORKER_TOKEN` only into the process running
`php bin/console durable-workflow:worker`; the `default::` processors leave the
opposite scoped credential unset. The Bundle binds the public autowired client
to the client credential and gives its private worker client only the worker
credential. Self-hosted deployments can instead set `credentials.token` from
`DURABLE_WORKFLOW_TOKEN` and inject that shared credential into both
processes. Keep values in the deployment platform's environment or secret store,
not YAML, generated container files, or committed environment files.

Run `php bin/console durable-workflow:worker`. `Client` and
`WorkflowClientInterface` are public autowired services. Handler services retain
normal Symfony autowiring, worker messages use the standard PSR logger when it
is installed, and `WorkerDiagnosticEvent` is dispatched through Symfony's event
dispatcher under the diagnostic name. A `KernelTestCase` can use
`InteractsWithDurableWorkflow::fakeDurableWorkflow()` to replace the autowired
interface with the same assertion-capable fake used by plain PHP and Laravel.

Both console commands accept `--queue` and `--poll-timeout`. They require
`ext-pcntl` so SIGINT and SIGTERM always request a graceful worker shutdown.
Configuration errors, an unreachable endpoint, rejected credentials, and
worker-protocol or contract mismatches are reported with remediation specific
to the failing boundary. Neither bridge stores workflow state or installs the
embedded Laravel workflow engine.

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

See [`examples/`](../examples/), the authored
[PHP developer portal](https://php.durable-workflow.com/), the generated
[API reference](https://php.durable-workflow.com/api/), and
[`docs/protocol.md`](protocol.md) for the complete client, schedule,
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
