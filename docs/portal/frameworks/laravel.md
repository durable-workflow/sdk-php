---
layout: layout.njk
title: Laravel adoption
description: Move a Laravel Workflow v1 application to Durable Workflow 2.0 embedded or service mode without changing Laravel merely to change runtime ownership.
lead: Keep the Laravel application experience. Choose who owns durable state first, then migrate starts and open runs deliberately.
previous:
  label: Testing
  url: /build/testing/
next:
  label: Symfony
  url: /frameworks/symfony/
---
## Choose the ownership model first

<div class="choice-grid">
  <div class="choice"><h3>Embedded Laravel</h3><p><code>durable-workflow/workflow</code> stores history in the application database and executes through Laravel queues. Choose it when the Laravel deployment should own durable state, backups, migrations, and workers.</p></div>
  <div class="choice"><h3>Cloud or Server</h3><p><code>durable-workflow/sdk</code> connects the same Laravel application structure to a separately operated namespace. The Laravel bridge keeps container resolution, configuration, Artisan, logging, events, and testing.</p></div>
</div>

The service-mode bridge supports Laravel 9, 10, 11, 12, and 13 on the PHP
versions supported by the SDK. Those are the same Laravel majors supported by
embedded v2, so changing state ownership does not force a Laravel upgrade.

Do not install the two packages as interchangeable storage adapters. They have
different authoritative histories, worker lifecycles, queue boundaries,
backups, and rollback procedures.

### Application call sites stay class-shaped

Changing the state owner does not turn application services into protocol
bootstrap code. The minimal start remains a class plus ordinary input:

| Mode | Container-resolved application call |
| --- | --- |
| Stable v1 embedded | `WorkflowStub::make(GreetingWorkflow::class)->start('Ada')` |
| v2 embedded | `WorkflowStub::make(GreetingWorkflow::class)->start('Ada')` |
| v2 service | `$workflows->start(GreetingWorkflow::class, ['Ada'])` |

The service form injects `LaravelWorkflowClientInterface` as `$workflows`. It
derives `laravel.greeting` from the workflow service's attribute and the queue
from `config/durable-workflow.php`; application code supplies those strings only
when it intentionally overrides the Laravel defaults.

## Supported transition matrix

| Starting point | Destination | State owner after the change | Open-run choices | History rule |
| --- | --- | --- | --- | --- |
| Laravel Workflow v1 | v2 embedded | Laravel application | Drain v1, or let v1 and v2 coexist while new starts use v2 | Open v1 history stays on the v1 engine |
| Laravel Workflow v1 | v2 service mode | Server or Cloud namespace | Drain v1, or keep v1 running while new starts go to service mode | Moving history requires an explicit migration; installing the SDK does not move it |
| v2 embedded | v2 service mode | Server or Cloud namespace | Drain embedded runs, or operate both runtimes with explicit ingress routing | Export is evidence and backup, not an automatic import into Server or Cloud |

The public [Laravel adoption contract](/laravel-adoption-contract.json) is the
machine-readable authority for this matrix, supported Laravel/PHP cells,
continuity surfaces, and qualification destinations. Its embedded transition
row references the transition manifest shipped by `durable-workflow/workflow`,
which owns the embedded support policy.

## Plan continuity before changing packages

Record this cutover sheet for every workflow family:

| Surface | Decision to record |
| --- | --- |
| Workflow type | Map the old class/type to one explicit v2 type. Keep that type stable after service workers register it. |
| Workflow ID | Route each ID to exactly one authoritative runtime. Keep a separate business correlation ID when an old run and replacement run must coexist. |
| Task queue | Change the starter and worker together. A matching queue name in two runtimes does not connect those runtimes. |
| Payload | Translate old serialized inputs to the v2 contract and validate the Avro-compatible shape before the first new start. |
| Memo and search metadata | Recreate required fields on new service-mode starts; application database columns do not become namespace search attributes. |
| Signals and updates | Pause ingress, let accepted messages settle, switch the ID-to-runtime route, then resume. Do not broadcast a message to both owners. |
| Rollback | Roll back new starts per runtime. Never point an older runtime at history created by a newer or different owner. |

Take a database backup and capture the list of open workflow IDs before every
transition. Keep the old worker deployment available until its open-run policy
has completed.

## Path A: v1 to v2 embedded

This path keeps state ownership in Laravel.

1. Install the v2 channel in the existing application and run its migrations:

   ```bash
   composer require 'durable-workflow/workflow:^2.0' --with-all-dependencies
   php artisan migrate --force
   ```

2. Inventory the v1 executions that still own history:

   ```bash
   php artisan workflow:v1:list --json
   ```

3. Choose one policy:

   - Drain: stop new v1 starts and messages, leave v1 workers running until the
     command reports no open runs, then start v2 traffic.
   - Coexist: leave open runs on the v1 engine and route only new IDs to v2.
     Keep the routing record until the final v1 run closes.

4. Author v2 workflow and activity classes under the v2 API, use a stable type
   mapping, configure a real Laravel queue driver, and start one canary with a
   new workflow ID. Inspect its result before widening traffic.

5. Recreate memo/search fields and message routing for new v2 runs. A v1
   serialized payload or signal row is not a v2 history event.

Rollback means stopping new v2 starts and restoring the prior application
deployment while v1 still owns its original histories. Runs already created by
v2 remain v2 runs; do not make v1 workers consume them.

## Path B: v1 directly to v2 service mode

This path changes state ownership without first adopting the embedded v2 API.
The SDK can be installed while v1 remains present and draining.

1. Install the service-mode channel and publish configuration:

   ```bash
   composer require 'durable-workflow/sdk:^2.0'
   php artisan vendor:publish --tag=durable-workflow-config
   ```

2. Add attributed service-mode workflow and activity classes, register them in
   `config/durable-workflow.php`, and qualify them with the Laravel fake shown
   below.

3. Provision a Server or Cloud namespace. Set its runtime URI, namespace, and
   task queue, then cache configuration without credentials:

   ```bash
   export DURABLE_WORKFLOW_RUNTIME_URL='https://runtime.example'
   export DURABLE_WORKFLOW_NAMESPACE='orders'
   export DURABLE_WORKFLOW_TASK_QUEUE='orders-service'

   env -u DURABLE_WORKFLOW_TOKEN \
     -u DURABLE_WORKFLOW_CLIENT_TOKEN \
     -u DURABLE_WORKFLOW_WORKER_TOKEN \
     php artisan config:cache
   ```

4. Start the worker with only its worker credential. Start one canary through
   the injected application client with only its client credential:

   ```bash
   env -u DURABLE_WORKFLOW_CLIENT_TOKEN \
     DURABLE_WORKFLOW_WORKER_TOKEN="$WORKER_SECRET" \
     php artisan durable-workflow:worker

   env -u DURABLE_WORKFLOW_WORKER_TOKEN \
     DURABLE_WORKFLOW_CLIENT_TOKEN="$CLIENT_SECRET" \
     php artisan app:start-greeting greeting-canary Ada
   ```

5. Drain v1 or maintain an explicit ID-to-runtime routing table. Switch starts,
   signals, queries, and updates together; then inspect the service-mode result.

Existing v1 history remains in the Laravel database. Reusing a type name or
workflow ID in the service namespace does not copy that history. For rollback,
stop new service starts, keep service workers available for runs they already
own, and route only new IDs back to v1. A service run cannot resume from a v1
history row.

## Path C: v2 embedded to v2 service mode

The worker code can become Laravel service classes, but the history does not
move with the class name.

1. Inventory open embedded v2 runs and export important histories for audit or
   rollback evidence:

   ```bash
   php artisan workflow:v2:history-export <workflow-id> \
     --pretty --output=storage/app/workflow-history/<workflow-id>.json
   ```

2. Install and configure the SDK bridge using Path B. Preserve public workflow
   type names where their payload contracts remain compatible, and choose a
   service task queue that the starter and Artisan worker share.

3. Drain open embedded runs, or keep embedded and service runtimes active with
   an explicit owner for every workflow ID. Pause message ingress while that
   owner changes.

4. Start new service runs with translated payloads plus the required memo and
   search attributes. Inspect their result through the injected client.

The embedded history export is not a Server or Cloud import command. Moving an
authoritative open history requires a separately qualified migration procedure;
it is never implied by `composer remove` or `composer require`.

Rollback is also per runtime: stop new service starts and send new IDs back to
embedded v2, while service workers finish or explicitly terminate the runs that
the service namespace already owns.

## Build the service-mode form as Laravel services

Both handler classes are resolved from Laravel's container. Constructor
injection is ordinary application code. Workflow and activity bodies use the
same straight-line v2 authoring model; there is no generator/yield ceremony or
standalone bootstrap script:

```php
namespace App\Workflows;

use App\GreetingPrefix;
use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

final class GreetingWorkflow
{
    public function __construct(private GreetingPrefix $prefix) {}

    #[Workflow('laravel.greeting')]
    public function run(WorkflowContext $context, string $name): string
    {
        if ($this->prefix->value === '') {
            throw new \LogicException('Greeting configuration is required.');
        }

        $approved = $context->waitCondition(
            static fn (): bool => $context->signals('approve-greeting') !== [],
            key: 'greeting-approved',
            timeout: 300,
        );
        if (!$approved) {
            return 'Greeting approval timed out.';
        }

        return $context->activity('laravel.greet', [$name]);
    }

    #[Signal('approve-greeting')]
    public function approveGreeting(): void
    {
        // The declaration is admitted by the worker; run() reads committed signal history.
    }
}

final class GreetingActivity
{
    public function __construct(private GreetingPrefix $prefix) {}

    #[Activity('laravel.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "{$this->prefix->value}, {$name}";
    }
}
```

Register both classes in the published configuration:

```php
// config/durable-workflow.php
'runtime_url' => env('DURABLE_WORKFLOW_RUNTIME_URL', 'http://localhost:8080'),
'namespace' => env('DURABLE_WORKFLOW_NAMESPACE', 'default'),
'task_queue' => env('DURABLE_WORKFLOW_TASK_QUEUE', 'orders-service'),
'handlers' => [
    App\Workflows\GreetingWorkflow::class,
    App\Workflows\GreetingActivity::class,
],
```

An empty handler list is a startup error. The worker resolves every configured
class through the Laravel container before polling. Dependencies read by a
workflow must be deterministic and stable across replay; put database, network,
clock, and other side-effecting dependencies in activities.

Container resolution is unchanged for a concurrent journey. A workflow service can fan out without giving up constructor injection or straight-line Fiber code:

```php
#[Workflow('laravel.customer-summary')]
public function summary(WorkflowContext $context, string $customerId): array
{
    [$customer, $recommendations] = $context->all([
        static fn () => $context->activity('laravel.customer', [$customerId]),
        static fn () => $context->childWorkflow('laravel.recommendations', [$customerId]),
    ]);

    return compact('customer', 'recommendations');
}
```

The same is true for compensated journeys. Inject application configuration
into the attributed workflow and activity services, then create the saga from
the workflow context. Compensation handlers remain ordinary attributed
activities and may be routed to another language worker by activity type:

```php
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;

final class CompensatedTripWorkflow
{
    public function __construct(private TripPolicy $policy) {}

    #[Workflow('laravel.compensated-trip')]
    public function run(WorkflowContext $context, string $tripId): array
    {
        return $context->saga()->run(function (Saga $saga) use ($context, $tripId): array {
            $flight = $context->activity($this->policy->reserveFlightType, [$tripId]);
            $saga->addCompensation('python.cancel-flight', [$flight]);

            $hotel = $context->activity($this->policy->reserveHotelType, [$tripId]);
            $saga->addCompensation('python.cancel-hotel', [$hotel]);

            $context->activity($this->policy->chargeType, [$tripId]);

            return compact('flight', 'hotel');
        });
    }
}
```

The Artisan worker reports a terminal compensation failure through the normal
`worker.handler_failed` PSR log and `WorkerDiagnosticEvent`. Its
`saga_failure` context names the failed forward step, failed compensation,
registration order, messages, and exception types.

Laravel's fake keeps the class-shaped start and assertion for this journey:

```php
$fake = DurableWorkflow::fake()->setWorkflowResult('trip-test', [
    'status' => 'compensated',
]);

$result = DurableWorkflow::start(
    CompensatedTripWorkflow::class,
    ['trip-1'],
    workflowId: 'trip-test',
)->result();

$fake->assertWorkflowStarted(
    CompensatedTripWorkflow::class,
    ['trip-1'],
    workflowId: 'trip-test',
);
```

Use `WorkerTestHarness` with the container-built worker when a test needs to
drive committed forward-failure and compensation history. Use the client fake
for application-level starts, handles, and result assertions.

Laravel still resolves the workflow, activity, and child-handler services from the application container. The Artisan worker emits the usual PSR log records and `WorkerDiagnosticEvent` events while group/path metadata remains visible in Server or Cloud diagnostics.

### Upgrade a Laravel workflow without losing open runs

Container resolution does not change the version-marker contract. Keep constructor injection as-is and put the decision inside the straight-line workflow method:

```php
$format = $context->getVersion('laravel-greeting-format', -1, 1);

return $format === -1
    ? $context->activity('laravel.greet-legacy', [$name])
    : $context->activity('laravel.greet', [$name]);
```

Deploy that bridge before changing the branch. A replacement worker may raise the maximum for new runs, while open runs reuse their recorded value during cold replay. Use `$context->patched('require-greeting-approval')` for a boolean rollout and later `$context->deprecatePatch('require-greeting-approval')` when the conditional branch is removed. Laravel logging and `WorkerDiagnosticEvent` report a typed nondeterminism failure with the change ID if a deployment drops a recorded value from its supported range.

## Inject the Laravel client and inspect the result

`LaravelWorkflowClientInterface` derives the protocol workflow type from the
configured service class's `#[Workflow]` method and uses the configured task
queue. Controllers, commands, listeners, and queued jobs stay class-shaped:

```php
use App\Workflows\GreetingWorkflow;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;

final class StartGreeting
{
    public function __construct(private LaravelWorkflowClientInterface $workflows) {}

    public function __invoke(string $workflowId, string $name): string
    {
        return $this->workflows->start(
            GreetingWorkflow::class,
            [$name],
            workflowId: $workflowId,
        )->result();
    }
}
```

Omit `workflowId` to generate a new ID. Pass `WorkflowStartOptions` when an
application needs a queue override, timeouts, duplicate policy, memo, search
attributes, priority, fairness, or build ID. `handle(GreetingWorkflow::class,
$workflowId)` returns the existing workflow handle after validating the same
configured attributed class. The lower-level `WorkflowClientInterface` and
`Client` bindings remain available when a cross-language contract intentionally
uses explicit string types and queues. Prefer `WorkflowClientInterface` for
normal dependency injection: both documented interfaces defer application
credential resolution until their first client operation, so services may be
discovered during worker-only Artisan bootstrap. Direct `Client` injection is
the explicit eager surface and validates the application credential as soon as
Laravel resolves it.

Expose that injected action through an ordinary application command:

```php
// routes/console.php
use Illuminate\Support\Facades\Artisan;

Artisan::command('app:start-greeting {id} {name}', function (): void {
    $this->line(app(\App\Actions\StartGreeting::class)(
        $this->argument('id'),
        $this->argument('name'),
    ));
});
```

Worker diagnostics use Laravel's PSR logger and dispatch
`WorkerDiagnosticEvent` through Laravel events. Configuration contains no
credentials; the provider resolves the self-hosted shared token or scoped
client token on the first operation through either application interface, and
resolves the worker token only when it constructs the worker factory.
After registration, Artisan prints a registered-and-polling line naming the
runtime host, namespace, task queue, registered workflow and activity types, and
credential role. Compare that line with the starter's explicit workflow type and
task queue before treating a pending run as a runtime problem.

## Test through the Laravel fake

```php
use DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow;

$workflows = DurableWorkflow::fake()
    ->setWorkflowResult('greeting-1001', 'hello from Laravel, Ada');

$result = app(StartGreeting::class)('greeting-1001', 'Ada');

$this->assertSame('hello from Laravel, Ada', $result);
$workflows->assertWorkflowStarted(
    GreetingWorkflow::class,
    ['Ada'],
    workflowId: 'greeting-1001',
);
$workflows->assertResultRequested('greeting-1001');
```

The fake replaces both the class-shaped Laravel interface and its low-level
transport, then records and asserts the same service-class call made by the
application. It works with PHPUnit, Pest, or a plain Laravel test and needs
neither a running Server nor source inspection.

Use the same fake assertions for a concurrent application start; concurrency is an implementation detail of the registered handler, so the class-shaped start contract does not change. For handler-level coverage, build the registered Laravel worker in the test container and inspect `WorkerTestHarness::runWorkflow()`: every declared leaf must appear in the same command sequence with one group/path identity. Replay mixed-order or partial history through the same method to assert declaration-order results without starting a Server.

## Qualification boundary

Clean compatibility jobs install this SDK in every supported Laravel/PHP cell
and run the same container, credential, config-cache, diagnostics,
workflow/activity, and fake smoke. Separate clean applications begin with the
maintained stable v1 and embedded v2 Workflow package channels, retain an open
embedded-owned run, add the SDK, and prove a new service-mode start with the same
injected application dependency. Embedded behavior is qualified in the Workflow
package against the local Laravel runtime. Service-mode release qualification
must use published SDK artifacts against both the current standalone Server and
managed Cloud; source-only or standalone-script execution is not bridge
evidence.
