---
layout: layout.njk
title: Symfony
description: Use the first-party Durable Workflow Symfony Bundle, attributed autowired handlers, Console worker, and testing helper with Cloud or Server.
lead: "The SDK's Bundle keeps the Symfony experience native: environment-backed configuration, attribute autoconfiguration, autowired clients and handlers, one Console worker, PSR logging, and framework test support."
previous:
  label: Laravel
  url: /frameworks/laravel/
next:
  label: Deployment
  url: /operate/deployment/
---
## 1. Install and enable the Bundle

```bash
{{ release.composerCommand }}
```

Register the Bundle in `config/bundles.php`:

```php
return [
    // ...
    DurableWorkflow\Bridge\Symfony\DurableWorkflowBundle::class => ['all' => true],
];
```

The package supports the Symfony versions declared by this SDK release. No third-party Bundle or raw callback command is required.

## 2. Configure the runtime and roles

```yaml
# config/packages/durable_workflow.yaml
durable_workflow:
  endpoint: '%env(DURABLE_WORKFLOW_RUNTIME_URL)%'
  namespace: '%env(DURABLE_WORKFLOW_NAMESPACE)%'
  task_queue: '%env(DURABLE_WORKFLOW_TASK_QUEUE)%'
  credentials:
    control_token: '%env(default::DURABLE_WORKFLOW_CLIENT_TOKEN)%'
    worker_token: '%env(default::DURABLE_WORKFLOW_WORKER_TOKEN)%'
```

Use a bare self-hosted Server origin or the complete provisioned Cloud namespace runtime URI. Inject the control token only into application processes and the worker token only into the Console worker. A local Server can instead use one `credentials.token` value sourced from `DURABLE_WORKFLOW_TOKEN`.

## 3. Let attributes autoconfigure handlers

Symfony's normal autoconfigured `src/` import recognizes the SDK attributes. This is the primary path; do not also list the same services under `durable_workflow.handlers`.

```php
namespace App\Workflow;

use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\WorkflowContext;

final class FulfillOrderWorkflow
{
    public function __construct(private InventoryGateway $inventory) {}

    #[Workflow('orders.fulfill')]
    public function run(WorkflowContext $context, string $orderId): mixed
    {
        $released = $context->waitCondition(
            static fn (): bool => $context->signals('orders.release') !== [],
            key: 'inventory-release',
            timeout: 300,
        );
        if (!$released) {
            return ['status' => 'timed_out'];
        }

        return $context->activity('orders.reserve', [$orderId]);
    }

    #[Signal('orders.release')]
    public function release(): void
    {
        // The Fiber re-evaluates the predicate from committed signal history.
    }
}
```

Add an autowired activity service with `#[Activity('orders.reserve')]`. The Bundle tags both services during container compilation, and the worker resolves their ordinary constructor dependencies. Use the explicit `handlers` list only for classes outside Symfony's autoconfigured imports.

Autowired workflow services use the same ordered barrier for concurrent work:

```php
[$reservation, $pricing] = $context->parallel([
    static fn () => $context->activity('orders.reserve', [$orderId]),
    static fn () => $context->childWorkflow('orders.price', [$orderId]),
]);

return compact('reservation', 'pricing');
```

Both leaves are scheduled before the Fiber suspends. Symfony's container continues to resolve handler dependencies, while PSR logs and `WorkerDiagnosticEvent` report failures and the durable group/path identity through the normal Bundle integration.

### Upgrade an autowired workflow safely

Version decisions remain ordinary `WorkflowContext` calls inside the autowired service:

```php
$version = $context->getVersion('symfony-inventory-reservation', -1, 1);

return $version === -1
    ? $context->activity('orders.reserve-legacy', [$orderId])
    : $context->activity('orders.reserve', [$orderId]);
```

Deploy the bridge branch first, then raise the maximum in a later worker build. Each run keeps its recorded decision across process replacement and cold replay. `patched()` provides the same `-1` legacy / `1` patched rollout as a boolean, and `deprecatePatch()` keeps that marker in place after removing the conditional. Keep the change ID stable and do not switch one ID between `getVersion()` and the patch helpers.

## 4. Inspect, then run the Console worker

```bash
php bin/console debug:container DurableWorkflow\\WorkflowClientInterface
php bin/console debug:container --tag=durable_workflow.handler

env -u DURABLE_WORKFLOW_CLIENT_TOKEN \
  DURABLE_WORKFLOW_WORKER_TOKEN="$WORKER_SECRET" \
  php bin/console durable-workflow:worker
```

<div class="outcome"><strong>Framework checkpoint</strong><p>The container lists attributed handler services, and the command reports the configured task queue before it polls. The command requires <code>ext-pcntl</code> so signals always enter the managed shutdown path.</p></div>

## Supervise the built-in command

```ini
[program:durable-workflow-orders]
command=php /srv/app/bin/console durable-workflow:worker
directory=/srv/app
autostart=true
autorestart=true
stopasgroup=true
killasgroup=true
stopsignal=TERM
stopwaitsecs=45
```

Use Messenger for ingress that calls `WorkflowClientInterface`; do not put the worker's polling loop inside a Messenger message. PSR logs and `WorkerDiagnosticEvent` expose lifecycle, retry, handler failure, and shutdown diagnostics.

## Test through the Symfony helper

In a `KernelTestCase`, use `InteractsWithDurableWorkflow::fakeDurableWorkflow()` to replace the autowired interface with `WorkflowClientFake`, arrange results, and assert workflow interactions without a network runtime.

The Symfony fake continues to cover application starts and handles. For the workflow handler itself, resolve the worker through the test container and use `WorkerTestHarness::runWorkflow()` to assert that every parallel leaf is emitted in one command sequence. Replay recorded mixed-order or partial group history to exercise ordered results and restart behavior without starting a Server.

<div class="note"><strong>Role boundary</strong><p>Application code receives the public control client. The worker factory owns a private worker client, so a scoped credential is not accidentally reused across roles.</p></div>
