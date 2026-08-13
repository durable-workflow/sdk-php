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
  endpoint: '%env(DURABLE_WORKFLOW_ENDPOINT)%'
  namespace: '%env(DURABLE_WORKFLOW_NAMESPACE)%'
  task_queue: '%env(DURABLE_WORKFLOW_TASK_QUEUE)%'
  credentials:
    control_token: '%env(default::DURABLE_WORKFLOW_CONTROL_TOKEN)%'
    worker_token: '%env(default::DURABLE_WORKFLOW_WORKER_TOKEN)%'
```

Use a bare self-hosted Server origin or the complete provisioned Cloud namespace runtime URI. Inject the control token only into application processes and the worker token only into the Console worker. A local Server can instead use one `credentials.token` value sourced from `DURABLE_WORKFLOW_TOKEN`.

## 3. Let attributes autoconfigure handlers

Symfony's normal autoconfigured `src/` import recognizes the SDK attributes. This is the primary path; do not also list the same services under `durable_workflow.handlers`.

```php
namespace App\Workflow;

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\WorkflowContext;

final class FulfillOrderWorkflow
{
    public function __construct(private InventoryGateway $inventory) {}

    #[Workflow('orders.fulfill')]
    public function run(WorkflowContext $context, string $orderId): mixed
    {
        return $context->activity('orders.reserve', [$orderId]);
    }
}
```

Add an autowired activity service with `#[Activity('orders.reserve')]`. The Bundle tags both services during container compilation, and the worker resolves their ordinary constructor dependencies. Use the explicit `handlers` list only for classes outside Symfony's autoconfigured imports.

## 4. Inspect, then run the Console worker

```bash
php bin/console debug:container DurableWorkflow\\WorkflowClientInterface
php bin/console debug:container --tag=durable_workflow.handler

env -u DURABLE_WORKFLOW_CONTROL_TOKEN \
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

<div class="note"><strong>Role boundary</strong><p>Application code receives the public control client. The worker factory owns a private worker client, so a scoped credential is not accidentally reused across roles.</p></div>
