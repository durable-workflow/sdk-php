---
layout: layout.njk
title: Laravel
description: Use the first-party Durable Workflow Laravel bridge for Cloud or Server, or choose the embedded Laravel engine when the application owns durable state.
lead: Laravel has two deliberate first-party paths. Service mode uses this SDK's auto-discovered provider and Artisan worker; embedded mode uses Laravel queues and application-owned persistence.
previous:
  label: Testing
  url: /build/testing/
next:
  label: Symfony
  url: /frameworks/symfony/
---
## Choose the ownership model

<div class="choice-grid">
  <div class="choice"><h3>Cloud or Server</h3><p><code>durable-workflow/sdk</code> connects the Laravel application to a separately operated namespace. The SDK provides the service provider, configuration, worker command, container resolution, logging, events, and test fake.</p></div>
  <div class="choice"><h3>Embedded Laravel</h3><p><code>durable-workflow/workflow</code> stores history in the application database and executes through Laravel queues. Choose it when Laravel itself should own the runtime.</p></div>
</div>

Do not install both as interchangeable adapters. Their state ownership, worker lifecycle, backups, and migration boundaries differ.

## Service mode: configure before the worker starts

Laravel auto-discovers the provider from the published SDK. Follow this order in a clean application.

### 1. Install and publish configuration

```bash
{{ release.composerCommand }}
php artisan vendor:publish --tag=durable-workflow-config
```

The published `config/durable-workflow.php` reads the endpoint, namespace, and task queue from `DURABLE_WORKFLOW_*` environment values. It intentionally contains no credentials, so `config:cache` cannot serialize a secret.

### 2. Create attributed handler services

```php
namespace App\Workflows;

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\WorkflowContext;

final class FulfillOrderWorkflow
{
    #[Workflow('orders.fulfill')]
    public function run(WorkflowContext $context, string $orderId): array
    {
        $reservation = $context->activity('orders.reserve', [$orderId]);

        return ['order_id' => $orderId, 'reservation' => $reservation];
    }
}
```

Put the side-effecting method in an activity service with `#[Activity('orders.reserve')]`. Both classes remain ordinary Laravel services and can receive constructor-injected dependencies.

### 3. Register every handler

Edit the published file before launching the worker:

```php
// config/durable-workflow.php
'handlers' => [
    App\Workflows\FulfillOrderWorkflow::class,
    App\Activities\ReserveInventoryActivity::class,
],
```

An empty list is a startup error. The worker resolves these classes through Laravel's container and discovers their attributes before polling.

### 4. Cache non-secret settings

```bash
export DURABLE_WORKFLOW_ENDPOINT='http://localhost:8080'
export DURABLE_WORKFLOW_NAMESPACE='default'
export DURABLE_WORKFLOW_TASK_QUEUE='orders'

env -u DURABLE_WORKFLOW_TOKEN \
  -u DURABLE_WORKFLOW_CONTROL_TOKEN \
  -u DURABLE_WORKFLOW_WORKER_TOKEN \
  php artisan config:cache
```

For Cloud, set `DURABLE_WORKFLOW_ENDPOINT` to the complete provisioned namespace runtime URI. Do not append the SDK-owned `/api` suffix.

### 5. Start the first-party worker command

Inject only the worker role credential into the supervised process:

```bash
env -u DURABLE_WORKFLOW_CONTROL_TOKEN \
  DURABLE_WORKFLOW_WORKER_TOKEN="$WORKER_SECRET" \
  php artisan durable-workflow:worker
```

<div class="outcome"><strong>Framework checkpoint</strong><p>The command reports that it is starting on task queue <code>orders</code>. The published framework smoke creates handlers, publishes and configures the file, caches configuration without credentials, then launches this same command in that order.</p></div>

## Inject the application client

`Client` and `WorkflowClientInterface` are container bindings. Web, queue, command, and listener code should depend on the interface and receive only the control role credential:

```bash
env -u DURABLE_WORKFLOW_WORKER_TOKEN \
  DURABLE_WORKFLOW_CONTROL_TOKEN="$CONTROL_SECRET" \
  php artisan queue:work
```

Worker diagnostics go through Laravel's PSR logger and dispatch `WorkerDiagnosticEvent` through Laravel events.

## Test through the Laravel fake

```php
$workflows = DurableWorkflow::fake()
    ->setWorkflowResult('order-1001', ['status' => 'fulfilled']);

// Exercise application code, then assert the public interaction.
$workflows->assertWorkflowStarted('orders.fulfill', ['1001']);
```

This replaces the injectable `WorkflowClientInterface`; it does not require a callback harness or a running Server.

## Embedded Laravel: use its class API

When Laravel should own durable state, follow the [versioned embedded installation guide](https://durable-workflow.com/docs/2.0/installation/), run its migrations, and use a real queue driver. Generate `Workflow` and activity classes, start them through `WorkflowStub`, and test them with the embedded fake and dispatch/signal/update assertions. The `sync` queue driver is not a production worker.

<div class="warning"><strong>Migration boundary</strong><p>Moving between embedded and service mode is a state migration, not a package rename. Plan workflow identity, open histories, message ingress, backups, and rollback explicitly.</p></div>
