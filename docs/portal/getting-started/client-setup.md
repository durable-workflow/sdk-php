---
layout: layout.njk
title: Client setup
description: Configure Durable Workflow PHP clients for local Server, Cloud namespaces, role-specific authentication, and safe workflow handles.
lead: The client owns the runtime origin, namespace, authentication boundary, and transport. Workflow values use the official Apache Avro payload codec.
previous:
  label: Your first workflow
  url: /getting-started/first-workflow/
next:
  label: Workflows & activities
  url: /build/workflows-activities/
---
## Install and construct

Install from the supported stable 2.0 line shown throughout this portal:

```bash
{{ release.composerCommand }}
```

Pass a Server origin or the complete Cloud runtime base URI. Do not add a terminal `/api`.

```php
use DurableWorkflow\Client;

$controlClient = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL') ?: 'http://localhost:8080',
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE') ?: 'default',
    controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN') ?: null,
);

$workerClient = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL') ?: 'http://localhost:8080',
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE') ?: 'default',
    workerToken: getenv('DURABLE_WORKFLOW_WORKER_TOKEN') ?: null,
);
```

Use the shorter `token:` constructor argument when one credential is intentionally allowed to perform both roles. Production Cloud provisioning normally gives client and worker principals separate credentials.

## Select a namespace without rebuilding the client

`withNamespace()` returns a new client with the same authentication and transport. Workflow payload encoding remains Apache Avro:

```php
$billing = $controlClient->withNamespace('billing');
$orders = $controlClient->withNamespace('orders');
```

The original client is unchanged. This makes tenant or application boundaries visible in dependency injection instead of mutating shared state.

## Start once, then retain the handle

```php
$handle = $controlClient->startWorkflow(
    workflowType: 'orders.process',
    workflowId: 'order-1001',
    taskQueue: 'orders',
    input: [['order_id' => '1001']],
);

$result = $handle->result(timeoutSeconds: 30);
```

The workflow ID is the stable instance identity. The run ID identifies one execution in that instance's chain. Ordinary handle methods follow the current run after continue-as-new; methods ending in `SelectedRun` keep the original run guard and fail instead of silently crossing to a successor.

## Inject a PSR transport when you need one

Guzzle is included as the default transport. To reuse your application's PSR-18 client and PSR-17 factories, construct `Psr18Transport` and pass it as `transport:`. Authentication still belongs in an `Authentication` implementation; do not bury rotating role credentials in a generic HTTP middleware stack.

## Check the runtime contract at deployment

```php
$cluster = $client->clusterInfo();

printf(
    "server=%s worker_protocol=%s\n",
    $cluster->version,
    $cluster->raw['worker_protocol']['version'] ?? 'unknown',
);
```

This SDK declares worker protocol `{{ release.workerProtocolVersion }}` and is qualified against the {{ release.serverChannel }} line. Runtime discovery is authoritative for what a particular endpoint advertises.

<div class="note"><strong>Connection rule</strong><p>Keep the Cloud path prefix exactly as provisioned, but remove a trailing slash. The client normalizes that boundary and appends its own API paths.</p></div>
