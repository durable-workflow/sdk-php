---
layout: layout.njk
title: Authentication
description: Configure bearer, role-specific, and static-header authentication for Durable Workflow PHP clients and workers without leaking credentials.
lead: Authentication is applied at the transport boundary. Prefer separate control-plane and worker principals, rotate them through your secret manager, and never place tokens in workflow history.
previous:
  label: Deployment
  url: /operate/deployment/
next:
  label: Graceful shutdown
  url: /operate/graceful-shutdown/
---
## One development token

```php
$client = new Client('http://localhost:8080', token: 'dev-token');
```

This is convenient for a local self-hosted Server. It gives control and worker requests the same bearer token and should not become the default production role model.

## Separate client and worker credentials

```php
$controlClient = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE') ?: 'default',
    controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN') ?: null,
);

$workerClient = new Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE') ?: 'default',
    workerToken: getenv('DURABLE_WORKFLOW_WORKER_TOKEN') ?: null,
);
```

Control-plane operations use the control token; worker registration, heartbeats, polls, and task completion use the worker token. The fallback rules keep a deliberately shared token usable, but explicit role tokens make least privilege review clearer.

## Static headers for an authenticated gateway

`StaticHeadersAuthentication` adds the same fixed header set to both roles. Use it only when a trusted gateway owns authentication and the fixed headers contain no per-request signature:

```php
use DurableWorkflow\Auth\StaticHeadersAuthentication;

$authentication = new StaticHeadersAuthentication([
    'X-Internal-Service' => 'orders-worker',
]);
```

For rotating or request-signed authentication, implement `Authentication::headers(bool $workerRequest)` or provide a transport that owns the signing protocol. Do not construct signatures from inside workflow code.

## Keep secrets out of durable state

Workflow input, activity input/result, signal arguments, update arguments, memo, and search attributes may be retained or exposed to operators. Pass a secret reference to an activity and resolve it at execution time instead of recording the credential itself.

## Rotation checklist

- overlap old and new credentials during the rollout when the provider permits it;
- update worker pools and confirm fresh registrations;
- update client processes and exercise one control-plane call;
- revoke the old credential only after both roles have moved;
- ensure logs redact `Authorization` and application secret headers.

<div class="warning"><strong>Browser boundary</strong><p>Do not expose a Server or Cloud bearer token to browser JavaScript. Call the SDK from a trusted PHP backend and authorize the end user there.</p></div>
