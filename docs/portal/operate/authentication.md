---
layout: layout.njk
title: Authentication
description: Configure bearer, role-specific, and static-header authentication for Durable Workflow PHP clients and workers without leaking credentials.
lead: Authentication is applied at the transport boundary. Prefer separate control-plane and worker principals, rotate them through your secret manager, and never place tokens in workflow history.
previous:
  label: Upgrading
  url: /operate/upgrading/
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

## Static credential validation

PHP SDK {{ release.sdkVersion }} validates each supplied static credential in its original form,
before choosing a role or falling back to a shared token. This applies to
`Client` and `TokenAuthentication` constructor options `token`, `controlToken`
and `workerToken`, framework `ServiceConfiguration` credential settings, and
Laravel's process-environment and explicit process-token handoff. An invalid
unused role also fails validation; supplying a valid token for another role
does not hide it.

Static credential strings refuse:

- malformed UTF-8 byte sequences;
- leading or trailing Unicode `White_Space`, including ordinary spaces,
  nonbreaking spaces and Unicode line separators;
- C0 controls, DEL and C1 controls anywhere in the string, including tabs,
  newlines and NUL.

This deliberately rejects malformed UTF-8 that older releases could accept.
Valid text is preserved byte for byte: the SDK does not trim, case-fold,
normalize Unicode or rewrite internal spaces. Text validation alone does not
establish that a runtime or HTTP intermediary accepts an arbitrary opaque
credential. Obtain a correctly formed replacement from the credential issuer
instead of trimming an invalid credential to make it pass.

`null` and the literal empty string retain their existing optional fallback
behavior. `TokenAuthentication` still requires at least one nonempty token;
an anonymous `Client` remains available where it was already supported. An
empty scoped token can fall back to a valid shared token, but a malformed
nonempty scoped token is an error.

The bridge's existing shared-versus-scoped mode rule still applies.
`ServiceConfiguration::fromArray()` treats literal-empty optional settings as
absence. Its direct constructor retains the exact supplied properties,
including empty strings, and does not normalize them before its mode check.
Use `null` or omission for an absent constructor argument; an empty shared
argument combined with scoped arguments still conflicts with that mode rule.

Errors identify only the setting, for example
`Invalid static credential setting DURABLE_WORKFLOW_WORKER_TOKEN.` They do
not include the rejected value. Keep that boundary in application error
handling and diagnostics too.

For [Laravel](/frameworks/laravel/#service-mode), keep credentials out of cached
configuration and inject the required role at process startup. The resolver
validates shared, client, worker and explicit process-handoff text before
selection, including values belonging to an unused role. For
[Symfony](/frameworks/symfony/), the shared configuration path applies the same
policy before constructing either role's client. Review all supplied settings
before deploying, rather than checking only the selected token.

Custom dynamic `Authentication` implementations retain their existing contract.
The static-token policy does not change their signing or refresh behavior.

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
