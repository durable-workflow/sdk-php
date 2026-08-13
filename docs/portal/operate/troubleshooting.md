---
layout: layout.njk
title: Troubleshooting
description: Diagnose Durable Workflow PHP connection, authentication, worker, queue, replay, timeout, and version problems by observable symptom.
lead: Start at the failing boundary and keep the runtime response. A worker process, registered worker, admitted task, durable history event, and terminal result are separate checkpoints.
previous:
  label: Graceful shutdown
  url: /operate/graceful-shutdown/
next:
  label: PHP SDK home
  url: /
---
## Triage by symptom

| Symptom | Check first | Likely correction |
| --- | --- | --- |
| Connection path returns 404 | Runtime URL passed to `Client` | Remove a manually appended `/api`; retain a Cloud path prefix. |
| 401 or 403 on start | Control-plane token and namespace | Use the client-role credential for that namespace. |
| Worker cannot register or poll | Worker token, queue, protocol discovery | Use the worker-role credential and a supported runtime. |
| Workflow remains pending | Worker visibility and registered workflow type | Start a compatible worker on the exact task queue. |
| Activity remains scheduled | Registered activity type and capacity | Deploy the handler or split an overloaded queue. |
| Result wait times out | Describe the workflow and inspect history | Distinguish client wait timeout from workflow terminal timeout. |
| `NonDeterministicWorkflow` | Deployed build and committed command sequence | Restore replay-compatible code or route old histories correctly. |
| Worker exits after a poll | Typed poll status and reason | Treat draining/stopped/stale registration as lifecycle, not empty work. |

## Verify each boundary

```php
$health = $client->health();
$cluster = $client->clusterInfo(includeDiagnostics: true);
$workers = $client->listWorkers(taskQueue: 'orders');
$queue = $client->describeTaskQueue('orders');
$execution = $client->describeWorkflow('order-1001');
```

Keep response status, reason, runtime version, worker protocol, namespace, task queue, workflow ID, run ID, and worker build ID together in an incident record. Remove tokens and sensitive payloads before sharing it.

## A start call succeeded, but no work runs

Confirm the client's `taskQueue` exactly matches the worker's queue. Then confirm the worker registered the exact workflow type string. PHP class autoload success does not prove the protocol type name matches.

If the workflow scheduled an activity, repeat the same check for the activity type. A workflow worker can be healthy while the activity capacity it needs is absent.

## A result wait timed out

`WorkflowHandle::result()` polls for a bounded caller duration. When it times out, the durable execution may still be running. Call `describe()` and inspect its status before starting anything again. Reuse the original workflow ID or an application idempotency key; do not create duplicates just because the client lost patience.

## Replay became nondeterministic

Compare the history's scheduled command type and sequence with the deployed workflow handler. Common causes are reordering context calls, renaming an activity type, inserting a timer before an old command, or returning before an operation that existing histories expect.

Restore the compatible implementation for open histories. Validate a fix with a retained replay fixture before redeploying.

## Escalate with useful evidence

Include the smallest reproduction, sanitized runtime discovery, the SDK version reported by `composer show durable-workflow/sdk`, Server version, workflow/run IDs, queue/build identity, and the typed exception or Server reason. Do not include bearer tokens, Cloud secrets, raw customer payloads, or full unredacted history.

<div class="note"><strong>Reference boundary</strong><p>Use the <a href="/api/">generated API Reference</a> for exact constructor and method signatures after the guide identifies the correct concept and failure layer.</p></div>
