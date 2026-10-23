---
layout: layout.njk
title: Failures & retries
description: Model activity failures, transport failures, workflow terminal states, cancellation, timeouts, and retry safety in the PHP SDK.
lead: Durable execution does not make every error retryable. Classify transport delivery, activity attempts, deterministic replay, and terminal workflow outcomes separately.
previous:
  label: Message Streams
  url: /build/message-streams/
next:
  label: Testing
  url: /build/testing/
---
## Know which layer failed

| Failure | Meaning | Usual response |
| --- | --- | --- |
| `TransportException` | The SDK could not complete an HTTP exchange. | Retry only when request identity makes replay safe. |
| `ServerException` | The runtime rejected or failed an operation. | Inspect status, reason, and details. |
| `ActivityFailed` | A recorded activity reached a failed terminal attempt. | Catch in workflow code only when compensation is deterministic. |
| `NonDeterministicWorkflow` | Code no longer matches committed history. | Restore compatible code; do not blindly retry. |
| `WorkflowEntryArgumentsMismatch` | Recorded start input cannot bind to the entry method, before its body runs. The workflow task is refused and the run remains recoverable. | Restore a compatible entry method, inspect the task and use run-scoped repair. |
| `WorkflowFailed` | The execution closed as failed. | Surface the recorded failure to the caller. |
| `WorkflowTimedOut` | A Server deadline closed the run. | Start a new workflow only if business policy permits. |
| `WorkflowCancelled` | The run closed as Cancelled, after terminal cancellation or successful cooperative cleanup. | Inspect the cancellation lifecycle and cleanup outcome. |
| `WorkflowTerminated` | Termination closed the run immediately. | Do not expect workflow-code cleanup after termination. |

## Make activity retries safe

Pass activity policy in the command options supported by your runtime, and make the downstream action idempotent:

```php
$receipt = $context->activity(
    'payments.capture',
    [$orderId, $amount],
    [
        'start_to_close_timeout' => 30,
        'retry_policy' => [
            'max_attempts' => 5,
            'backoff_seconds' => [1, 2, 4, 8],
        ],
    ],
);
```

`max_attempts` includes the first attempt. This example permits four retries,
with the listed delays in seconds. Unsupported option or retry-policy field
names throw `InvalidArgumentException` before an activity command is submitted.

Use the order or activity identity as the payment provider's idempotency key. A retry policy cannot repair a non-idempotent downstream API.

Older SDKs forwarded unsupported names and the runtime applied its defaults.
Correcting those names changes the durable activity policy. For existing runs,
keep the previously recorded policy in a replay-compatible version branch, or
finish those runs before deploying the changed policy.

## Durable cleanup

Workflow `finally` blocks can await cleanup activities, timers, and metadata
writes after success or a handled failure. Replay reconstructs those commands
from history. Discarding a suspended replay Fiber does not schedule cleanup or
complete the workflow; PHP may still unwind local `finally` code, so put external
effects in activities rather than directly in the workflow body.

Use `requestCancellation()` with cooperating workers when cancellation requires
bounded workflow cleanup. The request preserves one original deadline across
children, activities, duplicates and worker replacement. Shielded durable
cleanup replays after worker loss and successful cleanup closes the run as
`Cancelled`. See the [cooperative cancellation guide](https://durable-workflow.com/docs/2.0/polyglot/cancellation/)
for worker opt-in, operation policies and inspection.

Terminal `cancelWorkflow()` and `terminateWorkflow()` still close the run
immediately. They do not resume workflow cleanup, and an already closed run
cannot schedule compensation through `finally`.

## Heartbeat long attempts

```php
foreach ($batches as $index => $batch) {
    importBatch($batch);
    $context->heartbeat(['completed_batches' => $index + 1]);
}
```

Heartbeat details make slow work observable and provide a cancellation checkpoint. Catch `ActivityCancelled` only to release local resources, then rethrow it so the runtime records cancellation correctly.

Cooperative workers supervise callbacks independently of application
heartbeats. A stop receipt proves callback stop, while attempt fencing rejects
a stale result. Downstream side effects still need application idempotency or
reconciliation.

## Preserve terminal distinctions at the client

```php
try {
    $result = $handle->result(timeoutSeconds: 60);
} catch (WorkflowTimedOut $failure) {
    reportDeadline($failure);
} catch (WorkflowFailed $failure) {
    reportBusinessFailure($failure);
}
```

A client-side wait timeout is not proof that the workflow failed; describe the handle or wait again. A workflow terminal timeout is durable Server state.

## Treat nondeterminism as a deployment incident

Do not paper over `NonDeterministicWorkflow` with process retries. Compare the deployed build with the history's worker routing, restore the old command sequence, and roll forward with a replay-compatible change. Keep a replay corpus for long-lived workflow histories in [testing](/build/testing/).

<div class="outcome"><strong>Retry question</strong><p>Before adding a retry, identify the durable request ID, the side effect's idempotency boundary, and what evidence proves the prior attempt did not already succeed.</p></div>

## Bound failure diagnostics

Failure reporting keeps diagnostic content within finite defaults:

| Content | Default budget |
| --- | --- |
| UTF-8 message, including its marker | 62 KiB |
| Stack trace, including diagnostic markers | 128 KiB |
| Serialized details and diagnostics together | 64 KiB |
| Complete encoded failure request, including JSON escaping and identity | 1 MiB |

The message budget reserves 2 KiB below the original 64 KiB ceiling for
published runtime summaries. It does not repair the separate runtime summary
byte-bound defect tracked in shared issue 225.

Oversized content produces a visible `[truncated failure report; ...]` message
marker and bounded diagnostics with original sizes and omitted fields. The SDK
retains a complete valid Avro details envelope or explicitly omits it. It never
slices Avro or base64 bytes. Small reports keep their message and details.

The original failure type, retry decision, authored retry delays and claimed
attempt identity remain unchanged. Diagnostic limits are separate from business
payload and worker request timeout budgets. Cooperative callbacks also bound
failure diagnostics before sending them to their owning worker.

For a transport with a smaller request budget, narrow the client limit:

```php
$client = $client->withFailureReportLimit(128 * 1024);
```

An injected transport can expose a lower known body budget through
`RequestSizeLimitTransport`. The lower limit applies after markers and JSON
escaping are included. If required identity or fixed request content cannot fit,
`FailureReportRefused` reports `failure_report_refused` locally. Treat this as a
reporting refusal requiring configuration or identity repair, rather than a new
application failure or permission to publish under a different attempt.

Activity failures use existing diagnostic fields. Workflow-task failures use
the existing stack string for structured truncation information. The published
Server qualification records task failures and grants a legitimate retry turn,
but that runtime does not append a workflow-task failure event to workflow
history or persist its stack diagnostics. That visibility work remains tracked
in [shared issue 225](https://github.com/durable-workflow/.github/issues/225).
