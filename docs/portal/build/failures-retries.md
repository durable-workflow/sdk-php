---
layout: layout.njk
title: Failures & retries
description: Model activity failures, transport failures, workflow terminal states, cancellation, timeouts, and retry safety in the PHP SDK.
lead: Durable execution does not make every error retryable. Classify transport delivery, activity attempts, deterministic replay, and terminal workflow outcomes separately.
previous:
  label: Signals, queries & updates
  url: /build/messages/
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
| `WorkflowFailed` | The execution closed as failed. | Surface the recorded failure to the caller. |
| `WorkflowTimedOut` | A Server deadline closed the run. | Start a new workflow only if business policy permits. |
| `WorkflowCancelled` / `WorkflowTerminated` | Cooperative cancellation or forceful operator termination. | Preserve the distinct terminal reason. |

## Make activity retries safe

Pass activity policy in the command options supported by your runtime, and make the downstream action idempotent:

```php
$receipt = $context->activity(
    'payments.capture',
    [$orderId, $amount],
    [
        'start_to_close_timeout_seconds' => 30,
        'retry_policy' => [
            'maximum_attempts' => 5,
            'initial_interval_seconds' => 1,
            'maximum_interval_seconds' => 20,
        ],
    ],
);
```

Use the order or activity identity as the payment provider's idempotency key. A retry policy cannot repair a non-idempotent downstream API.

## Heartbeat long attempts

```php
foreach ($batches as $index => $batch) {
    importBatch($batch);
    $context->heartbeat(['completed_batches' => $index + 1]);
}
```

Heartbeat details make slow work observable and provide a cancellation checkpoint. Catch `ActivityCancelled` only to release local resources, then rethrow it so the runtime records cancellation correctly.

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
