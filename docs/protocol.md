# Standalone protocol

The SDK sends control-plane version `2` to workflow, schedule, namespace,
search-attribute, and service-operation routes. Cluster discovery uses
`/api/cluster/info`. Worker requests use protocol `1.13` under `/api/worker`.
Every request also carries `X-Namespace`; control and worker bearer tokens can
be configured independently. `Client::withNamespace()` creates another client
selection without mutating the original or replacing its transport, codec, or
authentication.

## Payloads

New payloads use the platform's fixed recursive Avro Value schema:

```text
base64(C3 01 || CRC-64-AVRO fingerprint || Avro Value datum)
```

`AvroPayloadCodec` delegates schema parsing, binary encoding, and datum reading
to the official `apache/avro` Composer package. Its fixed-schema writer keeps
map keys as ordered string/value pairs until the binary encoder receives them,
avoiding PHP's numeric-key coercion. The bundled writer-schema fingerprint
selects the immutable schema; unknown fingerprints and incompatible branches
fail with `unsupported_payload_schema` instead of falling back to JSON. Each
named union branch preserves null, boolean, signed 64-bit integer, finite
double, bytes, UTF-8 string, list, or string-keyed map identity.

PHP strings select the UTF-8 string branch. Use
`AvroBinaryValue::fromBytes($bytes)` to select the bytes branch. PHP arrays use
`array_is_list()` to distinguish lists from maps; every map key must be a
string, and keys are never stringified. Use `AvroMapValue::fromPairs()` for an
empty map or keys such as `"0"` that PHP arrays cannot retain as strings.
Decoding returns the adapter whenever a native array would lose map identity,
and re-encoding its ordered pairs preserves the original map. Convert objects,
resources, dates, UUIDs, decimals, enums, and arbitrary-precision integers to
explicit supported values before starting work.

Service-operation arguments use the same codec as workflow payloads. The SDK
sends the encoded argument blob with its `payload_codec` name to the public
`/api/service-endpoints/{endpoint}/services/{service}/operations/{operation}/execute`
surface. Start forces asynchronous/accepted semantics and returns a
handle for describe and cancel; execute waits for completion unless its
immutable `ServiceOperationOptions` specifies another wait policy.

## Run targeting

A workflow ID identifies a stable instance whose current run may change after
continue-as-new. Ordinary handle operations resolve the current run. A handle
returned from `startWorkflow()` also retains the selected run ID; methods such
as `querySelectedRun()`, `cancelSelectedRun()`, and `resultOfSelectedRun()` put
that ID in the URL so the server rejects stale targeting.

## Visibility and schedule paging

`Client::listWorkflows()` maps `workflow_type`, `status`, `query`, `page_size`,
and `next_page_token` to the public visibility route and returns a typed page.
`Client::listSchedules()` returns a typed page containing mapped schedules, the
server's exact `next_page_token`, and the complete raw response envelope. Its
supported filters map `status`, `workflowType`, `query`, `pageSize`, and
`nextPageToken` to `status`, `workflow_type`, `query`, `page_size`, and
`next_page_token`, respectively. Status and workflow type are exact matches;
the visibility query is passed unchanged to the server's schedule visibility
parser. Multiple filters are combined by the server with AND semantics. The
SDK maps the returned page as-is and does not apply client-side filtering.

Continuation tokens are opaque. Pass a non-null token back unchanged with the
same namespace, status, workflow type, and visibility query. A null token ends
the traversal. Malformed, filter-mismatched, cross-namespace, and stale tokens
are not converted into empty pages: `ServerException` retains the HTTP status,
machine-readable reason, and the complete response in `details`, including
field errors and last-safe-cursor evidence supplied by the server.
`Client::scheduleHistory()` exposes the route's `limit` and `after_sequence`
cursor without converting server refusals into empty results.

## Replay

PHP workflow handlers yield commands from `WorkflowContext`. On each workflow
task the worker re-runs the generator from the beginning, matches yielded
command shapes against positive durable sequence numbers, and sends recorded
activity, child-workflow, timer, and side-effect results back into the
generator. A changed command order fails the task with a typed
`NonDeterministicWorkflow` error instead of executing different work.

The runtime fetches all paginated history before replay. Activity handlers can
heartbeat and observe cancellation. Query handlers receive immutable committed
history; update handlers receive the accepted update and return a
`complete_update` command. SIGINT/SIGTERM request graceful loop shutdown when
the `pcntl` extension is available.

Managed worker registration advertises a command contract for every registered
workflow type. Its `queries` and `updates` lists come directly from that
worker's handler registries, including empty lists when a workflow has no such
handlers. The server can therefore snapshot the declared names when a run
starts and route later query or update requests without depending on PHP
framework metadata.

## Worker poll envelopes

`Client::pollWorkflowTaskResponse()`, `Client::pollActivityTaskResponse()`, and
`Client::pollQueryTaskResponse()` return the complete worker-protocol response
without discarding fields when no task is leased. This preserves `poll_status`,
`reason`, `protocol_version`, `server_capabilities`, and endpoint-specific or
future protocol metadata. The task-only `pollWorkflowTask()`,
`pollActivityTask()`, and `pollQueryTask()` methods delegate to the full response
methods and return only an array task or `null`.

`DurableWorkflow\Worker\PollResponse::isTerminal()` classifies stop decisions
from typed fields, not human-readable error text. Stale registrations
(`stale_worker_registration` or `worker_heartbeat_stale`) and server stop/drain
outcomes (`draining`, `stopped`, `worker_draining`, or `worker_stopped`) are
terminal. Managed `Worker::run()` and `Worker::tick()` stop all subsequent
polling on those outcomes. `empty`, `timeout`, `workflow_task_pending`, and
other non-terminal no-task outcomes remain idle. A terminal HTTP `409` poll body
is returned as the same response envelope; unrelated conflicts remain
`ServerException` failures.

`Worker::run()` adopts a valid `heartbeat_interval_seconds` from the worker
registration response and from later heartbeat acknowledgements. An invalid or
missing registration cadence preserves the configured fallback, while an
invalid later acknowledgement leaves the current valid cadence unchanged.
Because the managed PHP worker is synchronous, it bounds each individual long
poll below the active heartbeat interval when possible, refreshes proactively
when the next poll would reach the cadence, and rechecks after every workflow,
activity, and query response. Poll timeouts and ordinary empty responses retain
their normal non-terminal semantics while idle workers remain fresh.

Managed workers also retry a poll when its transport failure carries a complete
worker-protocol envelope that explicitly marks the refusal as retryable. The
SQLite `backend_lock_pressure` response is recognized by its HTTP 503 status,
typed `poll_status` and `reason`, empty task, and valid retry delay. Retries stay
on the same poll kind and worker registration, use capped backoff, refresh the
worker heartbeat while waiting, and check for shutdown in short sleep slices.
The optional `transientPollRetryObserver` constructor callback receives the
task kind, consecutive attempt, chosen delay in seconds, and `ServerException`
for operational telemetry. Authentication errors, malformed envelopes, generic
service failures, and responses that are not explicitly retryable still throw.

Before replaying or completing a workflow task, the managed worker requires a
successful lease-renewal response whose task ID, attempt, and lease owner match
the current claim. A typed `renewed=false`, `retryable=true` response is retried
with the same fencing values and interruptible capped backoff. Workflow code is
not invoked until renewal succeeds. A closed run is discarded under its typed
terminal contract, while lease loss, non-retryable refusal, and malformed or
mismatched renewal responses remain visible failures and cannot lead to task
completion.

## Custom transports and authentication

Inject `Transport` to adapt the SDK to another PSR-18 stack or a test harness.
`Psr18Transport` is the default and accepts any PSR-18 client plus PSR-17
request and stream factories. Inject `Authentication` to add signed gateway
headers or API keys. `TokenAuthentication` is the bearer-token default and can
select distinct credentials for each protocol plane.
