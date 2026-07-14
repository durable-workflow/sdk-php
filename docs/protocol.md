# Standalone protocol

The SDK sends control-plane version `2` to workflow, schedule, namespace,
search-attribute, and service-operation routes. Cluster discovery uses
`/api/cluster/info`. Worker requests use protocol `1.13` under `/api/worker`.
Every request also carries `X-Namespace`; control and worker bearer tokens can
be configured independently. `Client::withNamespace()` creates another client
selection without mutating the original or replacing its transport, codec, or
authentication.

## Payloads

New payloads use the platform's Avro generic wrapper:

```text
base64(0x00 || avro-binary(record { json: string, version: int }))
```

`AvroPayloadCodec` delegates schema parsing, datum writing, and datum reading
to the official `apache/avro` Composer package. The optional typed mode embeds
the writer schema after a `0x01` prefix and resolves it against an optional
reader schema. The generic wrapper accepts JSON-safe PHP values only; convert
objects, resources, dates, and enums to explicit scalar/array representations
before starting work.

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

## Custom transports and authentication

Inject `Transport` to adapt the SDK to another PSR-18 stack or a test harness.
`Psr18Transport` is the default and accepts any PSR-18 client plus PSR-17
request and stream factories. Inject `Authentication` to add signed gateway
headers or API keys. `TokenAuthentication` is the bearer-token default and can
select distinct credentials for each protocol plane.
