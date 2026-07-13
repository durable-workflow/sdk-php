# Standalone protocol

The SDK sends control-plane version `2` to `/api/workflows`, `/api/schedules`,
and `/api/namespaces`. Worker requests use protocol `1.13` under `/api/worker`.
Every request also carries `X-Namespace`; control and worker bearer tokens can
be configured independently.

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

## Run targeting

A workflow ID identifies a stable instance whose current run may change after
continue-as-new. Ordinary handle operations resolve the current run. A handle
returned from `startWorkflow()` also retains the selected run ID; methods such
as `querySelectedRun()`, `cancelSelectedRun()`, and `resultOfSelectedRun()` put
that ID in the URL so the server rejects stale targeting.

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
