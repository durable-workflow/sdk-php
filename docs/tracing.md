# Optional worker execution context

This integration adds scoped worker spans and log correlation. It also supports the optional service trace-context contract when the connected Server explicitly advertises it. Servers without that capability use independent task traces. Use the configured Server/SDK tuple qualified in [Workflow #773](https://github.com/durable-workflow/workflow/issues/773). Installing OpenTelemetry alone does not enable cross-process propagation.

Install the optional API and your preferred OpenTelemetry SDK/exporter. API 1.10.1 is the minimum supported API within the 1.x line. Configure application providers, propagators, sampling, buffering, exporter timeouts and shutdown before constructing the worker integration. The SDK does not install a provider, force a sampling decision or flush a provider per task.

```php
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Telemetry\LogContext;
use DurableWorkflow\Worker;

// $client, $provider and $logger are application-owned.
$logger->pushProcessor(new LogContext()); // Monolog 2 or 3
$worker = new Worker($client, 'orders', telemetry: new ExecutionTelemetry($provider));
```

Omit `telemetry` for the default execution path. `new ExecutionTelemetry(enabled: false)` also disables instrumentation. When the optional API is absent, the integration is a no-op. Applications can use `ExecutionTelemetry::current()` within a supported execution scope to obtain the current correlation fields without adding wrappers to individual HTTP or database calls.

For a Laravel console worker, explicitly bind `ExecutionTelemetry::class` in the application container after configuring OpenTelemetry. The bridge injects that binding into its worker factory. Add `DurableWorkflow\Bridge\Laravel\LogContext::class` to the relevant logging channel's `tap` list. For a Symfony console worker, define an `ExecutionTelemetry` service and register the `DurableWorkflow\Telemetry\LogContext` service with the `monolog.processor` tag. Both bridges leave instrumentation disabled when the service is absent. Handler constructors resolved at worker startup are outside a task scope; per-invocation handler resolution runs within the execution scope.

`durable.workflow.task`, `durable.activity.task` and `durable.query.task` span the claimed task. Actual callbacks produce `durable.activity.attempt` or `durable.local_activity.attempt` children. Replaying a completed activity does not create another attempt span. SDK-created workflow Fibers bind their hosting task context inside the Fiber and restore it on unwinding. Unrelated application Fibers receive no durable correlation fields automatically.

The instrumentation scope is `durable-workflow/sdk`, with the installed package version. Fields include `workflow_instance_id`, `workflow_run_id`, `workflow_type`, `namespace`, `task_id`, `task_queue`, `activity_id`, `activity_type`, `activity_attempt_id` and `activity_attempt_number` when available. `activity_id` identifies the logical Server activity execution across retries. Older local activity reports without a logical execution ID omit it. Trace and span IDs follow the currently active application child span. An application's log-record `request_id` is preserved.

Field values are bounded and control characters are rejected. Arguments, results, exception messages, stack arguments, credentials and HTTP bodies are excluded from span attributes. High-cardinality IDs are not installed as metric labels. Telemetry setup, status and exporter failures preserve the original callback result or exception.

## Optional service propagation

Configure the same integration on a client with `$client = $client->withTelemetry($telemetry)`. The returned client is a clone. Use the existing explicit `$client->clusterInfo()` discovery during application setup. Workers use their existing cluster discovery. The SDK caches only an exact supported `worker_protocol.server_capabilities.observability_context` object. Missing, malformed or unknown capabilities disable propagation. Business commands do not add a discovery request or retry because telemetry is unavailable.

When available, start, signal, update and cancellation requests capture the caller's context before HTTP instrumentation begins. Child-workflow and continue-as-new commands capture their actual workflow execution context. The SDK adds only the structural `observability_context` field and preserves application arguments and request keys.

Workers read optional metadata after the existing original claim and authority gates. A valid run origin becomes the task's parent. A task-bound dispatch context and validated command or outcome observations become links, without replacing the run parent. Child terminal events may omit their producer task ID. The SDK never fabricates one. Existing Servers continue to use independent task spans.

Official W3C propagators validate trace headers. Baggage is dropped unless its key appears in the `baggageAllowlist` constructor option, and links never carry baggage. Context and link envelopes are bounded to 4096 bytes, with at most 32 links. Invalid metadata is ignored; span diagnostics contain only `discarded` or `filtered`. The optional capability and published-tuple qualification determine Server propagation support.

## Cooperative callback processes

Cooperative activity callbacks run in a Unix child process. The owner retains the task span. By default, tracing in the child is disabled to avoid reusing inherited exporter connections. Durable log identities remain available when the API is installed.

To trace the child callback, supply `childProviderFactory` when constructing `ExecutionTelemetry`. The factory runs after fork and must create a fresh provider with a synchronous span processor and bounded exporter I/O. Child processes stop without running inherited PHP shutdown hooks; a batch processor that only drains at shutdown loses its buffered spans. A missing or failing factory uses a no-op child provider. Abrupt process termination can lose telemetry and does not provide exactly-once export.

The application owns the export queue limits, timeouts, retry policy and final shutdown of its long-running worker provider. Keep export within the worker's existing execution and lease budgets. Configure a synchronous child exporter with a finite request timeout before enabling child spans.
