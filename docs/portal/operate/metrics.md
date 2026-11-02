---
layout: layout.njk
title: Metrics
description: Record Durable Workflow PHP client requests, worker polls and worker tasks with bounded labels, and export them through OpenTelemetry or Prometheus.
lead: Count and time SDK requests, polls and tasks through a recorder you choose. Labels are bounded, so identifiers never become metric series.
previous:
  label: Execution tracing
  url: /operate/tracing/
next:
  label: Troubleshooting
  url: /operate/troubleshooting/
---
SDK metrics are optional. The default recorder discards observations, and the
SDK adds no exporter dependency. Instruments follow the shared SDK metrics
contract, so the Python and PHP SDKs emit the same names, units, tag keys and
route labels.

## Choose a recorder

Pass a `MetricsRecorder` to the client. A worker inherits its client's
recorder; give the worker its own recorder to send worker polls and tasks
somewhere else.

```php
use DurableWorkflow\Client;
use DurableWorkflow\Metrics\InMemoryMetricsRecorder;
use DurableWorkflow\Worker;

$metrics = new InMemoryMetricsRecorder();
$client = new Client('https://runtime.example', workerToken: $token, metrics: $metrics);

$worker = Worker::create($client, 'orders');                               // inherits $metrics
$separate = Worker::create($client, 'orders', metrics: $workerRecorder);   // explicit override
```

The SDK provides:

| Recorder | Use |
| --- | --- |
| `NoopMetricsRecorder` | The default. Pass it explicitly to discard observations. |
| `InMemoryMetricsRecorder` | Tests and diagnostics. Read series back with `counter($name, $tags)` and `histogram($name, $tags)`. |
| `OpenTelemetryMetricsRecorder` | Forwards to your OpenTelemetry `MeterProvider`. Needs the optional `open-telemetry/api` package. |

A custom recorder implements `increment(string $name, float $value = 1.0, array $tags = [])`
and `record(string $name, float $value, array $tags = [])`. Keep it fast. The SDK
catches recorder exceptions, so an exporter failure never changes a request
result, a retry decision, a deadline or task authority.

## Instruments

| Name | Type | Unit | Tags |
| --- | --- | --- | --- |
| `durable_workflow_client_requests` | counter | `1` | `method`, `route`, `plane`, `status_code`, `outcome` |
| `durable_workflow_client_request_duration_seconds` | histogram | `s` | same as above |
| `durable_workflow_worker_polls` | counter | `1` | `task_kind`, `task_queue`, `outcome` |
| `durable_workflow_worker_poll_duration_seconds` | histogram | `s` | same as above |
| `durable_workflow_worker_tasks` | counter | `1` | `task_kind`, `task_queue`, `outcome` |
| `durable_workflow_worker_task_duration_seconds` | histogram | `s` | same as above |

Each observation increments its counter by one and records one nonnegative
duration with the same tags. A client observation covers one logical request,
including the SDK's internal transport retries. Poll and task observations
describe this worker process, not durable execution counts, workflow completion
or schedule-to-start latency. A missing observation does not mean zero work or
a healthy process.

## Bounded labels

Labels never carry identifiers or secrets:

- `route` is a reviewed path template without `/api`, the query string or the
  fragment, for example `/workflows/{workflow_id}/runs/{run_id}`. Workflow, run,
  task, worker, session, schedule, stream and operation names in paths become
  placeholders. A path outside the reviewed list becomes the single value
  `unknown`.
- `status_code` is the final HTTP status, or `none` when the transport did not
  expose one.
- `plane` is `control` or `worker`.
- Request and response payloads, credentials, namespace names and exception
  messages are never labels.

`task_queue` is your configured queue name. Keep queue names bounded, not one
per run or customer. Review the cardinality of any resource attributes you add
in your exporter.

## Outcomes

| Instruments | Values |
| --- | --- |
| Client requests | `ok`; `http_error` for an HTTP error status; otherwise the exception class name, such as `DurableWorkflow\Exception\TransportException` |
| Worker polls | `task`, `empty`, `backpressure` (HTTP 429 or backend lock pressure), `stopped`, `error` |
| Worker tasks | `completed`, `failed`, `aborted` (task authority lost, such as a stale attempt), `cancelled`, `deferred`, `expired`, `error` |

A worker task outcome describes how this worker processed the task, not the
workflow's terminal state. A workflow that fails still records a `completed`
workflow task when the worker delivered its failure.

Activities that run in a cooperative child process do not record through the
parent's recorder; the parent worker records the task.

## Export with OpenTelemetry

Install `open-telemetry/api`, the OpenTelemetry SDK and an exporter. Your
application owns the meter provider, its readers and exporters, resource
attributes, flushing and shutdown.

```php
use DurableWorkflow\Client;
use DurableWorkflow\Metrics\OpenTelemetryMetricsRecorder;
use OpenTelemetry\Contrib\Otlp\MetricExporter;
use OpenTelemetry\Contrib\Otlp\OtlpHttpTransportFactory;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;

// composer require open-telemetry/sdk open-telemetry/exporter-otlp
$transport = (new OtlpHttpTransportFactory())
    ->create('http://otel-collector:4318/v1/metrics', 'application/x-protobuf');
$meterProvider = MeterProvider::builder()
    ->addReader(new ExportingReader(new MetricExporter($transport)))
    ->build();

$client = new Client('https://runtime.example', workerToken: $token,
    metrics: new OpenTelemetryMetricsRecorder($meterProvider));
```

The recorder uses instrumentation scope `durable-workflow/sdk` with the
installed package version. Counters use unit `1` and durations use `s`. PHP has
no background export thread: the reader exports when the provider is flushed or
shut down. A long-running worker should call `$meterProvider->forceFlush()` on
the cadence your monitoring needs, and `shutdown()` on exit.
`examples/metrics-opentelemetry.php` shows a complete worker.

## Scrape with Prometheus

The SDK has no direct Prometheus dependency. Export OTLP to an OpenTelemetry
Collector, as above, and let the Collector serve a Prometheus scrape endpoint:

```yaml
receivers:
  otlp:
    protocols:
      http:
exporters:
  prometheus:
    endpoint: 0.0.0.0:9464
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

Prometheus naming conventions then apply. Counter series gain a `_total`
suffix, and histograms expose `_bucket`, `_sum` and `_count` series. Check your
Collector's suffix settings before writing dashboards. The recorder names in
this page don't change.
