<?php

declare(strict_types=1);

// Export SDK client and worker metrics through the application's own
// OpenTelemetry meter provider. The SDK requires no exporter package. Install
// what your application uses, for example:
//
//   composer require open-telemetry/sdk open-telemetry/exporter-otlp
//
// The application owns the provider, its readers and exporters, resource
// attributes and shutdown. Labels are bounded: request routes are reviewed
// templates (or "unknown"), never workflow, run, task or worker IDs. Keep
// task queue names bounded too, because task_queue is an application label.

require_once __DIR__.'/bootstrap.php';

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Metrics\OpenTelemetryMetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricExporter\ConsoleMetricExporter;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;

final class MeteredGreeterWorkflow
{
    #[Workflow('metrics.php.greeter')]
    public function run(WorkflowContext $context, string $name): string
    {
        return $context->activity('metrics.php.greet', [$name]);
    }
}

final class MeteredGreetingActivities
{
    #[Activity('metrics.php.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

// Replace the console exporter with your OTLP or Prometheus pipeline.
$reader = new ExportingReader(new ConsoleMetricExporter());
$meterProvider = MeterProvider::builder()->addReader($reader)->build();

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN'),
    metrics: new OpenTelemetryMetricsRecorder($meterProvider),
);

// The worker inherits the client's recorder. Pass `metrics:` to Worker::create()
// to record worker polls and tasks somewhere else.
$worker = Worker::create($client, quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'))
    ->register(MeteredGreeterWorkflow::class, MeteredGreetingActivities::class);

register_shutdown_function(static function () use ($meterProvider): void {
    $meterProvider->shutdown();
});

$worker->run();
