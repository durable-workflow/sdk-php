<?php

declare(strict_types=1);

// Optional installed check: SDK metrics through the OpenTelemetry recorder and
// the real OpenTelemetry SDK in-memory exporter, against the live Server. Two
// workflows with different IDs must produce one bounded route series.

require '/consumer/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Metrics\OpenTelemetryMetricsRecorder;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricExporter\InMemoryExporter;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;

$exporter = new InMemoryExporter();
$reader = new ExportingReader($exporter);
$provider = MeterProvider::builder()->addReader($reader)->build();
$client = new Client((string) getenv('DW_URL'), token: (string) getenv('DW_TOKEN'),
    metrics: new OpenTelemetryMetricsRecorder($provider));
$suffix = bin2hex(random_bytes(6));
$ids = ["metrics-a-{$suffix}", "metrics-b-{$suffix}"];
foreach ($ids as $id) {
    $client->startWorkflow('installed.lifecycle', $id, (string) getenv('DW_TASK_QUEUE'), ['metrics']);
}
foreach ($ids as $id) {
    $client->describeWorkflow($id);
}
foreach ($ids as $id) {
    $client->signalWorkflow($id, 'approve', ['metrics']);
}
$reader->collect();

$series = [];
foreach ($exporter->collect() as $metric) {
    foreach ($metric->data->dataPoints as $point) {
        $series[] = ['name' => $metric->name, 'unit' => $metric->unit, 'scope' => $metric->instrumentationScope->getName(),
            'attributes' => $point->attributes->toArray(),
            'value' => property_exists($point, 'count') ? $point->count : $point->value];
    }
}
$describe = array_values(array_filter($series, static fn (array $s): bool => $s['name'] === 'durable_workflow_client_requests'
    && ($s['attributes']['route'] ?? null) === '/workflows/{workflow_id}' && $s['attributes']['method'] === 'GET'));
$encoded = json_encode($series, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
$leaks = array_values(array_filter($ids, static fn (string $id): bool => str_contains($encoded, $id)));
$ok = count($describe) === 1 && (int) $describe[0]['value'] === 2 && $leaks === [];
echo json_encode(['scenario' => 'otel_metrics', 'outcome' => $ok ? 'pass' : 'product-fail', 'workflow_ids' => $ids,
    'describe_series' => $describe, 'id_leaks' => $leaks, 'series' => $series], JSON_UNESCAPED_SLASHES)."\n";
