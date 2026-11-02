<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Metrics\InMemoryMetricsRecorder;
use DurableWorkflow\Metrics\OpenTelemetryMetricsRecorder;
use DurableWorkflow\Tests\Support\FakeTransport;
use OpenTelemetry\SDK\Metrics\Data\Histogram;
use OpenTelemetry\SDK\Metrics\Data\Sum;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricExporter\InMemoryExporter;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;
use PHPUnit\Framework\TestCase;

final class MetricsRecorderAdaptersTest extends TestCase
{
    private const TAGS = ['method' => 'GET', 'route' => '/workflows/{workflow_id}/runs/{run_id}',
        'plane' => 'control', 'status_code' => 'none', 'outcome' => 'ok'];

    public function testInMemoryRecorderKeepsSeriesByNameAndExactTags(): void
    {
        $recorder = new InMemoryMetricsRecorder();
        $client = new Client('https://server.invalid', transport: new FakeTransport([[], []]), metrics: $recorder);
        $client->describeWorkflow('private-workflow-1', 'private-run-1');
        $client->describeWorkflow('private-workflow-2', 'private-run-2');

        self::assertSame(2.0, $recorder->counter('durable_workflow_client_requests', self::TAGS));
        self::assertSame(2.0, $recorder->counter('durable_workflow_client_requests', array_reverse(self::TAGS, true)));
        $durations = $recorder->histogram('durable_workflow_client_request_duration_seconds', self::TAGS);
        self::assertCount(2, $durations);
        foreach ($durations as $seconds) {
            self::assertGreaterThanOrEqual(0.0, $seconds);
        }
        self::assertSame(0.0, $recorder->counter('durable_workflow_client_requests', ['route' => 'unknown']));
    }

    public function testOpenTelemetryRecorderExportsContractNamesUnitsAndBoundedAttributes(): void
    {
        $exporter = new InMemoryExporter();
        $reader = new ExportingReader($exporter);
        $provider = MeterProvider::builder()->addReader($reader)->build();
        $client = new Client('https://server.invalid', transport: new FakeTransport([[], []]),
            metrics: new OpenTelemetryMetricsRecorder($provider));
        $client->describeWorkflow('private-workflow-1', 'private-run-1');
        $client->describeWorkflow('private-workflow-2', 'private-run-2');
        $reader->collect();

        $metrics = [];
        foreach ($exporter->collect() as $metric) {
            $metrics[$metric->name] = $metric;
        }
        $contract = json_decode((string) file_get_contents(dirname(__DIR__).'/resources/protocol/sdk-metrics-v1.json'), true, flags: JSON_THROW_ON_ERROR);
        $units = array_column($contract['metrics'], 'unit', 'name');

        $counter = $metrics['durable_workflow_client_requests'];
        self::assertSame($units['durable_workflow_client_requests'], $counter->unit);
        self::assertSame('durable-workflow/sdk', $counter->instrumentationScope->getName());
        self::assertInstanceOf(Sum::class, $counter->data);
        self::assertTrue($counter->data->monotonic);
        $points = [...$counter->data->dataPoints];
        self::assertCount(1, $points, 'Two workflows with different IDs share one bounded series.');
        self::assertSame(2, (int) $points[0]->value);
        self::assertSame(self::TAGS, $points[0]->attributes->toArray());

        $histogram = $metrics['durable_workflow_client_request_duration_seconds'];
        self::assertSame($units['durable_workflow_client_request_duration_seconds'], $histogram->unit);
        self::assertInstanceOf(Histogram::class, $histogram->data);
        $points = [...$histogram->data->dataPoints];
        self::assertCount(1, $points);
        self::assertSame(2, $points[0]->count);
        self::assertSame(self::TAGS, $points[0]->attributes->toArray());

        self::assertStringNotContainsString('private-', serialize(array_map(
            static fn ($metric): array => [$metric->name, $metric->unit], $metrics)));
    }
}
