<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Metrics\InMemoryMetricsRecorder;
use DurableWorkflow\Metrics\OpenTelemetryMetricsRecorder;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
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

    public function testActualActivityDispatchExportsTheScheduleToStartHistogram(): void
    {
        $exporter = new InMemoryExporter();
        $reader = new ExportingReader($exporter);
        $provider = MeterProvider::builder()->addReader($reader)->build();
        $client = new Client('https://server.invalid', metrics: new OpenTelemetryMetricsRecorder($provider),
            transport: new FakeTransport([
                ['worker_protocol' => ['server_capabilities' => ['claimed_task_ready_age' => true]]],
                ['task' => null, 'poll_status' => 'empty'],
                ['task' => ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1', 'activity_type' => 'orders.simple', 'payload_codec' => 'avro', 'ready_age_ms' => 1500],
                    'poll_status' => 'leased'],
                ['recorded' => true, 'outcome' => 'completed'],
                ['task' => null, 'poll_status' => 'empty'],
            ]));
        $client->workerClusterInfo();
        $worker = new Worker($client, 'orders', workerId: 'worker-1', clock: static fn (): float => 0);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        $reader->collect();
        $metrics = [];
        foreach ($exporter->collect() as $metric) {
            $metrics[$metric->name] = $metric;
        }
        $metric = $metrics['durable_workflow_worker_task_schedule_to_start_seconds'];
        self::assertSame('s', $metric->unit);
        self::assertInstanceOf(Histogram::class, $metric->data);
        $points = [...$metric->data->dataPoints];
        self::assertCount(1, $points);
        self::assertSame(1, $points[0]->count);
        self::assertSame(1.5, $points[0]->sum);
        self::assertSame(['task_kind' => 'activity', 'task_queue' => 'orders'], $points[0]->attributes->toArray());
    }
}
