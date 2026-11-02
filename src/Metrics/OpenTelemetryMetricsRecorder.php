<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

use DurableWorkflow\SdkIdentity;
use OpenTelemetry\API\Metrics\CounterInterface;
use OpenTelemetry\API\Metrics\HistogramInterface;
use OpenTelemetry\API\Metrics\MeterInterface;
use OpenTelemetry\API\Metrics\MeterProviderInterface;

/**
 * Forwards SDK observations to the application's OpenTelemetry meter provider.
 *
 * Requires the optional `open-telemetry/api` package, which the SDK already
 * suggests for tracing. The application owns the provider, its readers and
 * exporters, and any resource attributes. Counters use unit "1" and durations
 * use seconds ("s"), matching the shared SDK metrics contract.
 */
final class OpenTelemetryMetricsRecorder implements MetricsRecorder
{
    private readonly MeterInterface $meter;

    /** @var array<string, CounterInterface> */
    private array $counters = [];

    /** @var array<string, HistogramInterface> */
    private array $histograms = [];

    public function __construct(MeterProviderInterface $meterProvider, string $instrumentationScope = 'durable-workflow/sdk')
    {
        $this->meter = $meterProvider->getMeter($instrumentationScope, SdkIdentity::version());
    }

    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        ($this->counters[$name] ??= $this->meter->createCounter($name, '1', 'Durable Workflow SDK '.$name))
            ->add($value, $tags);
    }

    public function record(string $name, float $value, array $tags = []): void
    {
        ($this->histograms[$name] ??= $this->meter->createHistogram($name, 's', 'Durable Workflow SDK '.$name))
            ->record($value, $tags);
    }
}
