<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/** Default recorder; the application opts into exporting observations. */
final class NoopMetricsRecorder implements MetricsRecorder
{
    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        // The default recorder intentionally discards observations.
    }

    public function record(string $name, float $value, array $tags = []): void
    {
        // The default recorder intentionally discards observations.
    }
}
