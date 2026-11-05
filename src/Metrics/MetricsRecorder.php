<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/** Application-owned sink for process observations, with no exporter dependency. */
interface MetricsRecorder
{
    /** @param array<string, string> $tags */
    public function increment(string $name, float $value = 1.0, array $tags = []): void;

    /** @param array<string, string> $tags */
    public function record(string $name, float $value, array $tags = []): void;
}
