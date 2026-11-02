<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Metrics\MetricsRecorder;

final class RecordingMetricsRecorder implements MetricsRecorder
{
    /** @var list<array{method: string, name: string, value: float, tags: array<string, string>}> */
    public array $events = [];

    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        $this->events[] = ['method' => 'increment', 'name' => $name, 'value' => $value, 'tags' => $tags];
    }

    public function record(string $name, float $value, array $tags = []): void
    {
        $this->events[] = ['method' => 'record', 'name' => $name, 'value' => $value, 'tags' => $tags];
    }
}
