<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/**
 * Keeps observations in process memory, for tests and diagnostics.
 *
 * Series are keyed by recorder name plus the exact tag set, so counters and
 * duration samples can be read back without an exporter.
 */
final class InMemoryMetricsRecorder implements MetricsRecorder
{
    /** @var array<string, float> */
    private array $counters = [];

    /** @var array<string, list<float>> */
    private array $histograms = [];

    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        $key = self::key($name, $tags);
        $this->counters[$key] = ($this->counters[$key] ?? 0.0) + $value;
    }

    public function record(string $name, float $value, array $tags = []): void
    {
        $this->histograms[self::key($name, $tags)][] = $value;
    }

    /** @param array<string, string> $tags */
    public function counter(string $name, array $tags = []): float
    {
        return $this->counters[self::key($name, $tags)] ?? 0.0;
    }

    /**
     * @param array<string, string> $tags
     * @return list<float>
     */
    public function histogram(string $name, array $tags = []): array
    {
        return $this->histograms[self::key($name, $tags)] ?? [];
    }

    /** @param array<string, string> $tags */
    private static function key(string $name, array $tags): string
    {
        ksort($tags);

        return $name."\0".json_encode($tags, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
