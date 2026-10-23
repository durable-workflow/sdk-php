<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

use Throwable;

/** @internal Optional observers cannot alter a business result or retry decision. */
final class MetricsObserver
{
    public function __construct(public readonly MetricsRecorder $recorder)
    {
    }

    /** @param array<string, string> $tags */
    public function observe(string $scope, float $seconds, array $tags): void
    {
        if ($this->recorder instanceof NoopMetricsRecorder) {
            return;
        }
        try {
            $pair = MetricsContract::pair($scope);
        } catch (Throwable) {
            return;
        }
        try {
            $this->recorder->increment($pair['counter'], 1.0, $tags);
        } catch (Throwable) {
            // A missing observation does not establish zero activity or health.
        }
        try {
            $this->recorder->record($pair['histogram'], max(0.0, $seconds), $tags);
        } catch (Throwable) {
            // Exporter errors never authorize repeating application work.
        }
    }
}
