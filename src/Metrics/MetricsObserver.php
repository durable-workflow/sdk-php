<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

use Throwable;

/** @internal Optional observers cannot alter a business result or retry decision. */
final class MetricsObserver
{
    private readonly int $ownerPid;
    private bool $recording = false;

    public function __construct(public readonly MetricsRecorder $recorder)
    {
        $this->ownerPid = function_exists('getmypid') ? (int) getmypid() : 0;
    }

    public function enabled(): bool
    {
        return !$this->recorder instanceof NoopMetricsRecorder && $this->ownerPid !== 0
            && getmypid() === $this->ownerPid && !$this->recording;
    }

    /** @param array<string, string> $tags */
    public function observe(string $scope, float $seconds, array $tags): void
    {
        if (!$this->enabled()) {
            return;
        }
        try {
            $pair = MetricsContract::pair($scope);
        } catch (Throwable) {
            return;
        }
        $this->recording = true;
        try {
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
        } finally {
            $this->recording = false;
        }
    }
}
