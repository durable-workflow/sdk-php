<?php

declare(strict_types=1);

namespace DurableWorkflow\Metrics;

/** @internal Measures the parent worker's dispatch and observed result. */
final class WorkerTaskObservation
{
    private readonly int $started;
    private string $outcome = 'completed';

    public function __construct(
        private readonly MetricsObserver $observer,
        private readonly string $kind,
        private readonly string $queue,
    ) {
        $this->started = hrtime(true);
    }

    public function outcome(string $outcome): void
    {
        $this->outcome = $outcome;
    }

    public function finish(): void
    {
        $this->observer->observe('worker_task', (hrtime(true) - $this->started) / 1e9,
            ['task_kind' => $this->kind, 'task_queue' => $this->queue, 'outcome' => $this->outcome]);
    }
}
