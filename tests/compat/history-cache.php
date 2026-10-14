<?php

declare(strict_types=1);

use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

/** Exercises configured bridge workers through polling and authoritative cold replay. */
final class ConfiguredHistoryCacheTransport implements Transport
{
    public int $historyFetches = 0;
    public int $completions = 0;
    private int $polls = 0;

    /** @return array<string, mixed> */
    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        $history = [['event_type' => 'WorkflowStarted', 'payload' => ['sequence' => 0]]];
        if (str_ends_with($uri, '/worker/register')) {
            return ['registered' => true, 'heartbeat_interval_seconds' => 10];
        }
        if (str_ends_with($uri, '/workflow-tasks/poll')) {
            ++$this->polls;
            if ($this->polls > 3) {
                return ['poll_status' => 'stopped', 'reason' => 'worker_stopped', 'task' => null];
            }
            $identity = $this->polls === 2 ? 'other' : 'original';

            return ['poll_status' => 'leased', 'task' => [
                'task_id' => 'cache-task-'.$this->polls,
                'workflow_task_attempt' => 1,
                'lease_owner' => 'cache-worker',
                'workflow_id' => 'cache-workflow-'.$identity,
                'run_id' => 'cache-run-'.$identity,
                'workflow_type' => 'cache.configuration',
                'payload_codec' => 'avro',
                'history_events' => $this->polls === 3 ? [] : $history,
                'sticky_replay_mode' => $this->polls === 3 ? 'sticky_hit_expected' : 'cold_replay',
            ]];
        }
        if (str_ends_with($uri, '/history')) {
            ++$this->historyFetches;

            return ['history_events' => $history, 'next_history_page_token' => null];
        }
        if (str_ends_with($uri, '/heartbeat')) {
            return [
                'task_id' => 'cache-task-'.$this->polls,
                'workflow_task_attempt' => 1,
                'lease_owner' => 'cache-worker',
                'renewed' => true,
            ];
        }
        if (str_ends_with($uri, '/complete')) {
            if (($body['sticky_cache']['ttl_seconds'] ?? null) !== 37
                || ($body['commands'][0]['type'] ?? null) !== 'complete_workflow') {
                throw new RuntimeException('Configured cache TTL or replay completion was not preserved.');
            }
            ++$this->completions;

            return ['completed' => true];
        }
        if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
            return ['poll_status' => 'empty', 'task' => null];
        }

        throw new RuntimeException("Unexpected configured cache request: {$method} {$uri}");
    }
}

/**
 * @param Closure(Transport): Worker $makeWorker
 * @return array{hit: int, miss: int, eviction: int, forced_cold_replay: int}
 */
function verifyConfiguredHistoryCache(Closure $makeWorker): array
{
    $transport = new ConfiguredHistoryCacheTransport();
    $worker = $makeWorker($transport);
    $worker->registerWorkflow('cache.configuration', static fn (WorkflowContext $context): string => 'same-result');
    for ($index = 0; $index < 12 && $transport->completions < 3; ++$index) {
        $worker->tick(0);
    }
    $worker->run(0);
    $metrics = $worker->stickyCacheMetrics();
    if ($transport->historyFetches !== 1 || $transport->completions !== 3
        || $metrics['eviction'] < 1 || $metrics['forced_cold_replay'] !== 1) {
        throw new RuntimeException('Configured capacity did not evict and fetch authoritative cold history.');
    }

    return $metrics;
}
