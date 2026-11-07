<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use ReflectionClass;
use Throwable;

/** Fixed real-socket SDK workload; usable with the predecessor or candidate source. */
final class WorkerPipelineScenario
{
    /** @param array<string, mixed> $observed
     * @return list<string>
     */
    public static function completionOrder(array $observed): array
    {
        $selected = [];
        foreach ($observed['events'] as $index => $event) {
            $label = match ([$event['kind'], $event['task']]) {
                ['completion_sent', 2] => 'parent_sent',
                ['callback', 3] => 'next_work',
                ['completion_answer', 2] => 'parent_answer',
                ['completion_sent', 3] => 'next_completion',
                default => null,
            };
            if ($label !== null) $selected[$index] = $label;
        }
        ksort($selected);
        return array_values($selected);
    }

    /**
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    public static function run(?bool $enabled, array $options = []): array
    {
        return CompletionPipelineScenario::run(static function (string $uri, string $path) use ($enabled, $options): array {
            $options += ['workflow_carrier' => true, 'task_count' => 3, 'hold_task' => 2, 'local_task' => 2];
            file_put_contents($path, json_encode(['events' => [], 'options' => $options], JSON_THROW_ON_ERROR));
            $mode = $options['mode'] ?? 'normal';
            $record = static function (string $kind, int $task) use ($path, $options): void {
                if ($task === $options['hold_task'] + 1 && $kind === 'callback' && !($options['oversized_parent_result'] ?? false)) {
                    $deadline = microtime(true) + 1;
                    do {
                        $received = false;
                        foreach (CompletionPipelineScenario::events($path) as $event) {
                            if ($event['kind'] === 'completion_sent' && $event['task'] === $options['hold_task']) $received = true;
                        }
                        if ($received) break;
                        usleep(1000);
                    } while (microtime(true) < $deadline);
                    if (!$received) throw new \RuntimeException('The socket fixture did not receive the parent body.');
                }
                $handle = fopen($path, 'c+');
                flock($handle, LOCK_EX);
                $state = json_decode(stream_get_contents($handle), true, flags: JSON_THROW_ON_ERROR);
                $state['events'][] = ['kind' => $kind, 'task' => $task];
                if ($task === $options['hold_task'] + 1 && $kind === 'callback' && ($options['release_parent'] ?? true)) {
                    $state['release_parent'] = true;
                }
                rewind($handle);
                ftruncate($handle, 0);
                fwrite($handle, json_encode($state, JSON_THROW_ON_ERROR));
                flock($handle, LOCK_UN);
                fclose($handle);
            };
            $recorder = new RecordingMetricsRecorder();
            $client = new Client($uri, workerToken: 'fixture-worker',
                metrics: $mode === 'client_metrics' ? $recorder : null,
                workerRequestTimeoutSeconds: $options['worker_timeout'] ?? null);
            if ($mode === 'client_telemetry') $client = $client->withTelemetry(new ExecutionTelemetry());
            $arguments = ['workerId' => 'pipeline-worker'];
            $retryObservations = [];
            $arguments['transientPollRetryObserver'] = static function (string $kind, int $attempt, float $delay) use (&$retryObservations): void {
                $retryObservations[] = ['kind' => $kind, 'attempt' => $attempt, 'delay' => $delay];
            };
            $parameters = array_map(static fn ($parameter): string => $parameter->getName(), (new ReflectionClass(Worker::class))->getConstructor()->getParameters());
            // The same consumer must exercise old synchronous behavior, not fail on a new named argument.
            if ($enabled !== null && in_array('enableCompletionPipelining', $parameters, true)) {
                $arguments['enableCompletionPipelining'] = $enabled;
            }
            if ($mode === 'worker_metrics') $arguments['metrics'] = $recorder;
            if ($mode === 'worker_telemetry') $arguments['telemetry'] = new ExecutionTelemetry();
            if ($mode === 'cooperative') $arguments['enableCooperativeCancellation'] = true;
            $worker = new Worker($client, 'pipeline', ...$arguments);
            $calls = [];
            $worker->registerWorkflow('pipeline.workflow', static function (WorkflowContext $context, int $task) use ($record, &$calls, &$worker, $options, $mode): mixed {
                $record('callback', $task);
                $calls[] = $task;
                $local = $task === $options['local_task'] || (($options['local_all'] ?? false) && $task >= $options['local_task']);
                if ($local && $mode !== 'no_local') {
                    try {
                        $context->localActivity('pipeline.local', [$task], $mode === 'retried_failure'
                            ? ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]]] : []);
                    } catch (ActivityFailed $error) {
                        if ($mode !== 'kept_failure') throw $error;
                    }
                }
                if ($local && $mode === 'schedule_activity') return $context->activity('pipeline.remote');
                if ($mode === 'shutdown' && $task === $options['hold_task'] + 1) $worker->requestShutdown();
                if ($mode === 'signal' && $task === $options['hold_task'] + 1) posix_kill(getmypid(), SIGTERM);
                if ($task === $options['hold_task'] && ($options['oversized_parent_result'] ?? false)) return str_repeat('x', 2048);
                return $task;
            });
            $localAttempts = 0;
            $worker->registerActivity('pipeline.local', static function (ActivityContext $context, int $task) use ($record, $mode, &$localAttempts): int {
                $record('local', $task);
                if ($mode === 'retried_failure' && ++$localAttempts === 1) throw new \RuntimeException('The first local failure is retained in the retry history.');
                if ($mode === 'kept_failure') throw new \RuntimeException('The local failure is deliberately retained.');
                usleep(20_000);
                return $task;
            });
            $error = null;
            try {
                if ($mode === 'tick') {
                    for ($turn = 0; $turn < 10 && $worker->tick(0); ++$turn) {}
                } else {
                    $worker->run(0);
                }
            } catch (Throwable $failure) {
                $error = $failure;
            }
            $armReusable = null;
            if (method_exists($client, 'armWorkflowCompletion')) {
                $owner = new \Fiber(static fn () => null);
                $armReusable = $client->armWorkflowCompletion($owner, 'probe');
                $client->disarmWorkflowCompletion($owner);
            }
            $state = json_decode(file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
            return $state + ['callbacks' => $calls, 'error' => $error, 'metrics' => $recorder->events,
                'arm_reusable' => $armReusable, 'retries' => $retryObservations];
        }, __DIR__.'/completion-worker-http.php', true);
    }
}
