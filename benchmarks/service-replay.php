<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\HandlerStack;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

// A diagnostic against the disposable claimed-task-recovery topology only.
// This profiles published cold replay/history caching, not retained execution.
if (count($argv) < 5) {
    fwrite(STDERR, "Usage: service-replay.php AUTOLOAD URL ACTIVITY_CYCLES CONCURRENT_RUNS [TIMER_SECONDS]\n");
    exit(2);
}
require_once $argv[1];
$cycles = filter_var($argv[3], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'max_range' => 1000]]);
$runs = filter_var($argv[4], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'max_range' => 16]]);
$timerSeconds = filter_var($argv[5] ?? '1', FILTER_VALIDATE_INT, ['options' => ['min_range' => 0, 'max_range' => 10]]);
if ($cycles === false || $runs === false || $timerSeconds === false) {
    throw new InvalidArgumentException('Use 1–1000 activity cycles, 1–16 runs and a 0–10 second final timer.');
}

function profileCpuSeconds(): float
{
    $usage = getrusage();

    return $usage['ru_utime.tv_sec'] + $usage['ru_utime.tv_usec'] / 1e6
        + $usage['ru_stime.tv_sec'] + $usage['ru_stime.tv_usec'] / 1e6;
}

/** @param list<float> $samples
 * @return array<string, int|float|null>
 */
function profileDistribution(array $samples): array
{
    sort($samples);
    $quantile = static fn (float $fraction): ?float => $samples === [] ? null
        : $samples[max(0, (int) ceil(count($samples) * $fraction) - 1)];

    return ['count' => count($samples), 'sum' => array_sum($samples),
        'p50' => $quantile(0.5), 'p95' => $quantile(0.95), 'p99' => $quantile(0.99)];
}

$suffix = bin2hex(random_bytes(6));
$namespace = 'php-profile-'.$suffix;
$queue = 'profile-'.$suffix;
$workerId = 'profile-worker-'.$suffix;
$buildId = getenv('DW_PROFILE_BUILD_ID') ?: null;
$admin = new Client($argv[2], controlToken: 'claimed-task-admin');
$admin->createNamespace($namespace);
$serverVersion = $admin->clusterInfo()->version;
$control = new Client($argv[2], namespace: $namespace, controlToken: 'claimed-task-client');
$active = false;
$worker = null;
$http = [];
$claims = [];
$completed = [];
$effects = [];
$diagnostics = [];
$profileDeadline = INF;
$expired = false;
$stack = HandlerStack::create();
$stack->push(static function (callable $next) use (&$active, &$http, &$claims, &$completed, &$worker, &$profileDeadline, &$expired, $runs): Closure {
    return static function (RequestInterface $request, array $options) use (
    $next, &$active, &$http, &$claims, &$completed, &$worker, &$profileDeadline, &$expired, $runs,
) {
    $started = hrtime(true);
    $sent = $active;

    return $next($request, $options)->then(static function (ResponseInterface $response) use (
        $request, $started, $sent, &$http, &$claims, &$completed, &$worker, &$profileDeadline, &$expired, $runs,
    ): ResponseInterface {
        if (!$sent) {
            return $response;
        }
        if (microtime(true) > $profileDeadline) {
            $expired = true;
            $worker?->requestShutdown();
        }
        $path = $request->getUri()->getPath();
        $category = match (true) {
            str_ends_with($path, '/poll') => 'poll',
            str_ends_with($path, '/history') => 'history_page',
            str_ends_with($path, '/complete') => 'completion',
            str_ends_with($path, '/heartbeat') => 'heartbeat',
            default => 'other',
        };
        $body = (string) $response->getBody();
        $decoded = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
        $task = is_array($decoded) ? ($decoded['task'] ?? null) : null;
        if (getenv('DW_PROFILE_TRACE') === '1') {
            fwrite(STDERR, json_encode(['path' => $path, 'status' => $response->getStatusCode(),
                'poll_status' => $decoded['poll_status'] ?? null, 'reason' => $decoded['reason'] ?? null,
                'task_id' => $task['task_id'] ?? null, 'commands' => str_ends_with($path, '/complete')
                    ? json_decode((string) $request->getBody(), true)['commands'] ?? null : null], JSON_THROW_ON_ERROR)."\n");
        }
        $entry = ['seconds' => (hrtime(true) - $started) / 1e9, 'bytes' => strlen($body),
            'status' => $response->getStatusCode()];
        if (is_array($task)) {
            $claims[] = ['task_id' => $task['task_id'] ?? null, 'lease_owner' => $task['lease_owner'] ?? null,
                'attempt' => $task['workflow_task_attempt'] ?? $task['attempt_number'] ?? null,
                'workflow_id' => $task['workflow_id'] ?? null, 'run_id' => $task['run_id'] ?? null];
            $entry['leased_at'] = $task['leased_at'] ?? null;
            $entry['available_at'] = $task['available_at'] ?? null;
        }
        $http[$category][] = $entry;
        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300
            && preg_match('~/worker/workflow-tasks/([^/]+)/complete$~D', $path, $match) === 1) {
            $document = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
            foreach ($document['commands'] ?? [] as $command) {
                if (($command['type'] ?? null) === 'complete_workflow') {
                    $completed[$match[1]] = true;
                }
            }
            if (count($completed) === $runs) {
                $worker?->requestShutdown();
            }
        }

        return $response;
    });
    };
});
$transport = new Psr18Transport(new GuzzleHttp\Client(['handler' => $stack, 'http_errors' => false,
    'timeout' => 65, 'connect_timeout' => 5]));
$client = new Client($argv[2], namespace: $namespace, workerToken: 'claimed-task-worker', transport: $transport);
putenv('DW_PROFILE_URL='.$argv[2]);
putenv('DW_PROFILE_NAMESPACE='.$namespace);
$client = $client->withActivityLeaseBootstrap(__DIR__.'/service-profile-lease-bootstrap.php');
$handler = static function (WorkflowContext $context, string $key, int $count, int $timer): array {
    $sum = 0;
    for ($index = 0; $index < $count; ++$index) {
        $sum += $context->activity('qualification.profile.echo', [$key, $index]);
    }
    if ($timer > 0) {
        $context->sleep($timer);
    }

    return ['key' => $key, 'sum' => $sum];
};
$handles = [];
for ($index = 0; $index < $runs; ++$index) {
    $key = 'run-'.$suffix.'-'.$index;
    $handles[$key] = $control->startWorkflow('qualification.profile.replay', $key, $queue,
        [$key, $cycles, $timerSeconds], buildId: $buildId);
}
$worker = new Worker($client, $queue, $workerId, buildId: $buildId,
    diagnosticListener: static function (string $event, array $context) use (&$diagnostics, &$worker): void {
        if (isset($context['exception'])) {
            $diagnostics[] = ['event' => $event, 'exception' => $context['exception']::class,
                'message' => $context['exception']->getMessage()];
            $worker?->requestShutdown();
        }
    });
$worker->registerWorkflow('qualification.profile.replay', $handler);
$worker->registerActivity('qualification.profile.echo', static function (ActivityContext $context, string $key, int $index) use (&$effects): int {
    $effectKey = $key.':'.$index;
    $effects[$effectKey] = ($effects[$effectKey] ?? 0) + 1;

    return $index;
});
$receipt = ['sdk_version' => InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'sdk_reference' => InstalledVersions::getReference('durable-workflow/sdk'), 'server_version' => $serverVersion,
    'php_version' => PHP_VERSION, 'namespace' => $namespace, 'worker_id' => $workerId, 'build_id' => $buildId,
    'cycles_per_run' => $cycles, 'concurrent_runs' => $runs, 'timer_seconds' => $timerSeconds,
    'worker_memory_limit_bytes' => trim((string) @file_get_contents('/sys/fs/cgroup/memory.max')),
    'worker_cpu_limit' => trim((string) @file_get_contents('/sys/fs/cgroup/cpu.max'))];
$started = hrtime(true);
$cpu = profileCpuSeconds();
$profileDeadline = microtime(true) + 600;
$active = true;
try {
    $worker->run(1);
    $receipt['outcome'] = $expired ? 'profile_error' : 'measured';
    if ($expired) {
        $receipt['error'] = ['type' => 'profile_deadline', 'message' => 'The bounded 600-second profiling trial did not drain.'];
    }
} catch (Throwable $exception) {
    $receipt['outcome'] = 'profile_error';
    $receipt['error'] = ['type' => $exception::class, 'message' => $exception->getMessage()];
} finally {
    $active = false;
    $receipt['worker_wall_seconds'] = (hrtime(true) - $started) / 1e9;
    $receipt['worker_cpu_seconds'] = profileCpuSeconds() - $cpu;
    $receipt['worker_maxrss_kib'] = getrusage()['ru_maxrss'];
    $receipt['worker_cgroup_peak_bytes'] = trim((string) @file_get_contents('/sys/fs/cgroup/memory.peak'));
    $receipt['worker_cgroup_memory_events'] = trim((string) @file_get_contents('/sys/fs/cgroup/memory.events'));
    $receipt['child_process_usage'] = getrusage(1);
    $receipt['sticky_cache'] = $worker->stickyCacheMetrics();
    $receipt['completion_count'] = count($completed);
    $receipt['effect_count'] = count($effects);
    $receipt['duplicate_effect_count'] = count(array_filter($effects, static fn (int $count): bool => $count !== 1));
    $receipt['diagnostics'] = $diagnostics;
    $receipt['claims'] = $claims;
    foreach ($http as $category => $entries) {
        $receipt['http'][$category] = ['duration_seconds' => profileDistribution(array_column($entries, 'seconds')),
            'response_bytes' => array_sum(array_column($entries, 'bytes')),
            'statuses' => array_count_values(array_column($entries, 'status'))];
    }
}
if ($receipt['outcome'] === 'measured') {
    try {
        foreach ($handles as $key => $handle) {
            $execution = $control->describeWorkflow($handle->workflowId, $handle->selectedRunId);
            $expected = ['key' => $key, 'sum' => intdiv($cycles * ($cycles - 1), 2)];
            if ($execution->status !== 'completed' || $execution->output !== $expected) {
                throw new RuntimeException('A workflow did not complete with the exact expected result.');
            }
            $history = [];
            $token = null;
            do {
                $page = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100, $token);
                array_push($history, ...($page['events'] ?? $page['history_events'] ?? []));
                $token = $page['next_page_token'] ?? null;
            } while ($token !== null);
            $queueWaits = [];
            $seenTasks = [];
            $timers = [];
            foreach ($history as $event) {
                $eventTask = $event['payload']['task'] ?? null;
                if (is_array($eventTask) && !isset($seenTasks[$eventTask['id'] ?? ''])
                    && isset($eventTask['available_at'], $eventTask['leased_at'])) {
                    $seenTasks[$eventTask['id']] = true;
                    $queueWaits[] = (float) (new DateTimeImmutable($eventTask['leased_at']))->format('U.u')
                        - (float) (new DateTimeImmutable($eventTask['available_at']))->format('U.u');
                }
                if (str_starts_with($event['event_type'], 'Timer')) {
                    $timers[] = $event;
                }
            }
            $samples = [];
            $cpuSamples = [];
            for ($pass = 0; $pass < 3; ++$pass) {
                $before = hrtime(true);
                $beforeCpu = profileCpuSeconds();
                $replay = (new Replayer($control->payloadCodec()))->replay($handler, $history,
                    [$key, $cycles, $timerSeconds], $queue);
                $cpuSamples[] = profileCpuSeconds() - $beforeCpu;
                $samples[] = (hrtime(true) - $before) / 1e9;
                if ($replay->terminalFailure !== null || count($replay->commands) !== 1
                    || $replay->commands[0]['type'] !== 'complete_workflow'
                    || $control->payloadCodec()->decodeEnvelope($replay->commands[0]['result']) !== $expected) {
                    throw new RuntimeException('Isolated canonical-history replay changed its command or result.');
                }
            }
            $receipt['runs'][] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
                'event_count' => count($history), 'event_types' => array_count_values(array_column($history, 'event_type')),
                'history_sha256' => hash('sha256', json_encode($history, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION)),
                'canonical_queue_wait_seconds' => profileDistribution($queueWaits), 'timers' => $timers,
                'isolated_replay_wall_seconds' => $samples, 'isolated_replay_cpu_seconds' => $cpuSamples];
            foreach ([10, 100, 1000] as $eventCount) {
                if (count($history) < $eventCount) {
                    continue;
                }
                $prefix = array_slice($history, 0, $eventCount);
                $baseline = null;
                $prefixSamples = [];
                for ($pass = 0; $pass < 3; ++$pass) {
                    $before = profileCpuSeconds();
                    $replay = (new Replayer($control->payloadCodec()))->replay($handler, $prefix,
                        [$key, $cycles, $timerSeconds], $queue);
                    $prefixSamples[] = profileCpuSeconds() - $before;
                    $commands = json_encode($replay->commands, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
                    if ($replay->terminalFailure !== null || ($baseline !== null && $baseline !== $commands)) {
                        throw new RuntimeException('Cold prefix replay changed its commands or failed.');
                    }
                    $baseline = $commands;
                }
                $receipt['prefix_replays'][] = ['workflow_id' => $handle->workflowId, 'event_count' => $eventCount,
                    'history_sha256' => hash('sha256', json_encode($prefix, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION)),
                    'cpu_seconds' => $prefixSamples, 'command_types' => array_column($replay->commands, 'type'),
                    'commands_sha256' => hash('sha256', (string) $baseline)];
            }
        }
        if ($receipt['completion_count'] !== $runs || $receipt['effect_count'] !== $runs * $cycles
            || $receipt['duplicate_effect_count'] !== 0 || $diagnostics !== []) {
            throw new RuntimeException('Completion, effect or diagnostic acceptance failed.');
        }
    } catch (Throwable $exception) {
        $receipt['outcome'] = 'profile_error';
        $receipt['error'] = ['type' => $exception::class, 'message' => $exception->getMessage()];
    }
}
$receipt['final_maxrss_kib'] = getrusage()['ru_maxrss'];
echo json_encode($receipt, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
    | JSON_THROW_ON_ERROR)."\n";
exit($receipt['outcome'] === 'measured' ? 0 : 1);
