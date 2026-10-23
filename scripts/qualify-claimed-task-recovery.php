<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url, $admin] = $argv;
require $autoload;

/** Exercise the default installed transport; HAProxy owns all injected failures. */
function route(string $admin, string $pattern, string $backend): void
{
    $peer = stream_socket_client($admin, $errno, $error, 3);
    if ($peer === false) {
        throw new RuntimeException($error);
    }
    stream_set_timeout($peer, 3);
    fwrite($peer, 'set map /usr/local/etc/haproxy/routes.map '.$pattern.' '.$backend."\n");
    stream_socket_shutdown($peer, STREAM_SHUT_WR);
    $reply = stream_get_contents($peer);
    fclose($peer);
    if (trim($reply) !== '') {
        throw new RuntimeException('HAProxy map change failed: '.$reply);
    }
}

function record(string $file, array $entry): void
{
    file_put_contents($file, json_encode($entry, JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);
}

if (($argv[4] ?? null) === '--worker') {
    [$phase, $queue, $directory, $pattern] = array_slice($argv, 5);
    $client = new Client($url, workerToken: 'claimed-task-worker');
    $worker = new Worker($client, $queue, workerId: $queue,
        diagnosticListener: static function (string $event, array $context) use ($admin, $pattern, $directory, $phase): void {
            $entry = ['event' => $event, 'pid' => getmypid(), 'operation' => $context['operation'] ?? null];
            if ($event === 'worker.retrying' && ($context['operation'] ?? null) === $phase) {
                $error = $context['exception'] ?? null;
                if (!$error instanceof \DurableWorkflow\Exception\ServerException || !$error->isTransientUpstreamFailure()) {
                    throw new RuntimeException('Expected a non-protocol proxy failure.');
                }
                $entry['http_status'] = $error->status;
                route($admin, $pattern, 'healthy');
            }
            record($directory.'/events', $entry);
        });
    $worker->registerWorkflow('qualification.claimed-task', static function (WorkflowContext $context, string $name) use ($phase): array {
        $value = $phase === 'activity_complete' ? $context->activity('qualification.claimed-task', [$name]) : $name;
        return ['value' => $value, 'max' => PHP_INT_MAX, 'text' => '雪☃', 'null' => null];
    });
    $worker->registerActivity('qualification.claimed-task', static function (ActivityContext $context, string $name) use ($directory): string {
        record($directory.'/activity-calls', ['name' => $name, 'pid' => getmypid()]);
        return $name;
    });
    $worker->run(0);
    exit(0);
}

$client = new Client($url, controlToken: 'claimed-task-client');
$result = ['sdk_version' => InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'sdk_reference' => InstalledVersions::getReference('durable-workflow/sdk'),
    'server_version' => $client->clusterInfo()->version, 'scenarios' => []];
$patterns = [
    'workflow_heartbeat' => '^/api/worker/workflow-tasks/[^/]+/heartbeat$',
    'workflow_complete' => '^/api/worker/workflow-tasks/[^/]+/complete$',
    'activity_complete' => '^/api/worker/activity-tasks/[^/]+/complete$',
];
foreach ($patterns as $phase => $pattern) {
    $queue = 'php-claim-'.bin2hex(random_bytes(6));
    $directory = sys_get_temp_dir().'/'.$queue;
    mkdir($directory, 0700);
    $process = null;
    try {
        route($admin, $pattern, 'killed');
        $process = proc_open([PHP_BINARY, __FILE__, $autoload, $url, $admin, '--worker', $phase, $queue, $directory, $pattern],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', $directory.'/stdout', 'w'], 2 => ['file', $directory.'/stderr', 'w']], $pipes);
        if (!is_resource($process)) {
            throw new RuntimeException('Could not start the managed worker.');
        }
        $pid = proc_get_status($process)['pid'];
        $deadline = microtime(true) + 30;
        do {
            $events = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
                is_file($directory.'/events') ? file($directory.'/events', FILE_IGNORE_NEW_LINES) : []);
            if (!proc_get_status($process)['running'] || microtime(true) >= $deadline) {
                throw new RuntimeException('Worker did not register: '.file_get_contents($directory.'/stderr'));
            }
            usleep(10000);
        } while (!in_array('worker.registered', array_column($events, 'event'), true));

        $runs = [];
        foreach (['first', 'later'] as $name) {
            $handle = $client->startWorkflow('qualification.claimed-task', $queue.'-'.$name, $queue, [$name]);
            $output = $handle->resultOfSelectedRun(30, 0.05);
            if ($output !== ['value' => $name, 'max' => PHP_INT_MAX, 'text' => '雪☃', 'null' => null]) {
                throw new RuntimeException('Unexpected typed result.');
            }
            if (!proc_get_status($process)['running'] || proc_get_status($process)['pid'] !== $pid) {
                throw new RuntimeException('The original worker process exited or was replaced.');
            }
            $events = [];
            $cursor = null;
            do {
                $page = $client->workflowHistory($handle->workflowId, (string) $handle->selectedRunId, 100, $cursor);
                array_push($events, ...($page['events'] ?? $page['history_events'] ?? []));
                $cursor = $page['next_page_token'] ?? null;
            } while ($cursor !== null);
            $counts = array_count_values(array_column($events, 'event_type'));
            if (($counts['WorkflowCompleted'] ?? 0) !== 1 || ($counts['WorkflowFailed'] ?? 0) !== 0
                || ($counts['ActivityCompleted'] ?? 0) !== ($phase === 'activity_complete' ? 1 : 0)) {
                throw new RuntimeException('Canonical history contains an unexpected outcome count.');
            }
            $runs[] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
                'workflow_completions' => $counts['WorkflowCompleted'], 'activity_completions' => $counts['ActivityCompleted'] ?? 0];
        }
        proc_terminate($process);
        $exit = proc_close($process);
        $process = null;
        $events = array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR), file($directory.'/events', FILE_IGNORE_NEW_LINES));
        $retries = array_values(array_filter($events, static fn (array $e): bool => $e['event'] === 'worker.retrying' && $e['operation'] === $phase));
        if ($exit !== 0 || count($retries) !== 1 || $retries[0]['http_status'] !== 503
            || count(array_filter($events, static fn (array $e): bool => $e['event'] === 'worker.registered')) !== 1
            || in_array('worker.failed', array_column($events, 'event'), true)) {
            throw new RuntimeException('Unexpected worker lifecycle: '.file_get_contents($directory.'/stderr'));
        }
        if ($phase === 'activity_complete' && count(file($directory.'/activity-calls')) !== 2) {
            throw new RuntimeException('Activity handler was repeated.');
        }
        $result['scenarios'][$phase] = ['outcome' => 'pass', 'worker_pid' => $pid, 'proxy_http_status' => 503, 'runs' => $runs];
    } catch (Throwable $error) {
        throw new RuntimeException($phase.' failed: '.(is_file($directory.'/stderr') ? file_get_contents($directory.'/stderr') : ''), previous: $error);
    } finally {
        route($admin, $pattern, 'healthy');
        if (is_resource($process)) {
            proc_terminate($process, 9);
            proc_close($process);
        }
        foreach (glob($directory.'/*') as $file) {
            unlink($file);
        }
        rmdir($directory);
    }
}
echo json_encode($result, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE)."\n";
