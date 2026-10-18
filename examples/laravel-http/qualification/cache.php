<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\WorkflowHandle;

[$script, $mode, $prefix, $checkpointPath] = $argv + array_fill(0, 4, null);
if (!in_array($mode, ['expiry', 'eviction', 'checkpoint', 'resume'], true)
    || !is_string($prefix) || $prefix === '' || !is_string($checkpointPath) || $checkpointPath === '') {
    throw new InvalidArgumentException('Usage: cache.php expiry|eviction|checkpoint|resume FRESH_PREFIX PRIVATE_CHECKPOINT');
}
$origin = getenv('DURABLE_WORKFLOW_RUNTIME_URL');
$namespace = getenv('DURABLE_WORKFLOW_NAMESPACE');
if (!$origin || !$namespace) throw new InvalidArgumentException('Configure the fixed runtime and namespace.');
$client = new Client($origin, namespace: $namespace, controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN') ?: null);

function snapshot(Client $client, WorkflowHandle $handle): array
{
    $execution = $handle->describeSelectedRun();
    $page = $client->workflowHistory($handle->workflowId, $handle->selectedRunId, pageSize: 100);
    $events = $page['events'] ?? null;
    if (($page['next_page_token'] ?? null) !== null || !is_array($events) || $events === [] || !array_is_list($events)) {
        throw new RuntimeException('The complete bounded canonical history is unavailable.');
    }
    $deadlines = [];
    foreach (['run_deadline_at', 'execution_deadline_at'] as $name) {
        $deadlines[$name] = $execution->raw[$name] ?? null;
        if (!is_string($deadlines[$name]) || $deadlines[$name] === '') throw new RuntimeException('Missing original deadline.');
    }

    return ['status' => strtolower((string) $execution->status), 'events' => $events,
        'counts' => array_count_values(array_column($events, 'event_type')), 'deadlines' => $deadlines];
}

function remaining(float $cutoff): float
{
    $seconds = $cutoff - microtime(true);
    if ($seconds <= 0) throw new RuntimeException('The original 90-second observation expired.');

    return $seconds;
}

if ($mode === 'resume') {
    $checkpoint = json_decode(file_get_contents($checkpointPath), true, 512, JSON_THROW_ON_ERROR);
    if ($checkpoint['origin'] !== $origin || $checkpoint['namespace'] !== $namespace || $checkpoint['prefix'] !== $prefix
        || $checkpoint['mode'] !== 'checkpoint') throw new RuntimeException('Checkpoint authority or identity differs.');
    if (!is_array($checkpoint['originals'] ?? null) || count($checkpoint['originals']) !== 1
        || !is_array($checkpoint['originals'][0]['before'] ?? null)) {
        throw new RuntimeException('The original waiting checkpoint was not completed.');
    }
    $cutoff = $checkpoint['cutoff'];
    remaining($cutoff);
} else {
    if (file_exists($checkpointPath)) throw new RuntimeException('Use a fresh checkpoint; do not overwrite an original.');
    $cutoff = microtime(true) + 90;
    $checkpoint = ['origin' => $origin, 'namespace' => $namespace, 'prefix' => $prefix, 'mode' => $mode,
        'cutoff' => $cutoff, 'originals' => []];
    foreach ($mode === 'eviction' ? ['first', 'neighbor'] : ['original'] as $suffix) {
        $handle = $client->startWorkflow('example.http', $prefix.'-'.$suffix,
            getenv('DURABLE_WORKFLOW_TASK_QUEUE') ?: 'service-http-example', ['alpha', 'signal'],
            executionTimeoutSeconds: 900, runTimeoutSeconds: 600);
        $checkpoint['originals'][] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId, 'before' => null];
        if (file_put_contents($checkpointPath, json_encode($checkpoint, JSON_THROW_ON_ERROR)) === false) {
            throw new RuntimeException('Cannot retain the admitted original identity.');
        }
        do {
            remaining($cutoff);
            $before = snapshot($client, $handle);
            if ($before['status'] === 'waiting' && ($before['counts']['ActivityCompleted'] ?? 0) === 1) break;
            usleep(200000);
        } while (true);
        if (isset($before['counts']['WorkflowCompleted'])) throw new RuntimeException('Premature completion.');
        $checkpoint['originals'][array_key_last($checkpoint['originals'])]['before'] = $before;
        if (file_put_contents($checkpointPath, json_encode($checkpoint, JSON_THROW_ON_ERROR)) === false) {
            throw new RuntimeException('Cannot write the private checkpoint.');
        }
    }
    if ($mode === 'checkpoint') {
        echo json_encode(['phase' => 'waiting', 'originals' => array_map(
            static fn (array $original): array => array_intersect_key($original, array_flip(['workflow_id', 'run_id'])),
            $checkpoint['originals'],
        ), 'observation_cutoff' => $cutoff], JSON_THROW_ON_ERROR), PHP_EOL;
        exit;
    }
    if ($mode === 'expiry') sleep(3);
}

$results = [];
foreach ($checkpoint['originals'] as $original) {
    remaining($cutoff);
    $handle = $client->workflowHandle($original['workflow_id'], $original['run_id']);
    $handle->signalSelectedRun('release');
    do {
        remaining($cutoff);
        $after = snapshot($client, $handle);
        if ($after['status'] === 'completed') break;
        usleep(200000);
    } while (true);
    $before = $original['before'];
    if ($before['deadlines'] !== $after['deadlines'] || array_slice($after['events'], 0, count($before['events'])) !== $before['events']) {
        throw new RuntimeException('Original history prefix or deadlines changed.');
    }
    foreach (['ActivityScheduled', 'ActivityCompleted', 'WorkflowCompleted'] as $event) {
        if (($after['counts'][$event] ?? 0) !== 1) throw new RuntimeException('Missing or duplicated '.$event);
    }
    if ($handle->resultOfSelectedRun(remaining($cutoff)) !== ['user' => 'alpha', 'echo' => 'hello alpha',
        'number' => 9007199254740993, 'nullable' => null, 'text' => '雪']) throw new RuntimeException('Typed output changed.');
    $results[] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
        'events' => $after['counts'], 'prefix_preserved' => true, 'deadlines' => $after['deadlines'], 'typed_output_exact' => true];
}
echo json_encode(['mode' => $mode, 'originals' => $results], JSON_THROW_ON_ERROR), PHP_EOL;
