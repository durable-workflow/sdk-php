<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

require $argv[1];

$startedAt = gmdate('c');
$version = InstalledVersions::getPrettyVersion('durable-workflow/sdk');
$reference = InstalledVersions::getReference('durable-workflow/sdk');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
$implementation = realpath((string) (new ReflectionClass(Client::class))->getFileName());
if ($version !== '3.18.2' || getenv('SDK_VERSION') !== $version
    || $reference !== 'f9483bb2b5b45f591a82d9e5419c5b60a6e967c6'
    || $root === false || $implementation === false || !str_starts_with($implementation, $root.'/')
    || getenv('SERVER_VERSION') !== '3.8.0'
    || getenv('SERVER_IMAGE') !== 'durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e') {
    throw new RuntimeException('Require the reviewed published SDK 3.18.2/native 3.8.0 tuple.');
}
$installed = json_decode(file_get_contents(dirname($argv[1]).'/composer/installed.json'), true, flags: JSON_THROW_ON_ERROR);
$packages = array_values(array_filter($installed['packages'] ?? [], static fn (array $row): bool => $row['name'] === 'durable-workflow/sdk'));
if (count($packages) !== 1 || ($packages[0]['installation-source'] ?? null) !== 'dist'
    || ($packages[0]['source']['reference'] ?? null) !== $reference
    || ($packages[0]['dist']['reference'] ?? null) !== $reference || ($packages[0]['dist']['type'] ?? null) !== 'zip') {
    throw new RuntimeException('Require the exact SDK registry distribution without source fallback.');
}
$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'));
$info = $client->clusterInfo();
if ($info->version !== '3.8.0' || ($info->raw['implementation'] ?? null) !== 'rust') {
    throw new RuntimeException('Require the selected published native producer.');
}
$nonce = bin2hex(random_bytes(8));
$type = 'pagination.workflow.'.$nonce;
$queue = 'published-pagination-'.$nonce;
$worker = new Worker($client, $queue);
$completed = 0;
$handler = static function (WorkflowContext $context, int $value) use (&$completed, $worker): int {
    if (++$completed === 6) {
        $worker->requestShutdown();
    }

    return $value;
};
$worker->registerWorkflow($type, $handler);
$worker->registerWorkflow($type.'.foreign', $handler);
$handles = [];
foreach (range(0, 5) as $index) {
    $handles[] = $client->startWorkflow($index === 5 ? $type.'.foreign' : $type,
        'pagination-'.$nonce.'-'.$index, $queue, [$index], executionTimeoutSeconds: 45, runTimeoutSeconds: 45);
}
$worker->run(0);
foreach ($handles as $index => $handle) {
    if ($handle->resultOfSelectedRun(timeoutSeconds: 5) !== $index) {
        throw new RuntimeException('Every owned workflow must finish with its exact typed result.');
    }
}
$recorder = new class implements MetricsRecorder {
    /** @var list<array<string, string>> */
    public array $requests = [];

    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        if ($name === 'durable_workflow_client_requests') {
            $this->requests[] = $tags;
        }
    }

    public function record(string $name, float $value, array $tags = []): void
    {
    }

    public function count(string $route): int
    {
        return count(array_filter($this->requests, static fn (array $tags): bool =>
            ($tags['method'] ?? null) === 'GET' && ($tags['route'] ?? null) === $route
            && ($tags['status_code'] ?? null) === '200' && ($tags['outcome'] ?? null) === 'ok'));
    }
};
$reader = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'), metrics: $recorder);
$iterator = $reader->iterateWorkflows(workflowType: $type, status: 'completed', pageSize: 2);
if ($recorder->requests !== []) {
    throw new RuntimeException('Constructing a lazy iterator must not make an HTTP request.');
}
$listed = [];
foreach ($iterator as $execution) {
    $listed[] = $execution->workflowId;
    if (count($listed) === 1 && $recorder->count('/workflows') !== 1) {
        throw new RuntimeException('The iterator must yield its first page before fetching subsequent pages.');
    }
}
$listPages = $recorder->count('/workflows');
$expected = array_map(static fn ($handle): string => $handle->workflowId, array_slice($handles, 0, 5));
sort($listed);
sort($expected);
if ($listed !== $expected || $listPages !== 3) {
    throw new RuntimeException('Three real filtered pages must contain all five owned workflows exactly once.');
}
$run = $handles[0];
if ($run->selectedRunId === null) {
    throw new RuntimeException('History must use the exact started run.');
}
$referencePage = $client->workflowHistory($run->workflowId, $run->selectedRunId, pageSize: 100);
$referenceEvents = $referencePage['events'] ?? [];
if (count($referenceEvents) < 3 || count($referenceEvents) > 64 || !empty($referencePage['next_page_token'])) {
    throw new RuntimeException('The bounded reference history must be complete and longer than one two-event page.');
}
$recorder->requests = [];
$history = $reader->iterateWorkflowHistory($run->workflowId, $run->selectedRunId, pageSize: 2);
if ($recorder->requests !== []) {
    throw new RuntimeException('Constructing a history iterator must not make an HTTP request.');
}
$events = [];
$route = '/workflows/{workflow_id}/runs/{run_id}/history';
foreach ($history as $event) {
    $events[] = $event;
    if (count($events) > 64 || (count($events) === 1 && $recorder->count($route) !== 1)) {
        throw new RuntimeException('History must remain bounded and yield its first page before fetching the rest.');
    }
}
$historyPages = $recorder->count($route);
if ($events !== $referenceEvents || $historyPages !== (int) ceil(count($referenceEvents) / 2)
    || $historyPages < 2 || ($client->listWorkers($queue)['workers'] ?? null) !== []) {
    throw new RuntimeException('Real multi-page history must equal the exact-run reference with no gaps, duplicates or live Worker.');
}
echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'server_image' => getenv('SERVER_IMAGE'),
    'runner_commit' => getenv('GITHUB_SHA') ?: null, 'started_at' => $startedAt, 'completed_at' => gmdate('c'),
    'workflow_ids' => $listed, 'excluded_workflow_id' => $handles[5]->workflowId,
    'list_page_size' => 2, 'list_page_requests' => $listPages,
    'history_workflow_id' => $run->workflowId, 'history_run_id' => $run->selectedRunId,
    'history_page_size' => 2, 'history_page_requests' => $historyPages, 'history' => $events,
    'lazy_first_page' => true, 'workflow_completed' => true, 'worker_deregistered' => true, 'source_fallback' => false,
], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES).PHP_EOL;
