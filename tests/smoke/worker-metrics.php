<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

require $argv[1];

$startedAt = gmdate('c');
$package = 'durable-workflow/sdk';
$version = InstalledVersions::getPrettyVersion($package);
$reference = InstalledVersions::getReference($package);
$root = realpath((string) InstalledVersions::getInstallPath($package));
$implementation = realpath((string) (new ReflectionClass(Worker::class))->getFileName());
if ($version !== getenv('SDK_VERSION') || !is_string($reference)
    || preg_match('/\A[0-9a-f]{40}\z/', $reference) !== 1
    || $root === false || $implementation === false || !str_starts_with($implementation, $root.'/')) {
    throw new RuntimeException('Load the exact registry SDK from the isolated consumer, without a source fallback.');
}

$recorder = new class implements MetricsRecorder {
    /** @var list<array{method: string, name: string, value: float, tags: array<string, string>}> */
    public array $events = [];

    public function increment(string $name, float $value = 1.0, array $tags = []): void
    {
        $this->events[] = ['method' => 'increment', 'name' => $name, 'value' => $value, 'tags' => $tags];
    }

    public function record(string $name, float $value, array $tags = []): void
    {
        $this->events[] = ['method' => 'record', 'name' => $name, 'value' => $value, 'tags' => $tags];
    }
};

$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'));
$info = $client->clusterInfo();
if ($info->version !== getenv('SERVER_VERSION') || ($info->raw['implementation'] ?? null) !== 'rust') {
    throw new RuntimeException('The endpoint must be the selected published native Server.');
}
$queue = 'published-worker-metrics';
$worker = new Worker($client, $queue, metrics: $recorder);
$worker->registerActivity('metrics.echo', static fn (ActivityContext $context, string $value): string => $value);
$worker->registerWorkflow('metrics.workflow', static function (WorkflowContext $context) use ($worker): string {
    $result = $context->activity('metrics.echo', ['metrics-ok']);
    $worker->requestShutdown();

    return $result;
});
$handle = $client->startWorkflow('metrics.workflow', 'metrics-'.bin2hex(random_bytes(8)), $queue,
    executionTimeoutSeconds: 60, runTimeoutSeconds: 60);
// Leave the first task eligible before registration so its real ready age is positive.
usleep(200_000);
$worker->run(0);
if ($handle->result(timeoutSeconds: 5) !== 'metrics-ok' || ($client->listWorkers($queue)['workers'] ?? []) !== []) {
    throw new RuntimeException('The workflow must complete and the managed worker must deregister.');
}

$instruments = [
    'durable_workflow_worker_polls' => 'increment',
    'durable_workflow_worker_poll_duration_seconds' => 'record',
    'durable_workflow_worker_tasks' => 'increment',
    'durable_workflow_worker_task_duration_seconds' => 'record',
    'durable_workflow_worker_task_schedule_to_start_seconds' => 'record',
];
$series = [];
foreach ($recorder->events as $event) {
    $dispatch = $event['name'] === 'durable_workflow_worker_task_schedule_to_start_seconds';
    $keys = array_keys($event['tags']);
    sort($keys);
    $expectedKeys = $dispatch ? ['task_kind', 'task_queue'] : ['outcome', 'task_kind', 'task_queue'];
    if (($instruments[$event['name']] ?? null) !== $event['method'] || $keys !== $expectedKeys
        || $event['tags']['task_queue'] !== $queue
        || !in_array($event['tags']['task_kind'], $dispatch ? ['workflow', 'activity'] : ['workflow', 'activity', 'query'], true)
        || !is_finite($event['value']) || $event['value'] < 0
        || ($event['method'] === 'increment' && $event['value'] !== 1.0)) {
        throw new RuntimeException('Unexpected worker metric name, instrument, labels or value: '.json_encode($event));
    }
    ksort($event['tags']);
    $key = json_encode($event['tags'], JSON_THROW_ON_ERROR);
    $series[$event['name']][$key][] = $event['value'];
}
if (array_diff(array_keys($instruments), array_keys($series)) !== []) {
    throw new RuntimeException('The installed worker omitted a required metric instrument.');
}
foreach (['poll', 'task'] as $scope) {
    $counters = $series['durable_workflow_worker_'.$scope.'s'];
    $durations = $series['durable_workflow_worker_'.$scope.'_duration_seconds'];
    if (array_keys($counters) !== array_keys($durations)) {
        throw new RuntimeException('Counter and duration labels differ.');
    }
    foreach ($counters as $tags => $values) {
        if (count($values) !== count($durations[$tags])) {
            throw new RuntimeException('A worker observation omitted its paired duration.');
        }
    }
}
foreach (['workflow', 'activity'] as $kind) {
    $tags = json_encode(['outcome' => 'completed', 'task_kind' => $kind, 'task_queue' => $queue], JSON_THROW_ON_ERROR);
    $tasks = $series['durable_workflow_worker_tasks'][$tags] ?? [];
    $dispatchTags = json_encode(['task_kind' => $kind, 'task_queue' => $queue], JSON_THROW_ON_ERROR);
    $ages = $series['durable_workflow_worker_task_schedule_to_start_seconds'][$dispatchTags] ?? [];
    if ($tasks === [] || count($tasks) !== count($ages)
        || ($kind === 'workflow' && max($ages) <= 0)) {
        throw new RuntimeException('Each dispatched '.$kind.' task must have a ready-age sample; queued workflow age must be positive.');
    }
}
// A completion may carry the activity lease, without a separate activity poll.
$leased = json_encode(['outcome' => 'task', 'task_kind' => 'workflow', 'task_queue' => $queue], JSON_THROW_ON_ERROR);
if (($series['durable_workflow_worker_polls'][$leased] ?? []) === []) {
    throw new RuntimeException('The worker omitted the successful workflow poll.');
}
$empty = json_encode(['outcome' => 'empty', 'task_kind' => 'query', 'task_queue' => $queue], JSON_THROW_ON_ERROR);
if (($series['durable_workflow_worker_polls'][$empty] ?? []) === []) {
    throw new RuntimeException('An empty query poll must be observed without a task or dispatch sample.');
}

echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'server_image' => getenv('SERVER_IMAGE'),
    'runner_commit' => getenv('GITHUB_SHA') ?: null,
    'started_at' => $startedAt, 'completed_at' => gmdate('c'),
    'workflow_completed' => true, 'worker_deregistered' => true,
    'observations' => $recorder->events,
], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES).PHP_EOL;
