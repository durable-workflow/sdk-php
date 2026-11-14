<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

use function DurableWorkflow\Tests\Smoke\verifyActivityRetryIdentity;

require $argv[1];
require __DIR__.'/activity-retry-identity-assertions.php';

$startedAt = gmdate('c');
$version = InstalledVersions::getPrettyVersion('durable-workflow/sdk');
$reference = InstalledVersions::getReference('durable-workflow/sdk');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
$implementation = realpath((string) (new ReflectionClass(Worker::class))->getFileName());
if ($version !== '3.18.2' || getenv('SDK_VERSION') !== $version
    || $reference !== 'f9483bb2b5b45f591a82d9e5419c5b60a6e967c6'
    || $root === false || $implementation === false || !str_starts_with($implementation, $root.'/')
    || getenv('SERVER_VERSION') !== '3.8.0'
    || getenv('SERVER_IMAGE') !== 'durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e') {
    throw new RuntimeException('Require the reviewed registry SDK 3.18.2/native 3.8.0 tuple without source fallback.');
}
$lock = json_decode(file_get_contents(dirname(dirname($argv[1])).'/composer.lock'), true, flags: JSON_THROW_ON_ERROR);
$packages = array_values(array_filter($lock['packages'] ?? [], static fn (array $row): bool => $row['name'] === 'durable-workflow/sdk'));
$installed = json_decode(file_get_contents(dirname($argv[1]).'/composer/installed.json'), true, flags: JSON_THROW_ON_ERROR);
$distributions = array_values(array_filter($installed['packages'] ?? [], static fn (array $row): bool => $row['name'] === 'durable-workflow/sdk'));
if (count($packages) !== 1 || ($packages[0]['version'] ?? null) !== $version
    || ($packages[0]['source']['reference'] ?? null) !== $reference
    || ($packages[0]['dist']['reference'] ?? null) !== $reference
    || ($packages[0]['dist']['type'] ?? null) !== 'zip'
    || count($distributions) !== 1 || ($distributions[0]['installation-source'] ?? null) !== 'dist'
    || ($distributions[0]['source']['reference'] ?? null) !== $reference
    || ($distributions[0]['dist']['reference'] ?? null) !== $reference) {
    throw new RuntimeException('The ordinary registry lock must bind the exact installed SDK distribution.');
}

$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'));
$info = $client->clusterInfo();
if ($info->version !== '3.8.0' || ($info->raw['implementation'] ?? null) !== 'rust') {
    throw new RuntimeException('Require the selected published native producer.');
}
$queue = 'published-activity-identity-'.bin2hex(random_bytes(8));
$callbacks = [];
$worker = new Worker($client, $queue);
$worker->registerActivity('identity.retry', static function (ActivityContext $context) use (&$callbacks): string {
    $callbacks[] = [
        'activity_execution_id' => $context->activityExecutionId,
        'workflow_id' => $context->workflowId, 'run_id' => $context->runId,
        'task_id' => $context->taskId, 'activity_attempt_id' => $context->activityAttemptId,
        'attempt_number' => $context->attemptNumber,
    ];
    if ($context->attemptNumber === 1) {
        throw new RuntimeException('Expected first-attempt failure for the published retry identity proof.');
    }
    if ($context->attemptNumber !== 2 || $context->activityExecutionId === null) {
        throw new RuntimeException('Require exactly the second attempt with a received execution identity.');
    }

    return $context->activityExecutionId;
});
$worker->registerWorkflow('identity.workflow', static function (WorkflowContext $context) use ($worker): string {
    $result = $context->activity('identity.retry', [], ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]]]);
    $worker->requestShutdown();

    return $result;
});
$handle = $client->startWorkflow('identity.workflow', 'identity-'.bin2hex(random_bytes(8)), $queue,
    executionTimeoutSeconds: 45, runTimeoutSeconds: 45);
if ($handle->selectedRunId === null) {
    throw new RuntimeException('The producer must return the exact started run.');
}
$worker->run(0);
$result = $handle->resultOfSelectedRun(timeoutSeconds: 5);
$history = [];
foreach ($client->iterateWorkflowHistory($handle->workflowId, $handle->selectedRunId, pageSize: 100) as $event) {
    $history[] = $event;
    if (count($history) > 64) {
        throw new RuntimeException('The single-activity retry history exceeded its bounded evidence size.');
    }
}
verifyActivityRetryIdentity($callbacks, $handle->workflowId, $handle->selectedRunId, $history);
if ($result !== $callbacks[1]['activity_execution_id']
    || $handle->describeSelectedRun()->status !== 'completed'
    || ($client->listWorkers($queue)['workers'] ?? null) !== []) {
    throw new RuntimeException('The exact run must complete with the received execution ID and deregister its Worker.');
}
echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'server_image' => getenv('SERVER_IMAGE'),
    'runner_commit' => getenv('GITHUB_SHA') ?: null,
    'started_at' => $startedAt, 'completed_at' => gmdate('c'), 'scope' => 'ordinary_remote_retry',
    'workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
    'callbacks' => $callbacks, 'history' => $history,
    'workflow_completed' => true, 'worker_deregistered' => true, 'source_fallback' => false,
], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES).PHP_EOL;
