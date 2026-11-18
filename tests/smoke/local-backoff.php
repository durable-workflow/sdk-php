<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

use function DurableWorkflow\Tests\Smoke\verifyLocalBackoff;

require $argv[1];
require __DIR__.'/local-backoff-assertions.php';

$version = InstalledVersions::getPrettyVersion('durable-workflow/sdk');
$reference = InstalledVersions::getReference('durable-workflow/sdk');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
$implementation = realpath((string) (new ReflectionClass(Worker::class))->getFileName());
if ($version !== '3.18.2' || getenv('SDK_VERSION') !== $version
    || $reference !== 'f9483bb2b5b45f591a82d9e5419c5b60a6e967c6'
    || $root === false || $implementation === false || !str_starts_with($implementation, $root.'/')
    || getenv('SERVER_VERSION') !== '3.8.0'
    || getenv('SERVER_IMAGE') !== 'durableworkflow/server@sha256:7988a23525d9e50e997a9f6f02e801300ed4f626de3cf6977033657ae7e9f58e') {
    throw new RuntimeException('Require the exact published SDK 3.18.2/native 3.8.0 tuple.');
}
$installed = json_decode(file_get_contents(dirname($argv[1]).'/composer/installed.json'), true, flags: JSON_THROW_ON_ERROR);
$packages = array_values(array_filter($installed['packages'] ?? [], static fn (array $row): bool => $row['name'] === 'durable-workflow/sdk'));
if (count($packages) !== 1 || ($packages[0]['installation-source'] ?? null) !== 'dist'
    || ($packages[0]['source']['reference'] ?? null) !== $reference
    || ($packages[0]['dist']['reference'] ?? null) !== $reference || ($packages[0]['dist']['type'] ?? null) !== 'zip') {
    throw new RuntimeException('Require the exact registry ZIP installation without source fallback.');
}
$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'));
$info = $client->clusterInfo();
if ($info->version !== '3.8.0' || ($info->raw['implementation'] ?? null) !== 'rust') {
    throw new RuntimeException('Require the selected published native producer.');
}
$cases = [];
foreach (['short' => [1, 2], 'empty' => []] as $name => $backoff) {
    $queue = 'published-local-backoff-'.bin2hex(random_bytes(8));
    $callbacks = [];
    $worker = new Worker($client, $queue);
    $worker->registerActivity('backoff.local', static function (ActivityContext $context) use (&$callbacks): string {
        $callbacks[] = [
            'workflow_id' => $context->workflowId, 'run_id' => $context->runId,
            'task_id' => $context->taskId, 'activity_attempt_id' => $context->activityAttemptId,
            'attempt_number' => $context->attemptNumber, 'monotonic_ns' => hrtime(true),
        ];
        if ($context->attemptNumber < 5) {
            throw new RuntimeException('Expected local retry failure.');
        }
        if ($context->attemptNumber !== 5) {
            throw new RuntimeException('Unexpected extra local attempt.');
        }

        return 'local-retry-complete';
    });
    $policy = ['max_attempts' => 5];
    if ($backoff !== []) {
        $policy['backoff_seconds'] = $backoff;
    }
    $worker->registerWorkflow('backoff.workflow', static function (WorkflowContext $context) use ($worker, $policy): string {
        $result = $context->localActivity('backoff.local', [], ['retry_policy' => $policy]);
        $worker->requestShutdown();

        return $result;
    });
    $handle = $client->startWorkflow('backoff.workflow', 'local-backoff-'.bin2hex(random_bytes(8)), $queue,
        executionTimeoutSeconds: 45, runTimeoutSeconds: 45);
    if ($handle->selectedRunId === null) {
        throw new RuntimeException('Require the exact started run.');
    }
    $worker->run(0);
    $history = [];
    foreach ($client->iterateWorkflowHistory($handle->workflowId, $handle->selectedRunId, pageSize: 100) as $event) {
        $history[] = $event;
        if (count($history) > 64) {
            throw new RuntimeException('The single local activity exceeded the bounded history.');
        }
    }
    $expected = $name === 'short' ? [1, 2, 2, 2] : [0, 0, 0, 0];
    $gaps = verifyLocalBackoff($callbacks, $handle->workflowId, $handle->selectedRunId, $history, $expected);
    $scheduled = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === 'ActivityScheduled'));
    if (($scheduled[0]['payload']['activity']['retry_policy'] ?? null) != $policy) {
        throw new RuntimeException('The producer must retain the original explicit-list policy without a generated maximum.');
    }
    if ($handle->resultOfSelectedRun(timeoutSeconds: 5) !== 'local-retry-complete'
        || $handle->describeSelectedRun()->status !== 'completed'
        || ($client->listWorkers($queue)['workers'] ?? null) !== []) {
        throw new RuntimeException('The exact run must finish and its managed Worker must deregister.');
    }
    $cases[$name] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
        'policy' => $policy, 'expected_delays_seconds' => $expected, 'callback_gaps_seconds' => $gaps,
        'callbacks' => $callbacks, 'history' => $history, 'workflow_completed' => true, 'worker_deregistered' => true];
}

// Synthetic recorded legacy history is a separate installed-Replayer check,
// not a database seeded by a previous SDK or an actual older-producer run.
$fixturePath = __DIR__.'/../fixtures/replay-regressions/local-activity-recorded-backoff-cold-replay.json';
$fixtureBytes = file_get_contents($fixturePath);
$fixture = json_decode($fixtureBytes, true, flags: JSON_THROW_ON_ERROR);
$codec = new AvroPayloadCodec();
$calls = 0;
$legacyWorkflow = static function (WorkflowContext $context): string {
    try {
        $context->localActivity('golden.local-unavailable', [],
            ['retry_policy' => ['max_attempts' => 4, 'backoff_seconds' => [1]]]);
    } catch (ActivityFailed) {
        return 'retries-exhausted';
    }

    throw new RuntimeException('The recorded legacy failure must remain authoritative.');
};
$commands = (new Replayer($codec))->replay($legacyWorkflow, $fixture['history'], [], 'legacy-replay',
    localActivityExecutor: static function () use (&$calls): never { ++$calls; throw new RuntimeException('Recorded local activity must not execute again.'); })->commands;
if ($calls !== 0 || count($commands) !== 1 || $commands[0]['type'] !== 'complete_workflow'
    || $codec->decodeEnvelope($commands[0]['result']) !== 'retries-exhausted'
    || array_column($fixture['history'][1]['payload']['attempts'], 'backoff_seconds') !== [1, 0, 0]
    || file_get_contents($fixturePath) !== $fixtureBytes) {
    throw new RuntimeException('Legacy recorded delays must replay without local calls or rewritten history.');
}
echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'server_image' => getenv('SERVER_IMAGE'),
    'runner_commit' => getenv('GITHUB_SHA') ?: null, 'cases' => $cases,
    'legacy_scope' => 'synthetic_recorded_history_installed_replayer',
    'legacy_fixture_sha256' => hash('sha256', $fixtureBytes), 'legacy_recorded_delays' => [1, 0, 0],
    'legacy_replay_callback_count' => $calls, 'legacy_replay_passed' => true, 'source_fallback' => false,
], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES).PHP_EOL;
