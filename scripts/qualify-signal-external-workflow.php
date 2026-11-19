<?php

declare(strict_types=1);

// End-to-end check of WorkflowContext::signalExternalWorkflow() against a
// runtime that advertises signal_external_workflow (Server #1134,
// durable-workflow/.github#223): a delivered signal, a missing target, a
// closed target, and a cold replay by a fresh worker process that sends
// nothing again.
// Usage: php qualify-signal-external-workflow.php AUTOLOAD SERVER_URL
// The Server must accept the operator/worker/admin tokens below (a disposable
// token-auth runtime, as in docker-compose.claimed-task-recovery.yml). Workers
// run in child processes because Worker::run() owns registration.

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;

/** @return array<string, mixed> */
function senderResult(WorkflowContext $context, string $target, string $missing, string $closed): array
{
    $results = [];
    foreach (['delivered' => $target, 'missing' => $missing, 'closed' => $closed] as $case => $workflowId) {
        $result = $context->signalExternalWorkflow($workflowId, 'approve', ['Ada', 3]);
        $results[$case] = [$result->outcome->value, $result->reason, $result->targetRunId, $result->targetCommandId];
    }
    // A durable wait after the signals, so a replacement worker replays them.
    $context->sleep(3);

    return $results;
}

$sender = static fn (WorkflowContext $context, string $target, string $missing, string $closed): array
    => senderResult($context, $target, $missing, $closed);

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue, $workerId] = array_slice($argv, 4);
    $worker = new Worker(new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'), $queue, workerId: $workerId);
    $worker->registerWorkflow('qualification.signal-external.sender', $sender);
    $worker->registerWorkflow('qualification.signal-external.target', static function (WorkflowContext $context): array {
        $context->waitCondition(static fn (): bool => $context->signals('approve') !== [], 'approved');

        return $context->signals('approve');
    });
    $worker->declareSignal('qualification.signal-external.target', 'approve', static fn (string $name, int $count): mixed => null);
    $worker->registerWorkflow('qualification.signal-external.done', static fn (WorkflowContext $context): string => 'done');
    $worker->declareSignal('qualification.signal-external.done', 'approve', static fn (string $name, int $count): mixed => null);
    try {
        $worker->run(1);
    } catch (Throwable $error) {
        fwrite(STDERR, 'worker stopped: '.$error::class.': '.$error->getMessage()."\n");
        exit(1);
    }
    exit(0);
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "FAIL: {$message}\n");
        exit(1);
    }
    echo "ok: {$message}\n";
}

/** @return list<array<string, mixed>> */
function events(Client $client, string $workflowId, string $runId): array
{
    $history = $client->workflowHistory($workflowId, $runId, 500);

    return $history['events'] ?? $history['history_events'] ?? [];
}

/** @return list<array<string, mixed>> */
function ofType(array $events, string $type): array
{
    return array_values(array_filter($events, static fn (array $event): bool => ($event['event_type'] ?? null) === $type));
}

function waitFor(callable $condition, string $what, ?callable $diagnose = null, int $seconds = 90): void
{
    $deadline = microtime(true) + $seconds;
    while (microtime(true) < $deadline) {
        if ($condition()) {
            return;
        }
        usleep(250_000);
    }
    if ($diagnose !== null) {
        fwrite(STDERR, json_encode($diagnose(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_PARTIAL_OUTPUT_ON_ERROR)."\n");
    }
    check(false, "timed out waiting for {$what}");
}

$admin = new Client($url, controlToken: 'claimed-task-admin');
check(($admin->clusterInfo()->capabilities['signal_external_workflow'] ?? null) === true,
    'cluster info advertises signal_external_workflow');
$suffix = bin2hex(random_bytes(6));
$namespace = 'php-signal-external-'.$suffix;
$queue = 'signal-external-'.$suffix;
$admin->createNamespace($namespace);
$client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$startWorker = static fn (string $workerId) => proc_open(
    [PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue, $workerId], [1 => STDOUT, 2 => STDERR], $pipes,
);
$finished = static fn (string $workflowId): bool
    => !in_array($client->describeWorkflow($workflowId)->status, ['running', 'waiting', 'pending'], true);

$process = $startWorker('signal-external-a-'.$suffix);
try {
    $done = $client->startWorkflow('qualification.signal-external.done', 'done-'.$suffix, $queue);
    waitFor(static fn (): bool => $finished($done->workflowId), 'the closed target to complete');
    check($client->describeWorkflow($done->workflowId)->status === 'completed', 'the closed target completed');
    $target = $client->startWorkflow('qualification.signal-external.target', 'target-'.$suffix, $queue);
    $missing = 'missing-'.$suffix;
    $senderRun = $client->startWorkflow('qualification.signal-external.sender', 'sender-'.$suffix, $queue,
        [$target->workflowId, $missing, $done->workflowId]);
    $senderRunId = (string) $senderRun->selectedRunId;

    // All three outcomes are recorded and the sender waits on its timer.
    waitFor(static fn (): bool => ofType(events($client, $senderRun->workflowId, $senderRunId), 'TimerScheduled') !== [],
        'the sender to record its signals and start its timer',
        static fn (): array => events($client, $senderRun->workflowId, $senderRunId));
    proc_terminate($process);
    proc_close($process);
    echo "ok: stopped the first worker while the sender waits\n";

    // A fresh process has no cached workflow state: it cold-replays the signals.
    $process = $startWorker('signal-external-b-'.$suffix);
    waitFor(static fn (): bool => $finished($senderRun->workflowId), 'the sender to complete');
    waitFor(static fn (): bool => $finished($target->workflowId), 'the target to complete');
    check($client->describeWorkflow($senderRun->workflowId)->status === 'completed', 'the sender completed after a cold replay');
    check($client->describeWorkflow($target->workflowId)->status === 'completed', 'the delivered target completed');

    $result = $senderRun->result(5);
    check(($result['delivered'][0] ?? null) === 'delivered' && array_key_exists(1, $result['delivered']) && $result['delivered'][1] === null
        && is_string($result['delivered'][2] ?? null) && is_string($result['delivered'][3] ?? null),
        'success: delivered with the target run and command ('.json_encode($result['delivered'] ?? null).')');
    check(($result['missing'][0] ?? null) === 'target_not_found' && ($result['missing'][1] ?? null) === 'instance_not_found',
        'missing target: target_not_found / instance_not_found ('.json_encode($result['missing'] ?? null).')');
    check(($result['closed'][0] ?? null) === 'target_closed' && ($result['closed'][1] ?? null) === 'run_not_active',
        'closed target: target_closed / run_not_active ('.json_encode($result['closed'] ?? null).')');
    check($target->result(5) === [['Ada', 3]], 'the target received the signal arguments once');

    $events = events($client, $senderRun->workflowId, $senderRunId);
    $requested = ofType($events, 'ExternalSignalRequested');
    check(count($requested) === 3, 'the sender recorded exactly three signal requests');
    check(count(ofType($events, 'ExternalSignalDelivered')) === 1 && count(ofType($events, 'ExternalSignalFailed')) === 2,
        'the sender recorded one delivered and two failed outcomes');
    check(array_column(array_column($requested, 'payload'), 'sequence') === [1, 2, 3], 'the requests took sequences 1, 2 and 3');
    check((ofType($events, 'TimerScheduled')[0]['payload']['sequence'] ?? null) === 4,
        'the durable timer after the signals took sequence 4');
    $targetEvents = events($client, $target->workflowId, (string) $target->selectedRunId);
    check(count(ofType($targetEvents, 'SignalReceived')) === 1, 'the target history has exactly one SignalReceived');
    $doneEvents = events($client, $done->workflowId, (string) $done->selectedRunId);
    check(ofType($doneEvents, 'SignalReceived') === [], 'the closed target received no signal');

    // Cold replay of the committed history through the official replayer.
    $codec = new AvroPayloadCodec();
    $replay = (new Replayer($codec))->replay($sender, $events, [$target->workflowId, $missing, $done->workflowId], $queue,
        ['workflow_id' => $senderRun->workflowId, 'run_id' => $senderRunId, 'workflow_type' => 'qualification.signal-external.sender']);
    check(array_column($replay->commands, 'type') === ['complete_workflow'], 'a cold replay of the committed history sends no signal again');
    check($codec->decodeEnvelope($replay->commands[0]['result']) === $result, 'the cold replay returns the recorded outcomes');

    echo json_encode(['namespace' => $namespace, 'result' => $result, 'sender_history' => array_column($events, 'event_type'),
        'target_history' => array_column($targetEvents, 'event_type')], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)."\n";
} finally {
    proc_terminate($process);
    proc_close($process);
}
