<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\SignalFailed;
use DurableWorkflow\Exception\UpdateFailed;
use DurableWorkflow\Exception\UpdateStillPending;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

// One bounded fixture operation per invocation. The owning runner verifies
// the registry archive and installed bytes before loading this read-only tool.
$input = stream_get_contents(STDIN, 65537);
if (!is_string($input) || strlen($input) > 65536) {
    throw new RuntimeException('The command-key fixture packet exceeds its limit.');
}
$packet = json_decode($input, true, flags: JSON_THROW_ON_ERROR);
if (!is_array($packet) || array_is_list($packet)) {
    throw new RuntimeException('The command-key fixture needs an object packet.');
}
$operation = $packet['operation'] ?? '';
if (!in_array($operation, ['bootstrap', 'drive', 'signal', 'update', 'read', 'value', 'history', 'describe'], true)) {
    throw new RuntimeException('Unknown command-key fixture operation.');
}
$workflow = $packet['workflow_id'] ?? '';
if (!is_string($workflow) || preg_match('/\Asdk-php-216-[a-zA-Z0-9_.-]{1,100}\z/', $workflow) !== 1) {
    throw new RuntimeException('The fixture workflow identity is outside its owned prefix.');
}
$namespace = $packet['namespace'] ?? 'default';
if (!is_string($namespace) || ($namespace !== 'default'
    && preg_match('/\Asdk-php-216-[a-zA-Z0-9_.-]{1,100}\z/', $namespace) !== 1)) {
    throw new RuntimeException('The fixture namespace is outside its owned prefix.');
}
$supply = json_decode((string) file_get_contents(__DIR__.'/command-key-supply.json'), true, flags: JSON_THROW_ON_ERROR);
$url = getenv('COMMAND_KEY_RUNTIME_URL');
if (!is_string($url) || preg_match('~\Ahttp://127\.0\.0\.1:([1-9][0-9]{0,4})\z~', $url, $port) !== 1
    || (int) $port[1] > 65535) {
    throw new RuntimeException('The fixture requires a numeric loopback runtime and explicit port.');
}

require $argv[1];
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
if ($root === false || InstalledVersions::getPrettyVersion('durable-workflow/sdk') !== $supply['sdk_version']
    || InstalledVersions::getReference('durable-workflow/sdk') !== $supply['sdk_reference']) {
    throw new RuntimeException('The installed SDK differs from the selected published supply.');
}
foreach ([Client::class, Worker::class, SignalFailed::class, UpdateFailed::class] as $class) {
    $file = realpath((string) (new ReflectionClass($class))->getFileName());
    if ($file === false || !str_starts_with($file, $root.'/')) {
        throw new RuntimeException('Source fallback is forbidden.');
    }
}

$client = new Client($url, namespace: $namespace,
    token: getenv('COMMAND_KEY_TOKEN'), requestTimeoutSeconds: 8, workerRequestTimeoutSeconds: 8);
$info = $client->clusterInfo();
if ($info->version !== $supply['server_version'] || ($info->raw['implementation'] ?? null) !== 'rust'
    || !$info->supportsCommandRequestKeys()) {
    throw new RuntimeException('The runtime lacks the selected native command-key contract.');
}
$identity = ['sdk_version' => $supply['sdk_version'], 'sdk_reference' => $supply['sdk_reference'],
    'server_version' => $info->version, 'php_version' => PHP_VERSION, 'source_fallback' => false];
$run = $packet['run_id'] ?? null;
$arguments = $packet['arguments'] ?? [];
if (isset($packet['arguments_envelope'])) {
    if (array_key_exists('arguments', $packet)) {
        throw new RuntimeException('Use one argument representation.');
    }
    $arguments = $client->payloadCodec()->decodeEnvelope($packet['arguments_envelope']);
}
if (!is_array($arguments) || !array_is_list($arguments)) {
    throw new RuntimeException('Command arguments must be a list.');
}

$receiptFields = ['workflow_id', 'run_id', 'command_id', 'signal_id', 'update_id', 'request_id', 'command_sequence', 'command_status',
    'update_status', 'outcome', 'retained', 'intent_verified', 'accepted_at'];
try {
    if ($operation === 'bootstrap' || $operation === 'drive') {
        $queue = $packet['task_queue'] ?? '';
        if (!is_string($queue) || preg_match('/\Asdk-php-216-[a-zA-Z0-9_.-]{1,100}\z/', $queue) !== 1) {
            throw new RuntimeException('The fixture queue is outside its owned prefix.');
        }
        $worker = new Worker($client, $queue);
        $worker->registerWorkflow('command.keys', static function (WorkflowContext $context, string $mode) use ($worker): mixed {
            $worker->requestShutdown();
            $context->waitCondition(static fn (): bool => $context->signals('release') !== []);
            if ($mode === 'continue') {
                $context->continueAsNew(['open']);
            }
            return null;
        });
        foreach (['signal', 'other'] as $name) {
            $worker->declareSignal('command.keys', $name, static fn (mixed $value): mixed => null);
            $worker->registerUpdate('command.keys', $name, static function (QueryContext $context, mixed $value) use ($worker): mixed {
                $worker->requestShutdown();
                return null;
            });
        }
        $worker->declareSignal('command.keys', 'release');
        if ($operation === 'bootstrap') {
            $mode = $packet['mode'] ?? 'open';
            if (!in_array($mode, ['open', 'close', 'continue'], true)) {
                throw new RuntimeException('Unknown workflow fixture mode.');
            }
            $client->startWorkflow('command.keys', $workflow, $queue, [$mode],
                executionTimeoutSeconds: 300, runTimeoutSeconds: 120);
        }
        // The runner supplies a finite process timeout even if no task exists.
        $worker->run(0);
        $result = ['worker_stopped' => true];
    } elseif ($operation === 'signal') {
        $receipt = $client->signalWorkflow($workflow, $packet['name'], $arguments, $run, $packet['request_id'] ?? null);
        $result = array_intersect_key($receipt, array_flip($receiptFields));
    } elseif ($operation === 'update') {
        $receipt = $client->updateWorkflowReceipt($workflow, $packet['name'], $arguments,
            $packet['wait_for'] ?? 'accepted', 1, $packet['request_id'] ?? null, $run);
        $result = array_intersect_key($receipt, array_flip($receiptFields));
    } elseif ($operation === 'read') {
        $receipt = $client->updateOutcomeReceipt($workflow, $packet['update_id'],
            waitFor: $packet['wait_for'] ?? 'accepted', waitTimeoutSeconds: 1);
        $result = array_intersect_key($receipt, array_flip($receiptFields));
        if (($receipt['update_status'] ?? null) === 'completed') {
            $result['completed_null'] = $client->updateOutcome($workflow, $packet['update_id'], waitTimeoutSeconds: 1) === null;
        }
    } elseif ($operation === 'value') {
        $result = ['completed_null' => $client->updateOutcome($workflow, $packet['update_id'], waitTimeoutSeconds: 1) === null];
    } elseif ($operation === 'describe') {
        $description = $client->describeWorkflow($workflow, $run);
        $result = ['workflow_id' => $description->workflowId, 'run_id' => $description->runId,
            'status' => $description->status];
    } elseif ($operation === 'history') {
        $counts = [];
        $admissions = [];
        foreach ($client->iterateWorkflowHistory($workflow, $run) as $event) {
            $type = $event['event_type'];
            $counts[$type] = ($counts[$type] ?? 0) + 1;
            if (in_array($type, ['SignalReceived', 'UpdateAccepted'], true)) {
                $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
                $admissions[] = ['event_type' => $type,
                    'command_id' => $payload['workflow_command_id'] ?? $event['workflow_command_id'] ?? null,
                    'signal_id' => $payload['signal_id'] ?? null, 'update_id' => $payload['update_id'] ?? null,
                    'request_id' => $payload['request_id'] ?? null];
            }
        }
        $result = ['event_counts' => $counts, 'admissions' => $admissions];
    } else {
        throw new RuntimeException('Unknown command-key fixture operation.');
    }
    echo json_encode($identity + ['outcome' => 'answered', 'result' => $result], JSON_THROW_ON_ERROR).PHP_EOL;
} catch (SignalFailed|UpdateFailed $error) {
    // A lost reply may follow a committed admission; do not label it refused.
    echo json_encode($identity + ['outcome' => 'error', 'exception' => $error::class, 'status' => $error->status,
        'reason' => $error->reason(), 'request_id' => $error->requestId(),
        'is_request_id_conflict' => $error->isRequestIdConflict(),
        'conflict' => $error->conflict(), 'retained_command_id' => $error->retainedCommandId(),
        'retained' => $error->details()['retained'] ?? null,
        'intent_verified' => $error->details()['intent_verified'] ?? null,
        'transient_connection_failure' => $error->isTransientConnectionFailure(),
        'transient_upstream_failure' => $error->isTransientUpstreamFailure()], JSON_THROW_ON_ERROR).PHP_EOL;
} catch (UpdateStillPending $error) {
    echo json_encode($identity + ['outcome' => 'pending', 'update_id' => $error->updateId,
        'run_id' => $error->runId, 'request_id' => $error->requestId], JSON_THROW_ON_ERROR).PHP_EOL;
} catch (InvalidArgumentException $error) {
    echo json_encode($identity + ['outcome' => 'local_invalid_argument', 'exception' => $error::class],
        JSON_THROW_ON_ERROR).PHP_EOL;
}
