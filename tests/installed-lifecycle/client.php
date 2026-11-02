<?php

declare(strict_types=1);

// Drives each scenario through the normally installed SDK client and prints one
// JSON line per scenario: {"scenario", "outcome": pass|product-fail|runner-blocked, ...}.

require '/consumer/vendor/autoload.php';

use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowFailed;

$client = new Client((string) getenv('DW_URL'), token: (string) getenv('DW_TOKEN'));
$queue = (string) getenv('DW_TASK_QUEUE');
$suffix = bin2hex(random_bytes(6));

function report(string $scenario, string $outcome, array $details): void
{
    echo json_encode(['scenario' => $scenario, 'outcome' => $outcome] + $details,
        JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)."\n";
}

function eventually(callable $probe, float $seconds = 20.0): mixed
{
    $until = microtime(true) + $seconds;
    do {
        $value = $probe();
        if ($value !== null) {
            return $value;
        }
        usleep(200_000);
    } while (microtime(true) < $until);

    return null;
}

/** Run a scenario; any unexpected exception becomes product-fail with its type and message. */
function scenario(string $name, callable $body): void
{
    try {
        [$outcome, $details] = $body();
        report($name, $outcome, $details);
    } catch (Throwable $error) {
        report($name, 'product-fail', ['error_type' => $error::class, 'error' => substr($error->getMessage(), 0, 300)]);
    }
}

$cluster = $client->clusterInfo();
report('cluster', 'info', ['server_version' => $cluster->version,
    'control_plane' => $cluster->controlPlane['version'] ?? null,
    'worker_protocol' => $cluster->raw['worker_protocol']['version'] ?? null,
    'sdk_registration' => DurableWorkflow\SdkIdentity::registration()]);

scenario('lifecycle', static function () use ($client, $queue, $suffix): array {
    $id = 'installed-lifecycle-'.$suffix;
    $started = $client->startWorkflow('installed.lifecycle', $id, $queue, ['ada']);
    $status = eventually(static function () use ($client, $id): ?array {
        $value = $client->queryWorkflow($id, 'status');

        return is_array($value) && ($value['activity_completed'] ?? false) === true ? $value : null;
    });
    if ($status === null) {
        return ['product-fail', ['step' => 'query', 'workflow_id' => $id]];
    }
    $update = $client->updateWorkflow($id, 'rename', ['grace']);
    $client->signalWorkflow($id, 'approve', ['ops']);
    $result = $client->workflowResult($id, timeoutSeconds: 30.0);
    $expected = ['greeting' => 'hello, ada', 'approved_by' => 'ops', 'renames' => ['grace']];
    $updateResult = $update;
    $ok = $result === $expected && $updateResult === 'renamed:grace';

    return [$ok ? 'pass' : 'product-fail', ['workflow_id' => $id, 'run_id' => $started->selectedRunId,
        'query' => $status, 'update_result' => $updateResult, 'result' => $result]];
});

scenario('cancellation', static function () use ($client, $queue, $suffix): array {
    $id = 'installed-cancel-'.$suffix;
    $started = $client->startWorkflow('installed.cancellable', $id, $queue);
    usleep(1_500_000);
    $client->cancelWorkflow($id, 'installed lifecycle cancellation');
    try {
        $result = $client->workflowResult($id, timeoutSeconds: 30.0);

        return ['product-fail', ['workflow_id' => $id, 'unexpected_result' => $result]];
    } catch (WorkflowCancelled $cancelled) {
        $status = $client->describeWorkflow($id)->status ?? null;

        return ['pass', ['workflow_id' => $id, 'run_id' => $started->selectedRunId,
            'error_type' => $cancelled::class, 'status' => $status]];
    }
});

scenario('typed_failure', static function () use ($client, $queue, $suffix): array {
    $id = 'installed-failure-'.$suffix;
    $started = $client->startWorkflow('installed.failing', $id, $queue, ['bad order']);
    try {
        $result = $client->workflowResult($id, timeoutSeconds: 30.0);

        return ['product-fail', ['workflow_id' => $id, 'unexpected_result' => $result]];
    } catch (WorkflowFailed $failed) {
        $text = $failed->getMessage().' '.json_encode($failed->failure);
        $ok = str_contains($text, 'rejected: bad order');

        return [$ok ? 'pass' : 'product-fail', ['workflow_id' => $id, 'run_id' => $started->selectedRunId,
            'error_type' => $failed::class, 'failure_type' => $failed->failureType,
            'message' => substr($failed->getMessage(), 0, 200), 'carries_activity_message' => $ok]];
    }
});
