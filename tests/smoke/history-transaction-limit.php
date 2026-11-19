<?php

declare(strict_types=1);

// durable-workflow/.github#228: run the actual Worker against a published native
// Server whose history-transaction budget (DW_V2_LIMIT_HISTORY_TRANSACTION_BYTES)
// is lowered, so a fan-out that fits the completion size limit is refused with
// 409 history_transaction_limit_exceeded. The run must fail once with a typed
// structural-limit failure, record no partial activities, and the same Worker
// must then complete a small fan-out.
//
// Usage: php tests/smoke/history-transaction-limit.php <vendor/autoload.php>
// Environment: HISTORY_LIMIT_RUNTIME_URL, HISTORY_LIMIT_TOKEN,
// HISTORY_LIMIT_SERVER_VERSION, HISTORY_LIMIT_BYTES, and optionally SDK_VERSION
// to require that exact installed package.

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

require $argv[1];

function requireHistoryLimit(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$package = 'durable-workflow/sdk';
requireHistoryLimit(InstalledVersions::isInstalled($package), 'The SDK package is not installed.');
$version = InstalledVersions::getPrettyVersion($package);
$reference = InstalledVersions::getReference($package);
$root = realpath((string) InstalledVersions::getInstallPath($package));
requireHistoryLimit($root !== false, 'The installed SDK path is missing.');
foreach ([Client::class, Worker::class] as $class) {
    $file = realpath((string) (new ReflectionClass($class))->getFileName());
    requireHistoryLimit($file !== false && str_starts_with($file, $root.'/'), 'Source fallback is forbidden.');
}
$expectedVersion = getenv('SDK_VERSION');
requireHistoryLimit(!is_string($expectedVersion) || $expectedVersion === '' || $version === $expectedVersion,
    'Unexpected installed SDK version.');
requireHistoryLimit(function_exists('pcntl_fork') && function_exists('posix_kill'), 'The fixture requires process control.');

$url = (string) getenv('HISTORY_LIMIT_RUNTIME_URL');
$token = (string) getenv('HISTORY_LIMIT_TOKEN');
$budget = (int) getenv('HISTORY_LIMIT_BYTES');
requireHistoryLimit($budget > 0, 'HISTORY_LIMIT_BYTES must name the lowered history budget.');
$client = new Client($url, token: $token);
$info = $client->clusterInfo();
requireHistoryLimit($info->version === getenv('HISTORY_LIMIT_SERVER_VERSION') && ($info->raw['implementation'] ?? null) === 'rust',
    'Unexpected published native Server.');
$completionCap = $info->limits['max_workflow_task_completion_bytes'] ?? null;
requireHistoryLimit(is_int($completionCap) && $completionCap > 4 * $budget, 'The fan-out must fit the completion size limit.');

$queue = 'history-limit-'.bin2hex(random_bytes(6));
$evidenceFile = tempnam(sys_get_temp_dir(), 'sdk-php-history-limit-');
requireHistoryLimit(is_string($evidenceFile), 'Cannot create owned worker evidence.');
$pid = pcntl_fork();
requireHistoryLimit($pid !== -1, 'Cannot start the worker.');
if ($pid === 0) {
    $diagnostics = [];
    $activityCalls = [];
    $exit = 0;
    $errorClass = null;
    try {
        $worker = new Worker(new Client($url, token: $token), $queue,
            diagnosticListener: static function (string $event, array $details) use (&$diagnostics, $evidenceFile, &$activityCalls): void {
                if ($event === 'worker.workflow_task_history_limit_exceeded') {
                    $diagnostics[] = array_intersect_key($details, array_flip(['reason', 'limit_kind', 'current_value', 'configured_limit']));
                    file_put_contents($evidenceFile, json_encode(['diagnostics' => $diagnostics, 'activity_calls' => $activityCalls]));
                }
            });
        $worker->registerActivity('history.length', static function (ActivityContext $context, string $value) use (&$activityCalls, &$diagnostics, $evidenceFile): int {
            $bytes = strlen($value);
            $activityCalls[$bytes] = ($activityCalls[$bytes] ?? 0) + 1;
            file_put_contents($evidenceFile, json_encode(['diagnostics' => $diagnostics, 'activity_calls' => $activityCalls]));

            return $bytes;
        });
        $worker->registerWorkflow('history.fanout', static function (WorkflowContext $context, int $count, int $bytes): array {
            $operations = [];
            $argument = str_repeat('x', $bytes);
            for ($index = 0; $index < $count; ++$index) {
                $operations[] = $context->deferActivity('history.length', [$argument]);
            }
            $lengths = $context->all($operations);

            return [count($lengths), array_sum($lengths)];
        });
        // run() registers the Worker and stops cleanly on the parent's SIGTERM.
        $worker->run(0);
    } catch (Throwable $error) {
        $exit = 1;
        $errorClass = $error::class.': '.$error->getMessage();
    } finally {
        file_put_contents($evidenceFile, json_encode(['diagnostics' => $diagnostics, 'activity_calls' => $activityCalls,
            'worker_error' => $errorClass], JSON_THROW_ON_ERROR));
    }
    exit($exit);
}

$parentError = null;
$summary = [];
try {
    $start = static fn (int $count, int $bytes) => $client->startWorkflow('history.fanout',
        'history-limit-'.bin2hex(random_bytes(8)), $queue, [$count, $bytes], executionTimeoutSeconds: 120, runTimeoutSeconds: 120);
    // 40 activities with 4 KB arguments: about 160 KB of ActivityScheduled
    // history in one completion, far under the 32 MiB completion cap but over
    // the lowered history budget.
    $over = $start(40, 4096);
    $failure = null;
    try {
        $over->result(timeoutSeconds: 60);
        throw new RuntimeException('The over-budget fan-out unexpectedly completed.');
    } catch (WorkflowFailed $failed) {
        $failure = $failed;
    }
    $events = iterator_to_array($client->iterateWorkflowHistory($over->workflowId, $over->selectedRunId));
    $counts = array_count_values(array_column($events, 'event_type'));
    $failures = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'WorkflowFailed'));
    $payload = $failures[0]['payload'] ?? [];
    $message = (string) ($payload['message'] ?? $payload['exception']['message'] ?? '');
    requireHistoryLimit(($counts['ActivityScheduled'] ?? 0) === 0 && ($counts['WorkflowTaskFailed'] ?? 0) === 0
        && count($failures) === 1 && ($payload['failure_category'] ?? null) === 'structural_limit'
        && ($payload['non_retryable'] ?? null) === true,
        'The over-budget fan-out admitted partial work or did not fail structurally: '.json_encode($counts).' '.json_encode($payload));
    requireHistoryLimit(str_starts_with($message, 'Structural limit exceeded: history_transaction_bytes: ')
        && str_contains($message, "over the server limit of {$budget} history bytes"),
        'The structural failure does not name the history budget: '.$message);

    // The same Worker then completes a small fan-out within the budget.
    $followup = $start(3, 10);
    requireHistoryLimit($followup->result(timeoutSeconds: 30) === [3, 30], 'The same Worker did not complete the follow-up.');
    $summary = ['over_history_counts' => $counts, 'structural_failure' => ['failure_category' => $payload['failure_category'],
        'non_retryable' => $payload['non_retryable'], 'message' => $message, 'exception_type' => $payload['exception_type'] ?? null],
        'workflow_failed_class' => $failure::class];
} catch (Throwable $error) {
    $parentError = $error;
} finally {
    posix_kill($pid, SIGTERM);
    pcntl_waitpid($pid, $workerStatus);
}

try {
    $evidence = json_decode((string) file_get_contents($evidenceFile), true, flags: JSON_THROW_ON_ERROR);
    if ($parentError !== null) {
        fwrite(STDERR, json_encode(['parent_error' => $parentError->getMessage(), 'worker' => $evidence], JSON_THROW_ON_ERROR).PHP_EOL);
        throw $parentError;
    }
    requireHistoryLimit(pcntl_wifexited($workerStatus) && pcntl_wexitstatus($workerStatus) === 0
        && ($evidence['worker_error'] ?? null) === null, 'The Worker did not shut down cleanly.');
    $diagnostic = $evidence['diagnostics'][0] ?? [];
    requireHistoryLimit(count($evidence['diagnostics']) === 1 && $diagnostic['reason'] === 'history_transaction_limit_exceeded'
        && $diagnostic['limit_kind'] === 'history_transaction_bytes' && $diagnostic['configured_limit'] === $budget
        && is_int($diagnostic['current_value']) && $diagnostic['current_value'] > $budget,
        'The Worker did not observe exactly one typed 409: '.json_encode($evidence['diagnostics']));
    requireHistoryLimit(($evidence['activity_calls'][4096] ?? 0) === 0 && ($evidence['activity_calls'][10] ?? 0) === 3,
        'Activity evidence differs: '.json_encode($evidence['activity_calls']));
    echo json_encode(['outcome' => 'pass', 'sdk_version' => $version, 'sdk_reference' => $reference, 'sdk_install_path' => $root,
        'server_version' => $info->version, 'php_version' => PHP_VERSION, 'history_transaction_bytes' => $budget,
        'completion_limit_bytes' => $completionCap, 'refusal' => $diagnostic] + $summary
        + ['activity_calls' => $evidence['activity_calls'], 'same_worker_followup' => true],
        JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR).PHP_EOL;
} finally {
    unlink($evidenceFile);
}
