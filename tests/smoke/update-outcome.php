<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\UpdateStillPending;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use Psr\Http\Message\RequestInterface;

require_once $argv[1];

final class InstalledUpdateAcceptanceFailed extends RuntimeException {}

function requireUpdateAcceptance(bool $condition, string $message): void
{
    if (!$condition) {
        throw new InstalledUpdateAcceptanceFailed($message);
    }
}

$package = 'durable-workflow/sdk';
$root = realpath((string) InstalledVersions::getInstallPath($package));
$version = InstalledVersions::getPrettyVersion($package);
$reference = InstalledVersions::getReference($package);
requireUpdateAcceptance($version === getenv('SDK_VERSION') && is_string($reference)
    && preg_match('/\A[0-9a-f]{40}\z/', $reference) === 1 && $root !== false, 'Unexpected installed package identity.');
foreach ([Client::class, Worker::class, UpdateStillPending::class] as $class) {
    $file = realpath((string) (new ReflectionClass($class))->getFileName());
    requireUpdateAcceptance($file !== false && str_starts_with($file, $root.'/'), 'Source fallback is forbidden.');
}

// Observe only method/wait-stage metadata, keeping credentials out of evidence.
$submissions = [];
$reads = [];
$stack = HandlerStack::create();
$stack->push(Middleware::tap(static function (RequestInterface $request) use (&$submissions, &$reads): void {
    $path = $request->getUri()->getPath();
    if ($request->getMethod() === 'POST' && str_contains($path, '/update/')) {
        $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
        $submissions[] = $body['wait_for'] ?? null;
    }
    if ($request->getMethod() === 'GET' && str_contains($path, '/updates/')) {
        parse_str($request->getUri()->getQuery(), $query);
        $reads[] = $query['wait_for'] ?? 'accepted';
    }
}));
$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'),
    transport: new Psr18Transport(new HttpClient(['handler' => $stack])));
$info = $client->clusterInfo();
requireUpdateAcceptance($info->version === getenv('SERVER_VERSION') && ($info->raw['implementation'] ?? null) === 'rust',
    'The runtime must be the selected published native Server.');
$workflowId = 'sdk-php-pending-null-'.bin2hex(random_bytes(8));
$requestId = 'sdk-php-pending-null-request';
$queue = 'published-update-outcome-'.bin2hex(random_bytes(6));
$newWorker = static function () use ($client, $queue): Worker {
    $worker = new Worker($client, $queue);
    $worker->registerWorkflow('pending.null', static function (WorkflowContext $context) use ($worker): void {
        $worker->requestShutdown();
        $context->sleep(30);
    });
    $worker->registerUpdate('pending.null', 'null', static function (QueryContext $context) use ($worker): mixed {
        $worker->requestShutdown();
        return null;
    });
    return $worker;
};
$client->startWorkflow('pending.null', $workflowId, $queue, executionTimeoutSeconds: 60, runTimeoutSeconds: 60);
// The first ordinary task binds the update contract; then stop that worker so
// the accepted update outlives the caller's wait before a replacement handles it.
$newWorker()->run(0);

try {
    // Omit waitFor deliberately: the normal value-returning API waits for completion.
    $client->updateWorkflow($workflowId, 'null', waitTimeoutSeconds: 1, requestId: $requestId);
    throw new InstalledUpdateAcceptanceFailed('A queued update must return typed pending while workers are paused.');
} catch (UpdateStillPending $pending) {
    requireUpdateAcceptance($pending->workflowId === $workflowId && $pending->requestId === $requestId
        && is_string($pending->updateId) && $pending->updateId !== '' && is_string($pending->runId),
        'Typed pending must retain the workflow, run, update and request identities.');
    $updateId = $pending->updateId;
    $runId = $pending->runId;
}
$before = $client->updateOutcomeReceipt($workflowId, $updateId, waitTimeoutSeconds: 1);
requireUpdateAcceptance(($before['update_status'] ?? null) === 'accepted', 'The read-only raw receipt must remain pending.');
try {
    $client->updateOutcome($workflowId, $updateId, waitTimeoutSeconds: 1);
    throw new InstalledUpdateAcceptanceFailed('A pending outcome read must retain its typed distinction.');
} catch (UpdateStillPending $pendingRead) {
    requireUpdateAcceptance($pendingRead->updateId === $updateId && $pendingRead->workflowId === $workflowId
        && str_contains($pendingRead->getMessage(), 'updateOutcome()'), 'The pending read must retain its stable id and read path.');
    // Native 3.8 omits the original key on GET. Preserve the compatible unknown
    // representation instead of inventing a key or resubmitting the update.
    $readKey = $before['request_id'] ?? '';
    requireUpdateAcceptance($pendingRead->requestId === $readKey, 'The pending read must not invent a request key.');
}

$newWorker()->run(0);
requireUpdateAcceptance($client->updateOutcome($workflowId, $updateId, waitTimeoutSeconds: 2) === null,
    'A completed null must return PHP null, not a pending receipt.');
$after = $client->updateOutcomeReceipt($workflowId, $updateId, waitFor: 'completed', waitTimeoutSeconds: 2);
requireUpdateAcceptance(($after['update_status'] ?? null) === 'completed', 'The retained update must be completed.');
$events = iterator_to_array($client->iterateWorkflowHistory($workflowId, $runId));
$counts = array_count_values(array_column($events, 'event_type'));
requireUpdateAcceptance(($counts['UpdateAccepted'] ?? 0) === 1 && ($counts['UpdateCompleted'] ?? 0) === 1,
    'Outcome reads must not admit or complete another update.');
requireUpdateAcceptance($submissions === ['completed'] && $reads === ['accepted', 'completed', 'completed', 'completed'],
    'Default wait stages or the single-submission/read-only contract changed.');

echo json_encode(['outcome' => 'pass', 'sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'php_version' => PHP_VERSION,
    'initial_outcome' => UpdateStillPending::class, 'pending_read' => UpdateStillPending::class,
    'completed_result' => null, 'update_accepted_events' => $counts['UpdateAccepted'],
    'update_completed_events' => $counts['UpdateCompleted'], 'update_submissions' => count($submissions),
    'outcome_reads' => count($reads), 'submission_default_wait' => $submissions[0],
    'read_wait_stages' => $reads, 'pending_read_original_request_key_available' => $readKey !== '',
    'source_fallback' => false], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR).PHP_EOL;
