<?php

declare(strict_types=1);

require_once $argv[1];
[$script, $autoload, $url, $operation, $seconds] = $argv;
$profile = $argv[5] ?? 'default';
$caFile = $argv[6] ?? '';
$authentication = new class implements DurableWorkflow\Auth\Authentication {
    private int $calls = 0;
    public function headers(bool $workerRequest): array
    {
        return ['Authorization' => 'Bearer rotated-'.++$this->calls];
    }
};
$transport = $profile === 'injected' ? new DurableWorkflow\Transport\Psr18Transport(
    new GuzzleHttp\Client(['verify' => $caFile, 'timeout' => (float) $seconds]),
) : null;
$arguments = [
    'baseUri' => $profile === 'uri_basic' ? str_replace('://', '://uri-user:uri-pass@', $url) : $url,
    'authentication' => $profile === 'uri_basic' ? null : $authentication,
    'transport' => $transport,
];
$workerOperation = str_starts_with($operation, 'worker-');
if ($profile !== 'published_baseline') {
    // Worker operations get the configured worker deadline and a generous
    // control deadline, so only the worker budget can bound them. The
    // worker_default profile keeps the SDK's worker default instead.
    $arguments['requestTimeoutSeconds'] = $transport === null ? ($workerOperation && $profile !== 'worker_default' ? 30.0 : (float) $seconds) : null;
    if ($transport === null && $workerOperation && $profile !== 'worker_default') {
        $arguments['workerRequestTimeoutSeconds'] = (float) $seconds;
    }
}
$client = new DurableWorkflow\Client(...$arguments);
$started = hrtime(true);
try {
    match ($operation) {
        'list' => $client->listWorkflows(),
        'worker' => $client->workerClusterInfo(),
        'payload' => (new DurableWorkflow\Transport\Psr18Transport(requestTimeoutSeconds: (float) $seconds))
            ->uploadPayload($url.'/api/external-payloads/v1', [], 'fixture', 1),
        'start' => $client->startWorkflow('test.timeout', 'test-timeout', 'test-timeout'),
        'signal' => $client->signalWorkflow('test-timeout', 'test.signal'),
        'update' => $client->updateWorkflow('test-timeout', 'test.update'),
        'worker-register' => $client->registerWorker('test-worker', 'test-queue', ['test.workflow'], []),
        'worker-heartbeat' => $client->heartbeatWorker('test-worker'),
        'worker-activity-heartbeat' => $client->heartbeatActivityTask('test-task', 'test-attempt', 'test-worker'),
        'worker-complete-workflow' => $client->completeWorkflowTask('test-task', 'test-worker', 1, []),
        'worker-fail-workflow' => $client->failWorkflowTask('test-task', 'test-worker', 1, 'test failure'),
        'worker-complete-activity' => $client->completeActivityTask('test-task', 'test-attempt', 'test-worker', 'done'),
        'worker-fail-activity' => $client->failActivityTask('test-task', 'test-attempt', 'test-worker', 'test failure'),
        'worker-deregister' => $client->deregisterWorkerRegistration('test-worker'),
        // A session ID ending in "poll" is not a task poll (DELETE .../worker/sessions/poll).
        'worker-close-session-poll' => $client->closeWorkerSession('test-worker', 'poll'),
        'worker-poll' => $client->pollActivityTaskResponse('test-worker', 'test-queue', 1),
        default => throw new InvalidArgumentException('Unknown control fixture operation.'),
    };
    if ($profile === 'dynamic') {
        $client->withNamespace('billing')->listWorkflows(workflowType: 'case/空间?');
    }
    echo json_encode(['outcome' => 'response', 'elapsed' => (hrtime(true) - $started) / 1e9], JSON_THROW_ON_ERROR)."\n";
} catch (DurableWorkflow\Exception\ServerException $error) {
    $contexts = [];
    if ($profile === 'published_baseline') {
        for ($cause = $error->getPrevious(); $cause !== null; $cause = $cause->getPrevious()) {
            if (method_exists($cause, 'getHandlerContext')) {
                $contexts[] = $cause->getHandlerContext();
            }
        }
    }
    echo json_encode(['outcome' => 'error', 'type' => $error::class, 'status' => $error->status,
        'reason' => $error->reason, 'message' => $error->getMessage(), 'elapsed' => (hrtime(true) - $started) / 1e9,
        'handler_contexts' => $contexts], JSON_THROW_ON_ERROR)."\n";
}
