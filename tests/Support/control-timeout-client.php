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
if ($profile !== 'published_baseline') {
    $arguments['requestTimeoutSeconds'] = $transport === null ? (float) $seconds : null;
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
            if ($cause instanceof GuzzleHttp\Exception\RequestException || $cause instanceof GuzzleHttp\Exception\ConnectException) {
                $contexts[] = $cause->getHandlerContext();
            }
        }
    }
    echo json_encode(['outcome' => 'error', 'type' => $error::class, 'status' => $error->status,
        'reason' => $error->reason, 'message' => $error->getMessage(), 'elapsed' => (hrtime(true) - $started) / 1e9,
        'handler_contexts' => $contexts], JSON_THROW_ON_ERROR)."\n";
}
