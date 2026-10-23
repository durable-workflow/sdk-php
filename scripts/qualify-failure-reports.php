<?php

declare(strict_types=1);

// Recording PSR-18 control. Pass a normal installation's autoloader.
require_once $argv[1];
$baseline = in_array('--baseline', $argv, true);
$http = new class implements Psr\Http\Client\ClientInterface {
    public array $requests = [];
    public function sendRequest(Psr\Http\Message\RequestInterface $request): Psr\Http\Message\ResponseInterface
    {
        $this->requests[] = $request;
        return new GuzzleHttp\Psr7\Response(200, ['Content-Type' => 'application/json'], '{}');
    }
};
$client = new DurableWorkflow\Client('https://recording.invalid',
    transport: new DurableWorkflow\Transport\Psr18Transport(client: $http));
$large = str_repeat('m', 3 * 1024 * 1024);
$results = [];
foreach (['activity', 'workflow-task'] as $kind) {
    $http->requests = [];
    if ($kind === 'activity') {
        $client->failActivityTask('original-task', 'original-attempt', 'original-owner', $large, 'OriginalFailure', true);
    } else {
        $client->failWorkflowTask('original-task', 'original-owner', 3, $large, 'OriginalFailure');
    }
    $request = $http->requests[array_key_last($http->requests)];
    $wire = (string) $request->getBody();
    $body = json_decode($wire, true, flags: JSON_THROW_ON_ERROR);
    $failure = $body['failure'];
    $results[$kind] = ['original_message_bytes' => strlen($large), 'encoded_request_bytes' => strlen($wire),
        'message_bytes' => strlen($failure['message']), 'original_type' => $failure['type'] === 'OriginalFailure',
        'exact_identity' => $body['lease_owner'] === 'original-owner'
            && ($kind === 'activity' ? $body['activity_attempt_id'] === 'original-attempt' : $body['workflow_task_attempt'] === 3),
        'marked_message' => str_contains($failure['message'], '[truncated failure report;'),
        'non_retryable' => $failure['non_retryable'] ?? null];
}
$pass = true;
foreach ($results as $result) {
    $pass = $pass && $result['original_type'] && $result['exact_identity']
        && ($baseline ? $result['encoded_request_bytes'] > 2097152 && !$result['marked_message']
            : $result['encoded_request_bytes'] <= 1048576 && $result['marked_message']);
}
echo json_encode(['baseline' => $baseline, 'sdk_version' => Composer\InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'pass' => $pass, 'results' => $results], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)."\n";
exit($pass ? 0 : 1);
