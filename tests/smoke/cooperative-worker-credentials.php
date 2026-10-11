<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

require $argv[1];

$package = 'durable-workflow/sdk';
$version = Composer\InstalledVersions::getPrettyVersion($package);
$root = realpath(Composer\InstalledVersions::getInstallPath($package));
$implementation = realpath((new ReflectionClass(Client::class))->getFileName());
if ($version !== getenv('SDK_VERSION') || $root === false || $implementation === false
    || !str_starts_with($implementation, $root.'/')) {
    throw new RuntimeException('The smoke must execute the selected registry SDK, without source fallback.');
}

$url = 'http://127.0.0.1:8081';
$token = getenv('DURABLE_WORKFLOW_WORKER_TOKEN');
$client = new Client($url, namespace: 'default', workerToken: $token, workerProtocolVersion: '1.20');
$info = $client->workerClusterInfo();
if ($info->version !== getenv('COOPERATIVE_SERVER_VERSION')) {
    throw new RuntimeException('Discovery returned the wrong published runtime version.');
}

try {
    $client->clusterInfo(includeDiagnostics: true);
    throw new RuntimeException('The SDK diagnostics method must require a control credential.');
} catch (InvalidArgumentException $failure) {
    if (!str_starts_with($failure->getMessage(), 'A control credential is required for this request.')) {
        throw $failure;
    }
}

// Cluster discovery accepts authenticated roles, including diagnostics mode.
// Operator metrics and mutations have actual Server role restrictions.
$restricted = new Client($url, namespace: 'default', controlToken: $token);
foreach (['diagnostics', 'start'] as $operation) {
    try {
        if ($operation === 'diagnostics') {
            $restricted->operatorMetrics();
        } else {
            $restricted->startWorkflow('smoke.worker_discovery', 'must-refuse-'.bin2hex(random_bytes(8)), 'smoke');
        }
        throw new RuntimeException('The worker credential unexpectedly authorized '.$operation.'.');
    } catch (ServerException $failure) {
        if ($failure->status !== 403) {
            throw $failure;
        }
    }
}

$registered = $deregistered = false;
$workerId = 'published-discovery-'.bin2hex(random_bytes(8));
$worker = new Worker($client, $workerId, workerId: $workerId, enableCooperativeCancellation: true,
    diagnosticListener: static function (string $event, array $details) use (&$worker, &$registered, &$deregistered): void {
        if ($event === 'worker.registered') {
            $registered = true;
            $worker->requestShutdown();
        } elseif ($event === 'worker.deregistered') {
            $deregistered = true;
        }
    });
$worker->registerWorkflow('smoke.worker_discovery', static fn (WorkflowContext $context): string => 'unused');
$worker->run(0);
if (!$registered || !$deregistered) {
    throw new RuntimeException('The cooperative worker did not register and deregister successfully.');
}
echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => Composer\InstalledVersions::getReference($package),
    'server_version' => $info->version, 'server_image' => getenv('COOPERATIVE_SERVER_IMAGE'),
    'worker_protocol' => '1.20', 'worker_registered' => $registered, 'worker_deregistered' => $deregistered,
    'sdk_diagnostics_requires_control_credential' => true,
    'operator_metrics_refused_by_server' => true, 'control_mutation_refused_by_server' => true,
], JSON_THROW_ON_ERROR).PHP_EOL;
