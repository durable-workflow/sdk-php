<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;

final class ServiceNamespaceQualificationFailure extends RuntimeException {}

if (($argv[3] ?? null) === '--worker') {
    [$role, $namespace, $targetNamespace, $queue] = array_slice($argv, 4);
    $worker = new Worker(new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'), $queue,
        workerId: $queue.'-'.$role, diagnosticListener: static function (string $event): void {
            echo json_encode(['event' => $event, 'pid' => getmypid()], JSON_THROW_ON_ERROR)."\n";
        });
    if ($role === 'target') {
        $worker->registerWorkflow('qualification.namespace.target', static fn (WorkflowContext $context, array $request): array => $request);
    } else {
        $worker->registerWorkflow('qualification.namespace.caller', static function (WorkflowContext $context, array $request) use ($targetNamespace): array {
            $call = $context->serviceOperation('billing', 'invoices', 'create', $request,
                new ServiceOperationOptions(targetNamespace: $targetNamespace));
            return ['response' => $call->responsePayload, 'call_id' => $call->serviceCallId];
        });
    }
    $worker->run(0);
    exit(0);
}

// Use only the disposable token-auth runtime from the documented proxy topology.
$admin = new Client($url, controlToken: 'claimed-task-admin');
$suffix = bin2hex(random_bytes(6));
$callerNamespace = 'php-caller-'.$suffix;
$targetNamespace = 'php-target-'.$suffix;
$queue = 'php-service-'.$suffix;
$directory = sys_get_temp_dir().'/'.$queue;
mkdir($directory, 0700);
$processes = [];
try {
    $admin->createNamespace($callerNamespace);
    $admin->createNamespace($targetNamespace);
    $transport = new Psr18Transport();
    $headers = ['Authorization' => 'Bearer claimed-task-admin', 'X-Namespace' => $targetNamespace,
        'Content-Type' => 'application/json', 'Accept' => 'application/json', 'X-Durable-Workflow-Control-Plane-Version' => '2'];
    $transport->send('POST', $url.'/api/service-endpoints', $headers, ['endpoint_name' => 'billing']);
    $transport->send('POST', $url.'/api/service-endpoints/billing/services', $headers, ['service_name' => 'invoices']);
    $transport->send('POST', $url.'/api/service-endpoints/billing/services/invoices/operations', $headers, [
        'operation_name' => 'create', 'operation_mode' => 'sync', 'handler_binding_kind' => 'start_workflow',
        'handler_target_reference' => 'qualification.namespace.target',
        'handler_binding' => ['workflow_type' => 'qualification.namespace.target', 'queue' => $queue],
        'deadline_policy' => ['timeout_seconds' => 30],
        'boundary_policy' => ['caller_namespaces' => ['allow' => [$callerNamespace]], 'required_roles' => ['worker']],
    ]);
    foreach (['target' => $targetNamespace, 'caller' => $callerNamespace] as $role => $namespace) {
        $stdout = $directory.'/'.$role.'.stdout';
        $stderr = $directory.'/'.$role.'.stderr';
        $process = proc_open([PHP_BINARY, __FILE__, $autoload, $url, '--worker', $role, $namespace, $targetNamespace, $queue],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', $stdout, 'w'], 2 => ['file', $stderr, 'w']], $pipes);
        if (!is_resource($process)) {
            throw new ServiceNamespaceQualificationFailure('Could not start '.$role.' worker.');
        }
        $processes[$role] = $process;
        $deadline = microtime(true) + 20;
        while (!str_contains((string) file_get_contents($stdout), 'worker.registered')) {
            if (!proc_get_status($process)['running'] || microtime(true) >= $deadline) {
                throw new ServiceNamespaceQualificationFailure($role.' registration failed: '.file_get_contents($stderr));
            }
            usleep(10000);
        }
    }
    $client = new Client($url, namespace: $callerNamespace, controlToken: 'claimed-task-client');
    $request = ['max' => PHP_INT_MAX, 'text' => '雪☃', 'null' => null];
    $handle = $client->startWorkflow('qualification.namespace.caller', $queue, $queue, [$request]);
    $output = $handle->resultOfSelectedRun(30, 0.05);
    if (($output['response'] ?? null) !== $request || !is_string($output['call_id'] ?? null)) {
        throw new ServiceNamespaceQualificationFailure('Typed cross-namespace result did not preserve its value and call identity.');
    }
    $page = $client->workflowHistory($handle->workflowId, (string) $handle->selectedRunId, 100);
    $events = $page['events'] ?? $page['history_events'] ?? [];
    $counts = array_count_values(array_column($events, 'event_type'));
    if (($page['next_page_token'] ?? null) !== null || ($counts['ServiceCallStarted'] ?? 0) !== 1
        || ($counts['ServiceCallCompleted'] ?? 0) !== 1 || ($counts['WorkflowCompleted'] ?? 0) !== 1) {
        throw new ServiceNamespaceQualificationFailure('Canonical caller history does not contain exactly one call and terminal delivery.');
    }
    $started = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ServiceCallStarted'))[0]['payload'];
    $binding = $started['service_call'] ?? [];
    if (($binding['caller_namespace'] ?? null) !== $callerNamespace || ($binding['namespace'] ?? null) !== $targetNamespace
        || ($binding['service_call_id'] ?? $binding['id'] ?? null) !== $output['call_id']) {
        throw new ServiceNamespaceQualificationFailure('Server did not retain the original caller authority and target binding.');
    }
    echo json_encode(['outcome' => 'pass', 'sdk_version' => InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
        'sdk_reference' => InstalledVersions::getReference('durable-workflow/sdk'), 'server_version' => $admin->clusterInfo()->version,
        'caller_namespace' => $callerNamespace, 'target_namespace' => $targetNamespace, 'workflow_id' => $handle->workflowId,
        'run_id' => $handle->selectedRunId, 'call_id' => $output['call_id'], 'event_counts' => $counts],
        JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE)."\n";
} catch (Throwable $error) {
    $diagnostics = [];
    foreach (glob($directory.'/*.stderr') as $file) {
        $diagnostics[basename($file)] = file_get_contents($file);
    }
    throw new ServiceNamespaceQualificationFailure(json_encode($diagnostics, JSON_THROW_ON_ERROR), previous: $error);
} finally {
    foreach ($processes as $process) {
        proc_terminate($process);
        proc_close($process);
    }
    foreach (glob($directory.'/*') as $file) {
        unlink($file);
    }
    rmdir($directory);
}
