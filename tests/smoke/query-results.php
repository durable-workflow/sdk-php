<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

require $argv[1];

function requireQueryAcceptance(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$version = InstalledVersions::getPrettyVersion('durable-workflow/sdk');
$reference = InstalledVersions::getReference('durable-workflow/sdk');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
requireQueryAcceptance($version === getenv('SDK_VERSION') && is_string($reference)
    && preg_match('/\A[0-9a-f]{40}\z/', $reference) === 1 && $root !== false, 'Unexpected installed SDK identity.');
foreach ([Client::class, Worker::class, AvroBinaryValue::class] as $class) {
    $file = realpath((string) (new ReflectionClass($class))->getFileName());
    requireQueryAcceptance($file !== false && str_starts_with($file, $root.'/'), 'Source fallback is forbidden.');
}
requireQueryAcceptance(function_exists('pcntl_fork') && function_exists('posix_kill'), 'Process control is required.');
$url = getenv('METRICS_RUNTIME_URL');
$token = getenv('METRICS_TOKEN');
$client = new Client($url, token: $token);
$info = $client->clusterInfo();
requireQueryAcceptance($info->version === getenv('SERVER_VERSION') && ($info->raw['implementation'] ?? null) === 'rust',
    'Unexpected published native Server.');
$queue = 'query-results-'.bin2hex(random_bytes(6));
$pid = pcntl_fork();
requireQueryAcceptance($pid !== -1, 'Cannot start the query Worker.');
if ($pid === 0) {
    try {
        $worker = new Worker(new Client($url, token: $token), $queue);
        $worker->registerWorkflow('smoke.query-results', static function (WorkflowContext $context): void {
            if (!$context->waitCondition(static fn (): bool => $context->signals('finish') !== [],
                key: 'finish', timeout: 45)) {
                throw new RuntimeException('The query observation window expired.');
            }
        });
        $worker->registerQuery('smoke.query-results', 'binary', static fn (QueryContext $context): array =>
            ['nested' => [AvroBinaryValue::fromBytes("\xff\x00\x80")]]);
        $worker->registerQuery('smoke.query-results', 'json', static fn (QueryContext $context): array =>
            ['values' => [null, true, 'λ', 1.0, -0.0]]);
        $worker->registerQuery('smoke.query-results', 'null', static fn (QueryContext $context): mixed => null);
        $worker->registerSignal('smoke.query-results', 'finish', static function (): void {});
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, static fn () => $worker->requestShutdown());
        $worker->run(0);
        exit(0);
    } catch (Throwable $error) {
        fwrite(STDERR, 'Query Worker failed: '.$error::class.PHP_EOL);
        exit(1);
    }
}
$handle = null;
$failure = null;
try {
    $deadline = microtime(true) + 15;
    while (($client->listWorkers($queue)['workers'] ?? []) === []) {
        requireQueryAcceptance(microtime(true) < $deadline, 'The query Worker did not register.');
        usleep(50_000);
    }
    $handle = $client->startWorkflow('smoke.query-results', $queue, $queue,
        executionTimeoutSeconds: 60, runTimeoutSeconds: 60);
    $json = $handle->query('json');
    requireQueryAcceptance($json === ['values' => [null, true, 'λ', 1.0, -0.0]]
        && pack('E', $json['values'][4]) === pack('E', -0.0), 'JSON query values changed.');
    $binary = $handle->query('binary');
    requireQueryAcceptance(is_array($binary) && ($binary['nested'][0] ?? null) instanceof AvroBinaryValue
        && $binary['nested'][0]->bytes === "\xff\x00\x80", 'The nested binary query did not round-trip.');
    requireQueryAcceptance($handle->query('null') === null, 'The null query result changed.');
    $handle->signal('finish');
    requireQueryAcceptance($handle->result(timeoutSeconds: 15, pollIntervalSeconds: 0.1) === null,
        'The query workflow did not complete.');
} catch (Throwable $error) {
    $failure = $error;
    if ($handle !== null) {
        try {
            $handle->terminate('Query acceptance cleanup');
        } catch (Throwable $cleanup) {
            fwrite(STDERR, 'Query cleanup failed: '.$cleanup::class.PHP_EOL);
        }
    }
} finally {
    posix_kill($pid, SIGTERM);
    $deadline = microtime(true) + 15;
    while (pcntl_waitpid($pid, $status, WNOHANG) === 0) {
        if (microtime(true) >= $deadline) {
            posix_kill($pid, SIGKILL);
            pcntl_waitpid($pid, $status);
            $failure ??= new RuntimeException('The query Worker failed to stop gracefully.');
            break;
        }
        usleep(50_000);
    }
    if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
        $failure ??= new RuntimeException('The query Worker exited unsuccessfully.');
    }
}
if ($failure !== null) {
    throw $failure;
}
requireQueryAcceptance(($client->listWorkers($queue)['workers'] ?? []) === [], 'The query Worker did not deregister.');
echo json_encode(['sdk_version' => $version, 'sdk_reference' => $reference,
    'server_version' => $info->version, 'binary_bytes' => 3, 'nested_binary_roundtrip' => true,
    'json_projection_control' => true, 'signed_zero_preserved' => true, 'null_result' => true,
    'workflow_completed' => true, 'worker_deregistered' => true], JSON_THROW_ON_ERROR).PHP_EOL;
