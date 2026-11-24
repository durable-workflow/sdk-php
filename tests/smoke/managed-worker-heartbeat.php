<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

function ensure(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function waitFor(Closure $observe, int $seconds, string $message): mixed
{
    $deadline = hrtime(true) + $seconds * 1_000_000_000;
    do {
        $observed = $observe();
        if ($observed !== null) {
            return $observed;
        }
        usleep(100_000);
    } while (hrtime(true) < $deadline);
    throw new RuntimeException($message);
}

ensure(isset($argv[1]), 'Pass the isolated Composer consumer vendor/autoload.php.');
require $argv[1];
ensure(function_exists('pcntl_signal') && function_exists('proc_open'), 'The fixture requires CLI pcntl and proc_open.');

final class HeartbeatObserver implements Transport
{
    private int $attempt = 0;

    public function __construct(
        private readonly Psr18Transport $http,
        private readonly string $workerId,
        private readonly bool $exerciseRetry,
    ) {}

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        if ($method === 'POST' && str_ends_with($uri, '/worker/register')) {
            $answer = $this->http->send($method, $uri, $headers, $body);
            echo json_encode(['event' => 'registered'], JSON_THROW_ON_ERROR).PHP_EOL;

            return $answer;
        }
        if ($method !== 'POST' || !str_ends_with($uri, '/worker/heartbeat')) {
            return $this->http->send($method, $uri, $headers, $body);
        }

        if ($this->exerciseRetry && ++$this->attempt === 2) {
            try {
                // The fixture has no listener on this loopback port. No credentials are sent.
                $this->http->send('POST', 'http://127.0.0.1:1/api/worker/heartbeat', []);
            } catch (TransportException $failure) {
                ensure($failure->transientConnectionFailure, 'Expected a refused connection in the retry case.');
                echo json_encode(['event' => 'connection_refused'], JSON_THROW_ON_ERROR).PHP_EOL;
                throw $failure;
            }
            throw new RuntimeException('The fixture loopback port unexpectedly answered.');
        }

        $answer = $this->http->send($method, $uri, $headers, $body);
        ensure(($answer['worker_id'] ?? null) === $this->workerId && ($answer['acknowledged'] ?? null) === true
            && ($answer['heartbeat_interval_seconds'] ?? null) === 2, 'Expected an acknowledged two-second heartbeat contract.');
        echo json_encode(['event' => 'acknowledged', 'at' => hrtime(true)], JSON_THROW_ON_ERROR).PHP_EOL;

        return $answer;
    }
}

$version = getenv('SDK_VERSION');
$reference = getenv('SDK_REFERENCE');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
$workerSource = realpath((string) (new ReflectionClass(Worker::class))->getFileName());
ensure(is_string($version) && is_string($reference) && preg_match('/\A[0-9a-f]{40}\z/', $reference) === 1
    && InstalledVersions::getPrettyVersion('durable-workflow/sdk') === $version
    && InstalledVersions::getReference('durable-workflow/sdk') === $reference
    && $root !== false && $workerSource !== false && str_starts_with($workerSource, $root.'/'),
    'The installed SDK does not match the selected version and source.');

$url = getenv('DURABLE_WORKFLOW_SERVER_URL');
$token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN');
ensure(is_string($url) && $url !== '' && is_string($token) && $token !== '', 'Set the isolated Server URL and token.');
$http = new Psr18Transport(requestTimeoutSeconds: 3, workerRequestTimeoutSeconds: 3);
$type = 'qualification.managed-heartbeat';

if (isset($argv[2], $argv[3], $argv[4])) {
    [, , $queue, $id, $case] = $argv;
    $client = new Client($url, transport: new HeartbeatObserver($http, $id, $case === 'retry'), token: $token);
    $worker = new Worker($client, $queue, workerId: $id, heartbeatIntervalSeconds: 4);
    $worker->registerWorkflow($type, static fn (WorkflowContext $context, string $value): string => $value);
    $worker->run(1);
    exit(0);
}

$client = new Client($url, transport: $http, token: $token);
$server = $client->clusterInfo();
ensure(is_string(getenv('SERVER_VERSION')) && $server->version === getenv('SERVER_VERSION')
    && ($server->raw['implementation'] ?? null) === 'rust', 'Use the selected published native Server.');
$queue = 'managed-heartbeat-'.bin2hex(random_bytes(6));
$workers = [];
$logs = [];

$lookup = static function (string $id, string $status) use ($client, $queue): ?array {
    $page = $client->listWorkers(taskQueue: $queue, status: $status);
    ensure(($page['stale_after_seconds'] ?? null) === 9, 'Start Server with a nine-second stale window.');
    foreach ($page['workers'] as $row) {
        if ($row['worker_id'] === $id) {
            return $row;
        }
    }

    return null;
};

$start = static function (string $case) use ($queue, $argv, $lookup, &$workers, &$logs): array {
    $id = $queue.'-'.$case;
    $log = tempnam(sys_get_temp_dir(), 'dw-heartbeat-');
    ensure($log !== false, 'Could not allocate a temporary worker log.');
    $logs[$id] = $log;
    $process = proc_open([PHP_BINARY, __FILE__, $argv[1], $queue, $id, $case], [
        ['file', '/dev/null', 'r'], ['file', $log, 'w'], ['redirect', 1],
    ], $pipes);
    ensure(is_resource($process), 'Could not start the worker process.');
    $workers[$id] = $process;
    $registration = waitFor(static function () use ($lookup, $id, $process): ?array {
        ensure(proc_get_status($process)['running'], 'Worker exited before registration.');

        return $lookup($id, 'active');
    }, 6, 'Worker did not register.');
    ensure($registration['runtime'] === 'php' && $registration['sdk_version'] === 'durable-workflow-php/'.getenv('SDK_VERSION'),
        'Unexpected worker runtime identity.');

    return [$id, $registration, $log];
};

$stop = static function (string $id, int $signal) use (&$workers): array {
    $process = $workers[$id];
    ensure(proc_terminate($process, $signal), 'Could not stop the worker.');
    $status = waitFor(static function () use ($process): ?array {
        $status = proc_get_status($process);

        return $status['running'] ? null : $status;
    }, 4, 'Worker did not exit after the signal.');
    proc_close($process);
    unset($workers[$id]);

    return $status;
};

$graceful = static function (string $id) use ($stop, $lookup): void {
    $status = $stop($id, SIGTERM);
    ensure($status['exitcode'] === 0 && $lookup($id, 'active') === null && $lookup($id, 'stale') === null,
        'Managed shutdown must exit cleanly and deregister the worker.');
};

try {
    [$id, $registered, $log] = $start('retry');
    $events = waitFor(static function () use ($log): ?array {
        $events = array_filter(array_map(
            static fn (string $line): mixed => json_decode($line, true),
            file($log, FILE_IGNORE_NEW_LINES|FILE_SKIP_EMPTY_LINES),
        ), static fn (mixed $row): bool => is_array($row) && isset($row['event']));

        return count($events) >= 5 ? array_values($events) : null;
    }, 12, 'The managed worker did not acknowledge heartbeats around a refused connection.');
    ensure(array_column(array_slice($events, 0, 5), 'event')
        === ['registered', 'acknowledged', 'connection_refused', 'acknowledged', 'acknowledged']
        && count(array_keys(array_column($events, 'event'), 'registered', true)) === 1,
        'Expected automatic heartbeats to resume without registering again.');
    $gap = ($events[4]['at'] - $events[3]['at']) / 1e9;
    $active = $lookup($id, 'active');
    ensure($gap > 0 && $gap <= 3 && $active !== null && $active['registered_at'] === $registered['registered_at']
        && $active['last_heartbeat_at'] > $registered['last_heartbeat_at'],
        'The managed worker did not maintain the negotiated cadence under its original registration.');
    $graceful($id);

    [$id] = $start('abrupt');
    $status = $stop($id, SIGKILL);
    ensure($status['signaled'] === true && $status['termsig'] === SIGKILL, 'The abrupt fixture exit did not occur.');
    waitFor(static fn (): ?array => $lookup($id, 'stale'), 13, 'The stopped worker did not become stale.');
    ensure($lookup($id, 'active') === null, 'The stopped worker is still active.');

    [$replacement] = $start('replacement');
    $result = $client->startWorkflow($type, $queue, $queue, ['replacement'])->resultOfSelectedRun(6, 0.1);
    ensure($result === 'replacement', 'The replacement worker did not complete the authored workflow.');
    $graceful($replacement);
} finally {
    foreach ($workers as $process) {
        proc_terminate($process, SIGKILL);
        proc_close($process);
    }
    foreach ($logs as $log) {
        unlink($log);
    }
}

echo json_encode([
    'outcome' => 'pass',
    'sdk_version' => $version,
    'sdk_reference' => $reference,
    'server_version' => $server->version,
    'negotiated_heartbeat_seconds' => 2,
    'heartbeat_gap_after_retry_seconds' => round($gap, 3),
    'retry_preserved_registration' => true,
    'graceful_deregistration' => true,
    'abrupt_exit_became_stale' => true,
    'replacement_completed' => true,
], JSON_THROW_ON_ERROR|JSON_PRETTY_PRINT).PHP_EOL;
