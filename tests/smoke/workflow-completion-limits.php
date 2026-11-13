<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use Psr\Http\Message\RequestInterface;

require $argv[1];

function requireCompletionAcceptance(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$package = 'durable-workflow/sdk';
$version = InstalledVersions::getPrettyVersion($package);
$reference = InstalledVersions::getReference($package);
$root = realpath((string) InstalledVersions::getInstallPath($package));
requireCompletionAcceptance($version === getenv('SDK_VERSION') && is_string($reference)
    && preg_match('/\A[0-9a-f]{40}\z/', $reference) === 1 && $root !== false, 'Unexpected installed SDK identity.');
foreach ([Client::class, Worker::class] as $class) {
    $file = realpath((string) (new ReflectionClass($class))->getFileName());
    requireCompletionAcceptance($file !== false && str_starts_with($file, $root.'/'), 'Source fallback is forbidden.');
}
requireCompletionAcceptance(function_exists('pcntl_fork') && function_exists('posix_kill'), 'The acceptance profile requires process control.');

$client = new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'));
$info = $client->clusterInfo();
$cap = $info->limits['max_workflow_task_completion_bytes'] ?? null;
$ordinary = $info->limits['max_payload_bytes'] ?? null;
$storage = $info->namespace['external_payload_storage']['status'] ?? 'unsupported';
requireCompletionAcceptance($info->version === getenv('SERVER_VERSION') && ($info->raw['implementation'] ?? null) === 'rust'
    && $cap === 33554432 && is_int($ordinary) && $ordinary > 0
    && in_array($storage, ['unconfigured', 'disabled', 'unsupported'], true), 'Unexpected published native limits or storage state.');
$queue = 'completion-limits-'.bin2hex(random_bytes(6));
$evidenceFile = tempnam(sys_get_temp_dir(), 'sdk-php-completion-');
requireCompletionAcceptance(is_string($evidenceFile), 'Cannot create owned worker evidence.');
$bootstrapFile = tempnam(sys_get_temp_dir(), 'sdk-php-completion-lease-');
requireCompletionAcceptance(is_string($bootstrapFile), 'Cannot create owned maintenance bootstrap.');
// Authentication remains in the inherited fixture environment, never in this
// file or the evidence. Only the owner needs HTTP observation middleware.
file_put_contents($bootstrapFile, '<?php return new \\DurableWorkflow\\Client('
    .'getenv("METRICS_RUNTIME_URL"), token: getenv("METRICS_TOKEN"));');
$pid = pcntl_fork();
requireCompletionAcceptance($pid !== -1, 'Cannot start the acceptance worker.');
if ($pid === 0) {
    $trace = new class implements MetricsRecorder {
        public array $completions = [];
        public array $diagnostics = [];
        public array $activityCalls = [];
        public int $uploads = 0;
        public function increment(string $name, float $value = 1.0, array $tags = []): void
        {
            if ($name === 'durable_workflow_client_requests' && ($tags['method'] ?? null) === 'POST'
                && str_contains($tags['route'] ?? '', '/workflow-tasks/') && str_ends_with($tags['route'], '/complete')) {
                $index = array_key_last($this->completions);
                if ($index !== null) {
                    $this->completions[$index]['status'] = (int) $tags['status_code'];
                }
            }
        }
        public function record(string $name, float $value, array $tags = []): void {}
    };
    $exit = 0;
    $errorClass = null;
    try {
        // Observe only actual wire size, command counts and status: no headers,
        // credentials or payload contents enter the evidence.
        $stack = HandlerStack::create();
        $stack->push(Middleware::tap(static function (RequestInterface $request) use ($trace): void {
            $path = $request->getUri()->getPath();
            if ($request->getMethod() === 'POST' && str_contains($path, '/workflow-tasks/') && str_ends_with($path, '/complete')) {
                $body = (string) $request->getBody();
                $trace->completions[] = ['bytes' => strlen($body),
                    'schedules' => substr_count($body, '"type":"schedule_activity"')];
            }
            if (in_array($request->getMethod(), ['POST', 'PUT'], true) && str_contains($path, '/external-payloads/')) {
                ++$trace->uploads;
            }
        }));
        $workerClient = (new Client(getenv('METRICS_RUNTIME_URL'), token: getenv('METRICS_TOKEN'),
            transport: new Psr18Transport(new HttpClient(['handler' => $stack])), metrics: $trace))
            ->withActivityLeaseBootstrap($bootstrapFile);
        $worker = new Worker($workerClient, $queue, diagnosticListener: static function (string $event, array $details) use ($trace): void {
            if ($event === 'worker.workflow_task_completion_too_large') {
                $trace->diagnostics[] = array_intersect_key($details, array_flip(['limit_kind', 'limit_bytes', 'size_bytes']));
            }
        });
        $worker->registerActivity('completion.length', static function (ActivityContext $context, string $value) use ($trace): int {
            $bytes = strlen($value);
            $trace->activityCalls[$bytes] = ($trace->activityCalls[$bytes] ?? 0) + 1;
            return $bytes;
        });
        $worker->registerWorkflow('completion.fanout', static function (WorkflowContext $context, int $count, int $bytes): array {
            $operations = [];
            $argument = str_repeat('x', $bytes);
            for ($index = 0; $index < $count; ++$index) {
                $operations[] = $context->deferActivity('completion.length', [$argument]);
            }
            $lengths = $context->all($operations);
            return [count($lengths), array_sum($lengths)];
        });
        $worker->run(1);
    } catch (Throwable $error) {
        $exit = 1;
        $errorClass = $error::class;
    } finally {
        file_put_contents($evidenceFile, json_encode(['completions' => $trace->completions,
            'diagnostics' => $trace->diagnostics, 'activity_calls' => $trace->activityCalls,
            'uploads' => $trace->uploads, 'worker_error_class' => $errorClass], JSON_THROW_ON_ERROR));
    }
    exit($exit);
}

$parentError = null;
try {
    $start = static fn (int $count, int $bytes) => $client->startWorkflow('completion.fanout',
        'completion-'.bin2hex(random_bytes(8)), $queue, [$count, $bytes], executionTimeoutSeconds: 180, runTimeoutSeconds: 180);
    $normal = $start(300, 10000);
    requireCompletionAcceptance($normal->result(timeoutSeconds: 120) === [300, 3000000], 'The valid fan-out did not complete.');
    $over = $start(60, 450000);
    try {
        $over->result(timeoutSeconds: 40);
        throw new RuntimeException('The oversized workflow unexpectedly completed.');
    } catch (WorkflowFailed) {
        // The real Worker must settle the typed HTTP413 as one structural failure.
    }
    $followup = $start(1, 16);
    requireCompletionAcceptance($followup->result(timeoutSeconds: 30) === [1, 16], 'The same Worker did not complete the follow-up.');
    $events = iterator_to_array($client->iterateWorkflowHistory($over->workflowId, $over->selectedRunId));
    $counts = array_count_values(array_column($events, 'event_type'));
    $failures = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'WorkflowFailed'));
    requireCompletionAcceptance(($counts['ActivityScheduled'] ?? 0) === 0 && ($counts['WorkflowTaskFailed'] ?? 0) === 0
        && count($failures) === 1 && ($failures[0]['payload']['failure_category'] ?? null) === 'structural_limit',
        'The oversized aggregate admitted partial work or did not settle structurally.');
} catch (Throwable $error) {
    $parentError = $error;
} finally {
    posix_kill($pid, SIGTERM);
    pcntl_waitpid($pid, $workerStatus);
}

try {
    $evidence = json_decode((string) file_get_contents($evidenceFile), true, flags: JSON_THROW_ON_ERROR);
    if ($parentError !== null) {
        fwrite(STDERR, json_encode(['parent_error_class' => $parentError::class, 'worker' => $evidence], JSON_THROW_ON_ERROR).PHP_EOL);
        throw $parentError;
    }
    requireCompletionAcceptance(pcntl_wifexited($workerStatus) && pcntl_wexitstatus($workerStatus) === 0
        && $evidence['worker_error_class'] === null, 'The Worker did not shut down cleanly.');
    $large = array_values(array_filter($evidence['completions'], static fn (array $row): bool => $row['bytes'] > $cap));
    $valid = array_values(array_filter($evidence['completions'], static fn (array $row): bool => $row['schedules'] === 300));
    requireCompletionAcceptance(count($large) === 1 && $large[0]['status'] === 413 && $large[0]['schedules'] === 60
        && count($valid) === 1 && $valid[0]['status'] === 200 && $valid[0]['bytes'] > $ordinary
        && count($evidence['diagnostics']) === 1 && $evidence['diagnostics'][0]['limit_bytes'] === $cap
        && $evidence['diagnostics'][0]['size_bytes'] === $large[0]['bytes']
        && ($evidence['activity_calls'][10000] ?? 0) === 300 && ($evidence['activity_calls'][450000] ?? 0) === 0
        && ($evidence['activity_calls'][16] ?? 0) === 1 && $evidence['uploads'] === 0,
        'Wire, activity, no-upload or typed-refusal evidence differs from the contract.');
    echo json_encode(['outcome' => 'pass', 'sdk_version' => $version, 'sdk_reference' => $reference,
        'server_version' => $info->version, 'php_version' => PHP_VERSION, 'ordinary_limit_bytes' => $ordinary,
        'completion_limit_bytes' => $cap, 'storage_status' => $storage, 'normal' => $valid[0], 'over' => $large[0],
        'over_history_counts' => $counts, 'structural_diagnostic' => $evidence['diagnostics'][0],
        'activity_calls' => $evidence['activity_calls'], 'uploads' => 0, 'same_worker_followup' => true,
        'source_fallback' => false], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR).PHP_EOL;
} finally {
    unlink($evidenceFile);
    unlink($bootstrapFile);
}
