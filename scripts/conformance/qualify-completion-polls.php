<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Qualification\CompletionPollEvidence;
use DurableWorkflow\Qualification\CompletionPollFailure;
use DurableWorkflow\Qualification\CompletionPollPacketDefaults;
use DurableWorkflow\Qualification\CompletionPollOppositeSlots;
use DurableWorkflow\Qualification\CompletionPollSubject;
use DurableWorkflow\Qualification\CompletionPollTrace;
use DurableWorkflow\Tests\Support\CompletionPipelineScenario;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

require_once __DIR__.'/Support/CompletionPollSubject.php';
require_once __DIR__.'/Support/CompletionPollTrace.php';
require_once __DIR__.'/Support/CompletionPollEvidence.php';
require_once dirname(__DIR__, 2).'/tests/Support/CompletionPipelineScenario.php';

/** @param list<array<string, mixed>> $events */
function completionParentSent(array $events, string $prefix): bool
{
    $taskId = null;
    foreach ($events as $event) {
        if ($event['kind'] === 'upstream') {
            foreach ([$event['answer']['task'] ?? null, $event['answer']['workflow_poll']['task'] ?? null] as $task) {
                if (($task['workflow_id'] ?? null) === $prefix.'-2') {
                    $taskId = $task['task_id'];
                }
            }
        }
        if ($taskId !== null && $event['kind'] === 'request'
            && str_ends_with($event['path'], '/workflow-tasks/'.$taskId.'/complete')) {
            return true;
        }
    }
    return false;
}

/** @param list<mixed> $arguments */
function completionIndex(array $arguments): int
{
    if (count($arguments) !== 1 || !is_int($arguments[0])) {
        throw new RuntimeException('completion_workflow_arguments');
    }
    return $arguments[0];
}

/** The caller owns the already-running disposable runtime and its image provenance.
 * @return array<string, mixed>
 */
function completionScenario(CompletionPollSubject $subject, string $clientToken, string $workerToken): array
{
    return CompletionPipelineScenario::run(static function (string $relay, string $stateFile) use ($subject, $clientToken, $workerToken): array {
        $data = $subject->data;
        $prefix = 'completion-'.bin2hex(random_bytes(8));
        $workerId = $prefix.'-worker';
        $queue = $prefix.'-queue';
        $pipeline = in_array($data['scenario'], ['pipeline', 'pipeline-loss', 'pipeline-buffer-age', 'prefetch-loss'], true);
        $localWorkload = $pipeline || in_array($data['scenario'], ['default-off', 'explicit-off'], true);
        $claimRefusal = in_array($data['scenario'], ['carrier-expiry', 'carrier-refence'], true);
        $slow = in_array($data['scenario'], ['completion-transit', 'carrier-expiry', 'carrier-refence'], true);
        $httpTimeout = $slow ? $data['deadline_seconds'] : 5;
        file_put_contents($stateFile, json_encode(['events' => [], 'endpoint' => $data['endpoint'],
            'prefix' => $prefix, 'scenario' => $data['scenario'], 'deadline_at' => hrtime(true) / 1e9 + $data['deadline_seconds']], JSON_THROW_ON_ERROR));
        $trace = new CompletionPollTrace($stateFile);
        $control = new Client($relay, namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 2);
        if ($control->clusterInfo()->version !== $data['server_version']) {
            throw new RuntimeException('completion_runtime_version');
        }
        $cleanup = new Client($data['endpoint'], namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 1);
        $actor = new Client($data['endpoint'], namespace: $data['namespace'], workerToken: $workerToken, requestTimeoutSeconds: 2);
        $deadline = microtime(true) + $data['deadline_seconds'];
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('completion_worker_start');
        }
        if ($pid === 0) {
            try {
                if (posix_setsid() === -1) {
                    throw new RuntimeException('completion_worker_process_group');
                }
                $arguments = ['workerId' => $workerId];
                if ($pipeline || $data['scenario'] === 'explicit-off') {
                    $arguments['enableCompletionPipelining'] = $pipeline;
                }
                if (in_array($data['scenario'], ['default-off', 'explicit-off'], true)) {
                    foreach (['__construct', 'create'] as $method) {
                        $options = array_filter((new ReflectionMethod(Worker::class, $method))->getParameters(),
                            static fn ($parameter): bool => $parameter->getName() === 'enableCompletionPipelining');
                        if (count($options) !== 1 || current($options)->getDefaultValue() !== false) {
                            throw CompletionPollFailure::product('completion_default_option_not_false');
                        }
                    }
                    $trace->record(['kind' => 'default_option', 'default' => false, 'explicit' => $data['scenario'] === 'explicit-off']);
                }
                if ($data['scenario'] === 'pipeline-buffer-age') $arguments['heartbeatIntervalSeconds'] = 1;
                $arguments['transientPollRetryObserver'] = static function (string $kind, int $attempt, float $delay, ServerException $error) use ($trace): void {
                    $trace->record(['kind' => 'poll_retry', 'task_kind' => $kind, 'attempt' => $attempt, 'delay' => $delay,
                        'status' => $error->status, 'reason' => $error->reason]);
                };
                $worker = new Worker(new Client($relay, namespace: $data['namespace'], workerToken: $workerToken,
                    requestTimeoutSeconds: 5, workerRequestTimeoutSeconds: $httpTimeout), $queue, ...$arguments);
                $worker->registerWorkflow('qualification.completion', static function (WorkflowContext $context, mixed ...$arguments) use ($trace, $prefix, $pipeline, $localWorkload): mixed {
                    $index = completionIndex($arguments);
                    if ($pipeline && $index === 3) {
                        $until = microtime(true) + 1;
                        while (!completionParentSent($trace->state()['events'], $prefix)) {
                            if (microtime(true) >= $until) {
                                throw new RuntimeException('completion_parent_body_not_observed');
                            }
                            usleep(1000);
                        }
                    }
                    $trace->record(['kind' => 'callback', 'index' => $index,
                        'workflow_id' => $context->workflowId, 'run_id' => $context->runId]);
                    if ($localWorkload) {
                        if ($index > 1) {
                            for ($step = 0; $step < 5; ++$step) {
                                $context->localActivity('qualification.local', [$index]);
                            }
                        }
                        return $index;
                    }
                    return $context->activity('qualification.remote', [$index]);
                });
                $worker->registerActivity('qualification.remote', static function (ActivityContext $context, mixed ...$arguments) use ($trace): int {
                    $index = completionIndex($arguments);
                    $trace->record(['kind' => 'activity', 'task_id' => $context->taskId, 'index' => $index]);
                    return $index;
                });
                $localCalls = [];
                $worker->registerActivity('qualification.local', static function (ActivityContext $context, mixed ...$arguments) use ($trace, $data, &$localCalls): int {
                    $index = completionIndex($arguments);
                    $trace->record(['kind' => 'local', 'index' => $index]);
                    if ($index === 3) {
                        $trace->change(static function (array &$state): void {
                            $state['release_parent'] = true;
                        });
                    }
                    $localCalls[$index] = ($localCalls[$index] ?? 0) + 1;
                    usleep($data['scenario'] === 'pipeline-buffer-age' && $index === 2 && $localCalls[$index] === 5 ? 1_250_000 : 20_000);
                    return $index;
                });
                // Queue the complete controlled workload before the worker
                // starts, so eager carriers have real eligible successors.
                while (!(($trace->state())['workload_ready'] ?? false)) {
                    if (microtime(true) >= $deadline) throw new RuntimeException('completion_workload_start_deadline');
                    usleep(10_000);
                }
                $worker->run(1);
                exit(0);
            } catch (Throwable $failure) {
                // Headers, payloads and arbitrary runtime exception messages stay out of diagnostics.
                $trace->record(['kind' => 'worker_error', 'class' => $failure::class,
                    'status' => $failure instanceof ServerException ? $failure->status : null,
                    'reason' => $failure instanceof ServerException ? $failure->reason : null]);
                exit(1);
            }
        }
        $handles = [];
        for ($index = 1; $index <= ($localWorkload ? 5 : 3); ++$index) {
            // Retain every owned ID before a start can have an unknown result.
            $handles[$index] = $control->workflowHandle($prefix.'-'.$index);
        }
        $failure = null;
        $expectedStop = false;
        $replacementToken = null;
        try {
            for ($index = 1; $index <= ($localWorkload ? 5 : 3); ++$index) {
                $handles[$index] = $control->startWorkflow('qualification.completion', $prefix.'-'.$index, $queue,
                    input: [$index], executionTimeoutSeconds: 180, runTimeoutSeconds: 150);
            }
            $trace->change(static function (array &$value): void { $value['workload_ready'] = true; });
            while (($control->listWorkers($queue)['workers'] ?? []) === []) {
                if (microtime(true) >= $deadline) throw new RuntimeException('completion_registration_deadline');
                usleep(50_000);
            }
            $pending = $handles;
            while ($pending !== []) {
                if (microtime(true) >= $deadline) throw new RuntimeException('completion_workflow_deadline');
                $state = $trace->state();
                if (($state['refence_ready'] ?? false) && $replacementToken === null) {
                    $old = array_values(array_filter($state['events'], static fn (array $event): bool =>
                        $event['kind'] === 'upstream' && isset($event['answer']['registration_sha256'])));
                    if ($old === []) throw new RuntimeException('completion_refence_receipt_required');
                    $replacement = $actor->registerWorker($workerId, $queue, ['qualification.completion'],
                        ['qualification.remote', 'qualification.local']);
                    $replacementToken = $replacement['registration_token'] ?? null;
                    if (($replacement['registered'] ?? null) !== true || ($replacement['worker_id'] ?? null) !== $workerId
                        || ($replacement['task_queue'] ?? null) !== $queue
                        || !is_string($replacementToken) || preg_match('/\A[a-f0-9]{32}\z/', $replacementToken) !== 1) {
                        throw new RuntimeException('completion_refence_receipt_required');
                    }
                    $digest = hash('sha256', $replacementToken);
                    if ($digest === $old[0]['answer']['registration_sha256']) {
                        throw CompletionPollFailure::product('completion_refence_token_not_rotated');
                    }
                    $trace->record(['kind' => 'refence', 'worker_id' => $workerId,
                        'old_registration_sha256' => $old[0]['answer']['registration_sha256'], 'registration_sha256' => $digest]);
                    $trace->change(static function (array &$value): void { $value['refence_complete'] = true; });
                }
                if (array_filter($state['events'], static fn (array $event): bool => $event['kind'] === 'relay_error') !== []) {
                    throw new RuntimeException('completion_relay_failed');
                }
                if ($claimRefusal && array_filter($state['events'], static fn (array $event): bool => $event['kind'] === 'worker_error') !== []) {
                    $expectedStop = true;
                    break;
                }
                foreach ($pending as $index => $handle) {
                    $description = $handle->describeSelectedRun();
                    if ($description->status === 'completed') {
                        if ($handle->resultOfSelectedRun(timeoutSeconds: 1, pollIntervalSeconds: 0.1) !== $index) {
                            throw CompletionPollFailure::product('completion_workflow_result');
                        }
                        unset($pending[$index]);
                    } elseif (in_array($description->status, ['failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                        throw CompletionPollFailure::product('completion_workflow_result');
                    }
                }
                if ($pending !== []) usleep(50_000);
            }
        } catch (Throwable $error) {
            $failure = $error;
        } finally {
            $cleanupDeadline = microtime(true) + 30;
            // The child owns this new process group, including its SDK maintenance helpers.
            if (!posix_kill(-$pid, SIGTERM)) {
                posix_kill($pid, SIGTERM);
            }
            $until = microtime(true) + 15;
            $status = 0;
            while (($waited = pcntl_waitpid($pid, $status, WNOHANG)) === 0) {
                if (microtime(true) >= $until) {
                    posix_kill(-$pid, SIGKILL);
                    posix_kill($pid, SIGKILL);
                    pcntl_waitpid($pid, $status);
                    $failure = new RuntimeException('completion_worker_cleanup_deadline', previous: $failure);
                    break;
                }
                usleep(50_000);
            }
            if ($waited === -1) {
                $failure = new RuntimeException('completion_worker_reap_failed', previous: $failure);
            } elseif (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                if (!$expectedStop) $failure ??= new RuntimeException('completion_worker_failed');
            }
            if (posix_kill(-$pid, 0)) {
                posix_kill(-$pid, SIGKILL);
                $failure = new RuntimeException('completion_worker_helper_survived', previous: $failure);
            }
            if ($failure !== null || $expectedStop) {
                foreach ($handles as $handle) {
                    if (microtime(true) >= $cleanupDeadline - 2) {
                        $trace->record(['kind' => 'cleanup_error']);
                        $failure = new RuntimeException('completion_workflow_cleanup_deadline', previous: $failure);
                        break;
                    }
                    try {
                        $description = $cleanup->describeWorkflow($handle->workflowId, $handle->selectedRunId);
                        if (!in_array($description->status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                            $cleanup->terminateWorkflow($handle->workflowId, 'Completion qualification cleanup', $handle->selectedRunId);
                        }
                    } catch (Throwable $cleanupError) {
                        if ($cleanupError instanceof ServerException && $cleanupError->status === 404 && $handle->selectedRunId === null) continue;
                        $trace->record(['kind' => 'cleanup_error']);
                        $failure = new RuntimeException('completion_workflow_cleanup_failed', previous: $cleanupError);
                    }
                }
            }
            if (is_string($replacementToken)) {
                try {
                    $actor->deregisterWorkerRegistration($workerId, $replacementToken);
                } catch (Throwable $error) {
                    $failure = new RuntimeException('completion_refence_cleanup_failed', previous: $error);
                }
            }
        }
        if ($failure !== null) {
            return ['scenario' => $data['scenario'], 'outcome' => CompletionPollFailure::diagnostic($failure)['outcome'],
                'diagnostic' => CompletionPollFailure::diagnostic($failure), 'events' => $trace->state()['events']];
        }
        if (($cleanup->listWorkers($queue)['workers'] ?? []) !== []) {
            throw new RuntimeException('completion_worker_registration_survived');
        }
        $events = $trace->state()['events'];
        try {
            $assertions = CompletionPollEvidence::verify($events, $data['scenario'], $workerId, $prefix);
            return ['scenario' => $data['scenario'], 'outcome' => 'pass', 'assertions' => $assertions,
                'workflow_count' => count($handles), 'worker_deregistered' => true, 'expected_claim_refusal_stop' => $expectedStop, 'events' => $events];
        } catch (Throwable $error) {
            return ['scenario' => $data['scenario'], 'outcome' => CompletionPollFailure::diagnostic($error)['outcome'],
                'diagnostic' => CompletionPollFailure::diagnostic($error), 'events' => $events];
        }
    }, __DIR__.'/Support/completion-poll-relay.php', true);
}

try {
    if (count($argv) !== 4) {
        throw new RuntimeException('completion_arguments');
    }
    CompletionPollSubject::freshProcess();
    $subject = CompletionPollSubject::read($argv[1], $argv[2], $argv[3]);
    require $argv[1];
    $identity = $subject->installed();
    if (!function_exists('pcntl_fork') || !function_exists('posix_kill') || !extension_loaded('curl')) {
        throw new RuntimeException('completion_toolchain');
    }
    if ($subject->data['scenario'] === 'packet-defaults') {
        require_once __DIR__.'/Support/CompletionPollPacketDefaults.php';
        echo json_encode(['schema' => 'durable-workflow.php-completion-polls-result/v1', 'outcome' => 'pass',
            'identity' => $identity, 'result' => CompletionPollPacketDefaults::run()], JSON_THROW_ON_ERROR).PHP_EOL;
        exit(0);
    }
    $clientToken = getenv('COMPLETION_TEST_CLIENT_TOKEN');
    $workerToken = getenv('COMPLETION_TEST_WORKER_TOKEN');
    if (!is_string($clientToken) || $clientToken === '' || !is_string($workerToken) || $workerToken === '' || $clientToken === $workerToken) {
        throw new RuntimeException('completion_scoped_test_tokens');
    }
    if ($subject->data['scenario'] === 'opposite-slots') {
        require_once __DIR__.'/Support/CompletionPollOppositeSlots.php';
        $result = CompletionPollOppositeSlots::run($subject, $clientToken, $workerToken);
    } else {
        $result = completionScenario($subject, $clientToken, $workerToken);
    }
    $outcome = $result['outcome'];
    echo json_encode(['schema' => 'durable-workflow.php-completion-polls-result/v1', 'outcome' => $outcome,
        'scope' => 'installed SDK on caller-owned loopback runtime; image publication/digest and execution admission are caller evidence',
        'identity' => $identity, 'server_version' => $subject->data['server_version'],
        'caller_server_image' => $subject->data['server_image'], 'result' => $result], JSON_THROW_ON_ERROR).PHP_EOL;
    exit($outcome === 'pass' ? 0 : 1);
} catch (Throwable $error) {
    fwrite(STDERR, json_encode(CompletionPollFailure::diagnostic($error), JSON_THROW_ON_ERROR).PHP_EOL);
    exit(1);
}
