<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Qualification\CompletionPollEvidence;
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
        $pipeline = in_array($data['scenario'], ['pipeline', 'pipeline-loss'], true);
        file_put_contents($stateFile, json_encode(['events' => [], 'endpoint' => $data['endpoint'],
            'prefix' => $prefix, 'scenario' => $data['scenario']], JSON_THROW_ON_ERROR));
        $trace = new CompletionPollTrace($stateFile);
        $control = new Client($relay, namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 5);
        if ($control->clusterInfo()->version !== $data['server_version']) {
            throw new RuntimeException('completion_runtime_version');
        }
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
                if ($pipeline) {
                    $arguments['enableCompletionPipelining'] = true;
                }
                $worker = new Worker(new Client($relay, namespace: $data['namespace'], workerToken: $workerToken,
                    requestTimeoutSeconds: 5, workerRequestTimeoutSeconds: 5), $queue, ...$arguments);
                $worker->registerWorkflow('qualification.completion', static function (WorkflowContext $context, mixed ...$arguments) use ($trace, $prefix, $pipeline): mixed {
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
                    if ($pipeline) {
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
                $worker->registerActivity('qualification.local', static function (ActivityContext $context, mixed ...$arguments) use ($trace): int {
                    $index = completionIndex($arguments);
                    $trace->record(['kind' => 'local', 'index' => $index]);
                    if ($index === 3) {
                        $trace->change(static function (array &$state): void {
                            $state['release_parent'] = true;
                        });
                    }
                    usleep(20_000);
                    return $index;
                });
                $worker->run(1);
                exit(0);
            } catch (Throwable $failure) {
                // Headers, payloads and arbitrary runtime exception messages stay out of diagnostics.
                $trace->record(['kind' => 'worker_error', 'class' => $failure::class]);
                exit(1);
            }
        }
        $handles = [];
        $failure = null;
        try {
            while (($control->listWorkers($queue)['workers'] ?? []) === []) {
                if (microtime(true) >= $deadline) {
                    throw new RuntimeException('completion_registration_deadline');
                }
                usleep(50_000);
            }
            for ($index = 1; $index <= ($pipeline ? 5 : 3); ++$index) {
                $handles[$index] = $control->startWorkflow('qualification.completion', $prefix.'-'.$index, $queue,
                    input: [$index], executionTimeoutSeconds: 180, runTimeoutSeconds: 150);
            }
            foreach ($handles as $index => $handle) {
                $remaining = (int) floor($deadline - microtime(true));
                if ($remaining < 1 || $handle->result(timeoutSeconds: $remaining, pollIntervalSeconds: 0.1) !== $index) {
                    throw new RuntimeException('completion_workflow_result');
                }
            }
        } catch (Throwable $error) {
            $failure = $error;
        } finally {
            // The child owns this new process group, including its SDK maintenance helpers.
            posix_kill(-$pid, SIGTERM);
            $until = microtime(true) + 15;
            while (pcntl_waitpid($pid, $status, WNOHANG) === 0) {
                if (microtime(true) >= $until) {
                    posix_kill(-$pid, SIGKILL);
                    pcntl_waitpid($pid, $status);
                    $failure ??= new RuntimeException('completion_worker_cleanup_deadline');
                    break;
                }
                usleep(50_000);
            }
            if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                $failure ??= new RuntimeException('completion_worker_failed');
            }
            if (posix_kill(-$pid, 0)) {
                posix_kill(-$pid, SIGKILL);
                $failure ??= new RuntimeException('completion_worker_helper_survived');
            }
            if ($failure !== null) {
                foreach ($handles as $handle) {
                    try {
                        $description = $handle->describe();
                        if (!in_array($description->status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                            $handle->terminate('Completion qualification cleanup');
                        }
                    } catch (Throwable) {
                        $trace->record(['kind' => 'cleanup_error']);
                    }
                }
            }
        }
        if ($failure !== null) {
            throw $failure;
        }
        if (($control->listWorkers($queue)['workers'] ?? []) !== []) {
            throw new RuntimeException('completion_worker_registration_survived');
        }
        $events = $trace->state()['events'];
        return ['scenario' => $data['scenario'], 'assertions' => CompletionPollEvidence::verify($events, $data['scenario'], $workerId, $prefix),
            'workflow_count' => count($handles), 'worker_deregistered' => true, 'events' => $events];
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
    $clientToken = getenv('COMPLETION_TEST_CLIENT_TOKEN');
    $workerToken = getenv('COMPLETION_TEST_WORKER_TOKEN');
    if (!is_string($clientToken) || $clientToken === '' || !is_string($workerToken) || $workerToken === '' || $clientToken === $workerToken) {
        throw new RuntimeException('completion_scoped_test_tokens');
    }
    $result = completionScenario($subject, $clientToken, $workerToken);
    echo json_encode(['schema' => 'durable-workflow.php-completion-polls-result/v1', 'outcome' => 'pass',
        'scope' => 'installed SDK on caller-owned loopback runtime; image publication/digest and execution admission are caller evidence',
        'identity' => $identity, 'server_version' => $subject->data['server_version'],
        'caller_server_image' => $subject->data['server_image'], 'result' => $result], JSON_THROW_ON_ERROR).PHP_EOL;
} catch (Throwable $error) {
    $message = $error->getMessage();
    $code = preg_match('/\Acompletion_[a-z_]+\z/', $message) === 1 ? $message : 'completion_consumer_failed';
    fwrite(STDERR, json_encode(['outcome' => 'fail', 'code' => $code, 'class' => $error::class], JSON_THROW_ON_ERROR).PHP_EOL);
    exit(1);
}
