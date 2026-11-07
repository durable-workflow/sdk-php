<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;
use Throwable;

require_once __DIR__.'/CompletionPollLoadEvidence.php';

/** One explicitly selected cell on an already-running caller-owned disposable runtime. */
final class CompletionPollLoad
{
    /** @return array<string, mixed> */
    public static function run(CompletionPollSubject $subject, string $clientToken, string $workerToken): array
    {
        $data = $subject->data;
        CompletionPollLoadProfile::validate($data);
        $workload = CompletionPollLoadProfile::workload($data['scenario']);
        $enabled = str_ends_with($data['scenario'], '-on');
        $prefix = 'load-'.bin2hex(random_bytes(8));
        $queue = $prefix.'-queue';
        $directory = sys_get_temp_dir().'/'.$prefix;
        $control = new Client($data['endpoint'], namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 2);
        $cleanup = new Client($data['endpoint'], namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 1);
        if (!mkdir($directory, 0700)) throw new RuntimeException('completion_load_directory');
        $deadline = hrtime(true) / 1e9 + $data['deadline_seconds'];
        $children = $handles = $reports = [];
        $failure = null;
        $finished = 0;
        $started = $elapsed = null;
        $workerCleanup = false;
        $workflowsCleanup = false;
        $filesCleanup = false;
        $counts = [];
        try {
            $info = $control->clusterInfo();
            if ($info->version !== $data['server_version']) throw new RuntimeException('completion_runtime_version');
            foreach (CompletionPollEvidence::CAPABILITIES as $capability) {
                if (($info->raw['worker_protocol']['server_capabilities'][$capability] ?? null) !== true) {
                    throw new RuntimeException('completion_load_capability_required');
                }
            }
            for ($number = 0; $number < CompletionPollLoadProfile::WORKERS; ++$number) {
                $pid = pcntl_fork();
                if ($pid === -1) throw new RuntimeException('completion_load_worker_start');
                if ($pid === 0) self::worker($subject, $workerToken, $queue, $prefix.'-'.$number, $directory.'/'.$number.'.json', $enabled);
                $children[] = $pid;
            }
            while (count($control->listWorkers($queue)['workers'] ?? []) !== CompletionPollLoadProfile::WORKERS) {
                self::check($deadline, $directory);
                usleep(20_000);
            }
            // Predeclare every owned ID before any start can have an unknown result.
            for ($index = 1; $index <= CompletionPollLoadProfile::WORKFLOWS; ++$index) {
                $handles[$index] = $control->workflowHandle($prefix.'-workflow-'.$index);
            }
            $started = hrtime(true);
            foreach ($handles as $index => $handle) {
                self::check($deadline, $directory);
                $handles[$index] = $control->startWorkflow('qualification.load', $handle->workflowId, $queue,
                    input: [$index], executionTimeoutSeconds: 180, runTimeoutSeconds: 150);
            }
            $pending = $handles;
            while ($pending !== []) {
                foreach ($pending as $index => $handle) {
                    self::check($deadline, $directory);
                    $status = $handle->describeSelectedRun()->status;
                    if ($status === 'completed') {
                        if ($handle->resultOfSelectedRun(timeoutSeconds: 1, pollIntervalSeconds: 0.1) !== $index) {
                            throw CompletionPollFailure::product('completion_load_workflow_result');
                        }
                        ++$finished;
                        unset($pending[$index]);
                    } elseif (in_array($status, ['failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                        throw CompletionPollFailure::product('completion_load_workflow_result');
                    }
                }
                if ($pending !== []) usleep(20_000);
            }
            $elapsed = (hrtime(true) - $started) / 1e9;
            $workflowsCleanup = true;
        } catch (Throwable $error) {
            $failure = $error;
            if ($started !== null) $elapsed = (hrtime(true) - $started) / 1e9;
        } finally {
            $cleanupDeadline = hrtime(true) / 1e9 + 30;
            try {
                self::stop($children, 15);
                $workers = $cleanup->listWorkers($queue)['workers'] ?? null;
                if (!is_array($workers) || $workers !== []) throw new RuntimeException('completion_worker_registration_survived');
                $workerCleanup = true;
                foreach (array_keys($children) as $number) {
                    $path = $directory.'/'.$number.'.json';
                    // No reports are consumed while workers can still write them.
                    $bytes = @file_get_contents($path, length: 131_073);
                    if ($bytes === false || strlen($bytes) > 131_072) throw new RuntimeException('completion_load_reports_missing');
                    $report = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
                    if (!is_array($report)) throw new RuntimeException('completion_load_report_shape');
                    $reports[] = $report;
                }
            } catch (Throwable $error) {
                $failure = new RuntimeException('completion_load_worker_cleanup_failed', previous: $error);
            }
            if ($failure === null) {
                try {
                    $counts = CompletionPollLoadEvidence::counts($reports);
                } catch (Throwable $error) {
                    $failure = $error;
                }
            }
            if (!$workflowsCleanup) {
                $workflowsCleanup = true;
                foreach ($handles as $handle) {
                    if (hrtime(true) / 1e9 >= $cleanupDeadline - 3) {
                        $workflowsCleanup = false;
                        break;
                    }
                    try {
                        $description = $cleanup->describeWorkflow($handle->workflowId, $handle->selectedRunId);
                        if (!in_array($description->status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                            $cleanup->terminateWorkflow($handle->workflowId, 'Load qualification cleanup', $handle->selectedRunId);
                            if (!in_array($cleanup->describeWorkflow($handle->workflowId, $handle->selectedRunId)->status,
                                ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) $workflowsCleanup = false;
                        }
                    } catch (Throwable $error) {
                        if ($error instanceof ServerException && $error->status === 404 && $handle->selectedRunId === null) continue;
                        $workflowsCleanup = false;
                    }
                }
            }
            for ($number = 0; $number < CompletionPollLoadProfile::WORKERS; ++$number) {
                $path = $directory.'/'.$number.'.json';
                if (is_file($path)) unlink($path);
            }
            $filesCleanup = rmdir($directory);
            if (!$workerCleanup || !$workflowsCleanup || !$filesCleanup) $failure = new RuntimeException('completion_load_cleanup_incomplete', previous: $failure);
        }
        return ['scenario' => $data['scenario'], 'outcome' => $failure === null ? 'pass' : CompletionPollFailure::diagnostic($failure)['outcome'],
            'diagnostic' => $failure === null ? null : CompletionPollFailure::diagnostic($failure), 'load' => $data['load'],
            'workload' => $workload, 'enable_completion_pipelining' => $enabled,
            'measurement' => 'first submission through last verified result; workers registered before measurement',
            'elapsed_seconds' => $elapsed, 'workflow_results_verified' => $finished, 'counts' => $counts,
            'worker_reports' => $reports, 'relay_used' => false, 'request_trace_used' => false,
            'cleanup' => ['complete' => $workerCleanup && $workflowsCleanup && $filesCleanup,
                'worker_groups_and_registrations' => $workerCleanup, 'owned_workflows' => $workflowsCleanup, 'temporary_reports' => $filesCleanup],
            'caller_disposable_runtime_cleanup_required' => true, 'performance_acceptance' => 'not_assessed'];
    }

    private static function worker(CompletionPollSubject $subject, string $token, string $queue, string $id, string $reportFile, bool $enabled): never
    {
        $steps = [];
        $callbacks = 0;
        $failure = null;
        try {
            if (posix_setsid() === -1) throw new RuntimeException('completion_worker_process_group');
            $client = new Client($subject->data['endpoint'], namespace: $subject->data['namespace'], workerToken: $token,
                requestTimeoutSeconds: 2, workerRequestTimeoutSeconds: 5);
            $worker = new Worker($client, $queue, workerId: $id, enableCompletionPipelining: $enabled);
            $local = str_contains($subject->data['scenario'], '-local-');
            $worker->registerWorkflow('qualification.load', static function (WorkflowContext $context, mixed ...$arguments) use ($local, &$callbacks): int {
                if (count($arguments) !== 1 || !is_int($arguments[0]) || $arguments[0] < 1 || $arguments[0] > CompletionPollLoadProfile::WORKFLOWS) {
                    throw new RuntimeException('completion_load_arguments');
                }
                ++$callbacks;
                $index = $arguments[0];
                for ($step = 0; $step < CompletionPollLoadProfile::STEPS; ++$step) {
                    $answer = $local ? $context->localActivity('qualification.step', [$index, $step])
                        : $context->activity('qualification.step', [$index, $step]);
                    if ($answer !== $index) throw new RuntimeException('completion_load_step_result');
                }
                return $index;
            });
            $worker->registerActivity('qualification.step', static function (ActivityContext $context, mixed ...$arguments) use (&$steps): int {
                if (count($arguments) !== 2 || !is_int($arguments[0]) || !is_int($arguments[1])
                    || $arguments[0] < 1 || $arguments[0] > CompletionPollLoadProfile::WORKFLOWS
                    || $arguments[1] < 0 || $arguments[1] >= CompletionPollLoadProfile::STEPS) throw new RuntimeException('completion_load_arguments');
                $key = $arguments[0].':'.$arguments[1];
                $steps[$key] = ($steps[$key] ?? 0) + 1;
                usleep(CompletionPollLoadProfile::STEP_MICROSECONDS);
                return $arguments[0];
            });
            $worker->run(1);
        } catch (Throwable $error) {
            $failure = CompletionPollFailure::diagnostic($error);
        }
        $bytes = json_encode(['worker_id' => $id, 'outcome' => $failure === null ? 'pass' : 'runner-blocked',
            'diagnostic' => $failure, 'workflow_callbacks' => $callbacks, 'steps' => $steps], JSON_THROW_ON_ERROR);
        $written = file_put_contents($reportFile, $bytes) === strlen($bytes);
        exit($failure === null && $written ? 0 : 1);
    }

    private static function check(float $deadline, string $directory): void
    {
        if (hrtime(true) / 1e9 >= $deadline) throw new RuntimeException('completion_load_deadline');
        for ($number = 0; $number < CompletionPollLoadProfile::WORKERS; ++$number) {
            if (is_file($directory.'/'.$number.'.json')) throw new RuntimeException('completion_load_worker_stopped_early');
        }
    }

    /** Signal all owned groups together; the grace is global, not multiplied by worker count.
     * @param list<int> $pids
     */
    public static function stop(array $pids, float $grace): void
    {
        $pending = array_fill_keys($pids, true);
        foreach ($pids as $pid) {
            if (!posix_kill(-$pid, SIGTERM)) posix_kill($pid, SIGTERM);
        }
        $deadline = hrtime(true) / 1e9 + $grace;
        $failed = false;
        while ($pending !== []) {
            foreach ($pending as $pid => $_) {
                $waited = pcntl_waitpid($pid, $status, WNOHANG);
                if ($waited !== 0) {
                    $failed = $failed || $waited === -1 || !pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0;
                    unset($pending[$pid]);
                }
            }
            if ($pending === []) break;
            if (hrtime(true) / 1e9 >= $deadline) {
                foreach ($pending as $pid => $_) {
                    posix_kill(-$pid, SIGKILL);
                    posix_kill($pid, SIGKILL);
                    pcntl_waitpid($pid, $status);
                }
                $failed = true;
                break;
            }
            usleep(10_000);
        }
        foreach ($pids as $pid) {
            if (posix_kill(-$pid, 0)) {
                posix_kill(-$pid, SIGKILL);
                $failed = true;
            }
        }
        if ($failed) throw new RuntimeException('completion_load_worker_stop_failed');
    }
}
