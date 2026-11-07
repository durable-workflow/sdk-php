<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\CompletionPipelineScenario;
use Throwable;
use RuntimeException;

require_once __DIR__.'/CompletionPollSubject.php';
require_once __DIR__.'/CompletionPollTrace.php';
require_once __DIR__.'/CompletionPollFailure.php';
require_once dirname(__DIR__, 3).'/tests/Support/CompletionPipelineScenario.php';

/** Public installed Client calls only. No administrative clock, SQL or producer implementation. */
final class CompletionPollOppositeSlots
{
    /** @return array<string, mixed> */
    public static function run(CompletionPollSubject $subject, string $clientToken, string $workerToken): array
    {
        return CompletionPipelineScenario::run(static function (string $relay, string $file) use ($subject, $clientToken, $workerToken): array {
            $data = $subject->data;
            $prefix = 'opposite-'.bin2hex(random_bytes(8));
            $worker = $prefix.'-worker';
            $queue = $prefix.'-queue';
            file_put_contents($file, json_encode(['events' => [], 'endpoint' => $data['endpoint'],
                'scenario' => 'opposite-slots', 'prefix' => $prefix], JSON_THROW_ON_ERROR));
            $trace = new CompletionPollTrace($file);
            $control = new Client($relay, namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 2);
            $actor = new Client($relay, namespace: $data['namespace'], workerToken: $workerToken, workerRequestTimeoutSeconds: 2);
            $cleanup = new Client($data['endpoint'], namespace: $data['namespace'], controlToken: $clientToken, requestTimeoutSeconds: 1);
            $deadline = hrtime(true) / 1e9 + $data['deadline_seconds'];
            $check = static function () use ($deadline): void {
                if (hrtime(true) / 1e9 >= $deadline) throw new RuntimeException('completion_opposite_deadline');
            };
            $token = null;
            $failure = null;
            $assertions = [];
            $terminated = 0;
            $handles = [];
            for ($i = 1; $i <= 3; ++$i) $handles[$i] = $control->workflowHandle($prefix.'-'.$i);
            try {
                if ($control->clusterInfo()->version !== $data['server_version']) throw new RuntimeException('completion_runtime_version');
                $registered = $actor->registerWorker($worker, $queue, ['qualification.wire'], ['qualification.remote'],
                    maxConcurrentWorkflowTasks: 2, maxConcurrentActivityTasks: 2);
                $token = $registered['registration_token'] ?? null;
                if (($registered['registered'] ?? null) !== true || ($registered['worker_id'] ?? null) !== $worker
                    || ($registered['task_queue'] ?? null) !== $queue) throw new RuntimeException('completion_opposite_registration_required');
                foreach (CompletionPollEvidence::CAPABILITIES as $capability) {
                    if (($registered['server_capabilities'][$capability] ?? null) !== true) throw new RuntimeException('completion_opposite_capability_required');
                }
                if (!is_string($token) || preg_match('/\A[a-f0-9]{32}\z/', $token) !== 1) throw new RuntimeException('completion_refence_receipt_required');
                foreach ($handles as $index => $handle) {
                    $check();
                    $handles[$index] = $control->startWorkflow('qualification.wire', $handle->workflowId, $queue,
                        input: [$index], executionTimeoutSeconds: 180, runTimeoutSeconds: 150);
                }
                $workflowOne = self::claim($actor->pollWorkflowTaskResponse($worker, $queue, 0, $prefix.'-wf1'), $worker, 'workflow');
                $workflowTwo = self::claim($actor->pollWorkflowTaskResponse($worker, $queue, 0, $prefix.'-wf2'), $worker, 'workflow');
                if ($workflowOne['task_id'] === $workflowTwo['task_id']) throw CompletionPollFailure::product('completion_opposite_distinct_leases');
                $check();
                $activityId = $prefix.'-carried-activity';
                $answer = $actor->completeWorkflowTask($workflowOne['task_id'], $worker, $workflowOne['workflow_task_attempt'],
                    self::schedule($actor), activityPoll: self::carrier($worker, $queue, $activityId));
                self::omitted($answer, 'activity_poll');
                // The other original workflow is still leased. A same-ID
                // ordinary activity poll proves eligible work was present.
                $activityOne = self::claim($actor->pollActivityTaskResponse($worker, $queue, 0, $activityId), $worker, 'activity');
                $actor->completeWorkflowTask($workflowTwo['task_id'], $worker, $workflowTwo['workflow_task_attempt'], self::schedule($actor));
                $activityTwo = self::claim($actor->pollActivityTaskResponse($worker, $queue, 0, $prefix.'-act2'), $worker, 'activity');
                if ($activityOne['task_id'] === $activityTwo['task_id']) throw CompletionPollFailure::product('completion_opposite_distinct_leases');
                $check();
                $workflowId = $prefix.'-carried-workflow';
                $workflowCarrier = self::carrier($worker, $queue, $workflowId) + ['history_page_size' => 100, 'task_kinds' => ['workflow']];
                $answer = $actor->completeActivityTask($activityTwo['task_id'], $activityTwo['activity_attempt_id'], $worker, 2,
                    workflowPoll: $workflowCarrier);
                self::omitted($answer, 'workflow_poll');
                // There is both a third original workflow and the activity's
                // next workflow task ready. Release the held activity normally,
                // then recover with the omitted carrier's exact original ID.
                $actor->completeActivityTask($activityOne['task_id'], $activityOne['activity_attempt_id'], $worker, 1);
                $recovered = self::claim($actor->pollWorkflowTaskResponse($worker, $queue, 0, $workflowId), $worker, 'workflow');
                $assertions = ['scope' => 'installed Client public-API opposite-slot control; managed Worker capacity is a separate row',
                    'live_workflow_declined_activity_carrier' => true, 'live_activity_declined_workflow_carrier' => true,
                    'eligible_work_and_original_id_followup' => true, 'recovered_workflow_task' => CompletionPollTrace::task($recovered)];
            } catch (Throwable $error) {
                $failure = $error;
            } finally {
                $cleanupDeadline = hrtime(true) / 1e9 + 25;
                foreach ($handles as $handle) {
                    try {
                        if (hrtime(true) / 1e9 >= $cleanupDeadline) throw new RuntimeException('completion_opposite_cleanup_deadline');
                        $description = $cleanup->describeWorkflow($handle->workflowId, $handle->selectedRunId);
                        if (!in_array($description->status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                            $cleanup->terminateWorkflow($handle->workflowId, 'Opposite-slot qualification cleanup', $handle->selectedRunId);
                            ++$terminated;
                            $closed = $cleanup->describeWorkflow($handle->workflowId, $handle->selectedRunId);
                            if (!in_array($closed->status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                                throw new RuntimeException('completion_opposite_cleanup_unconfirmed');
                            }
                        }
                    } catch (Throwable $error) {
                        if ($error instanceof ServerException && $error->status === 404 && $handle->selectedRunId === null) continue;
                        $trace->record(['kind' => 'cleanup_error']);
                        $failure = new RuntimeException('completion_opposite_cleanup_failed', previous: $error);
                    }
                }
                if (is_string($token)) {
                    try {
                        $actor->deregisterWorkerRegistration($worker, $token);
                    } catch (Throwable $error) {
                        $failure = new RuntimeException('completion_opposite_cleanup_failed', previous: $error);
                    }
                }
            }
            if ($failure === null && ($cleanup->listWorkers($queue)['workers'] ?? []) !== []) {
                $failure = new RuntimeException('completion_worker_registration_survived');
            }
            return ['scenario' => 'opposite-slots', 'outcome' => $failure === null ? 'pass' : CompletionPollFailure::diagnostic($failure)['outcome'],
                'diagnostic' => $failure === null ? null : CompletionPollFailure::diagnostic($failure),
                'assertions' => $assertions, 'workflows_intentionally_terminated' => $terminated, 'events' => $trace->state()['events']];
        }, __DIR__.'/completion-poll-relay.php', true);
    }

    /** @return array<string, mixed> */
    private static function carrier(string $worker, string $queue, string $id): array
    {
        return ['worker_id' => $worker, 'task_queue' => $queue, 'poll_request_id' => $id, 'timeout_seconds' => 0];
    }

    /** @return list<array<string, mixed>> */
    private static function schedule(Client $client): array
    {
        return [['type' => 'schedule_activity', 'activity_type' => 'qualification.remote',
            'arguments' => $client->payloadCodec()->envelope([1]), 'start_to_close_timeout' => 120]];
    }

    /** @param array<string, mixed> $answer
     * @return array<string, mixed>
     */
    private static function claim(array $answer, string $worker, string $kind): array
    {
        $task = $answer['task'] ?? null;
        if (($answer['poll_status'] ?? null) !== 'leased' || !is_array($task)
            || ($task['lease_owner'] ?? null) !== $worker || ($task['task_kind'] ?? null) !== $kind
            || !is_string($task['task_id'] ?? null) || !is_string($task['lease_expires_at'] ?? null)
            || ($kind === 'workflow' && (!is_int($task['workflow_task_attempt'] ?? null) || $task['workflow_task_attempt'] < 1))
            || ($kind === 'activity' && (!is_string($task['activity_attempt_id'] ?? null) || $task['activity_attempt_id'] === ''))) {
            throw CompletionPollFailure::product('completion_opposite_claim_required');
        }
        return $task;
    }

    /** @param array<string, mixed> $answer */
    public static function omitted(array $answer, string $member): void
    {
        if (($answer['recorded'] ?? null) !== true || array_key_exists($member, $answer)) {
            throw CompletionPollFailure::product('completion_opposite_carrier_not_declined');
        }
    }
}
