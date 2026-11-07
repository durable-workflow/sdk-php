<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use RuntimeException;

require_once __DIR__.'/CompletionPollFailure.php';
require_once __DIR__.'/CompletionPollFaults.php';

/** Server #1067's frozen registration-release vector, through the installed public Client. */
final class CompletionPollWorkflowRefence
{
    /** @param callable(): void $check
     * @return array<string, mixed>
     */
    public static function run(Client $actor, string $worker, string $queue, string $prefix, callable $check, string &$token): array
    {
        $first = $actor->pollWorkflowTaskResponse($worker, $queue, 0, $prefix.'-parent')['task'] ?? null;
        if (!is_array($first)) throw new RuntimeException('completion_refence_initial_claim');
        $scheduled = $actor->completeWorkflowTask($first['task_id'], $worker, $first['workflow_task_attempt'],
            [['type' => 'schedule_activity', 'activity_type' => 'qualification.remote',
                'arguments' => $actor->payloadCodec()->envelope([1]), 'start_to_close_timeout' => 120]]);
        if (($scheduled['recorded'] ?? null) !== true) throw CompletionPollFailure::product('completion_refence_initial_completion');
        $activity = $actor->pollActivityTaskResponse($worker, $queue, 0, $prefix.'-activity')['task'] ?? null;
        if (!is_array($activity)) throw new RuntimeException('completion_refence_initial_claim');
        $pollId = $prefix.'-original-workflow';
        $carriedStarted = hrtime(true);
        $answer = $actor->completeActivityTask($activity['task_id'], $activity['activity_attempt_id'], $worker, 1,
            workflowPoll: ['worker_id' => $worker, 'task_queue' => $queue, 'poll_request_id' => $pollId,
                'timeout_seconds' => 0, 'history_page_size' => 100, 'task_kinds' => ['workflow']]);
        $original = $answer['workflow_poll']['answer']['task'] ?? null;
        if (($answer['recorded'] ?? null) !== true || ($answer['workflow_poll']['leased'] ?? null) !== true
            || !is_array($original) || ($original['lease_owner'] ?? null) !== $worker
            || !is_int($original['workflow_task_attempt'] ?? null) || !is_array($original['history_events'] ?? null)) {
            throw CompletionPollFailure::product('completion_refence_carried_workflow_missing');
        }
        $check();
        $oldDigest = hash('sha256', $token);
        $replacement = $actor->registerWorker($worker, $queue, ['qualification.wire'], ['qualification.remote'],
            maxConcurrentWorkflowTasks: 2, maxConcurrentActivityTasks: 2);
        $replacementToken = $replacement['registration_token'] ?? null;
        if (($replacement['registered'] ?? null) !== true || ($replacement['worker_id'] ?? null) !== $worker
            || ($replacement['task_queue'] ?? null) !== $queue || !is_string($replacementToken)
            || preg_match('/\A[a-f0-9]{32}\z/', $replacementToken) !== 1) throw new RuntimeException('completion_refence_receipt_required');
        // Transfer cleanup authority immediately after a verified response.
        $token = $replacementToken;
        if (hash_equals($oldDigest, hash('sha256', $token))) throw CompletionPollFailure::product('completion_refence_token_not_rotated');
        // Charge the whole original completion and registration transit. A
        // slow cell cannot use expiry as evidence of registration revocation.
        if ((hrtime(true) - $carriedStarted) / 1e9 >= CompletionPollFaults::window($answer['workflow_poll']['answer']) - 5) {
            throw new RuntimeException('completion_refence_live_window_required');
        }
        $commands = [['type' => 'complete_workflow', 'result' => $actor->payloadCodec()->envelope(1)]];
        $heartbeat = static fn (): array => $actor->heartbeatWorkflowTask($original['task_id'], $worker, $original['workflow_task_attempt']);
        $completion = static fn (): array => $actor->completeWorkflowTask($original['task_id'], $worker, $original['workflow_task_attempt'], $commands);
        foreach ([$heartbeat, $completion] as $index => $operation) {
            $check();
            self::refused($operation, $original, 'task_not_leased', null, $index === 1);
        }
        $check();
        $recovered = $actor->pollWorkflowTaskResponse($worker, $queue, 0, $pollId, historyPageSize: 100)['task'] ?? null;
        if (!is_array($recovered)) throw CompletionPollFailure::product('completion_refence_recovery_missing');
        self::recovered($original, $recovered);
        foreach ([$heartbeat, $completion] as $index => $operation) {
            $check();
            self::refused($operation, $original, 'workflow_task_attempt_mismatch', $recovered['workflow_task_attempt'], $index === 1);
        }
        $check();
        $confirmed = $actor->heartbeatWorkflowTask($recovered['task_id'], $worker, $recovered['workflow_task_attempt']);
        if (($confirmed['renewed'] ?? null) !== true) throw CompletionPollFailure::product('completion_refence_new_authority_lost');
        $completed = $actor->completeWorkflowTask($recovered['task_id'], $worker, $recovered['workflow_task_attempt'], $commands);
        if (($completed['recorded'] ?? null) !== true) throw CompletionPollFailure::product('completion_refence_new_authority_lost');
        return ['registration_rotated' => true, 'pre_recovery_reason' => 'task_not_leased',
            'post_recovery_reason' => 'workflow_task_attempt_mismatch', 'original_id_fresh_claim' => true,
            'original_attempt' => $original['workflow_task_attempt'], 'current_attempt' => $recovered['workflow_task_attempt'],
            'history_preserved' => true, 'current_authority_completed' => true,
            'old_registration_sha256' => $oldDigest, 'registration_sha256' => hash('sha256', $token)];
    }

    /** @param array<string, mixed> $original
     * @param array<string, mixed> $current
     */
    public static function recovered(array $original, array $current): void
    {
        foreach (['task_id', 'workflow_id', 'run_id', 'lease_owner', 'workflow_type', 'input', 'history_events'] as $key) {
            if (($original[$key] ?? null) !== ($current[$key] ?? null)) throw CompletionPollFailure::product('completion_refence_recovery_changed');
        }
        if (!is_array($original['history_events'] ?? null) || !is_int($original['workflow_task_attempt'] ?? null)
            || ($current['workflow_task_attempt'] ?? null) !== $original['workflow_task_attempt'] + 1
            || !is_string($current['lease_expires_at'] ?? null)) throw CompletionPollFailure::product('completion_refence_recovery_changed');
    }

    /** @param callable(): array<string, mixed> $operation
     * @param array<string, mixed> $original
     */
    private static function refused(callable $operation, array $original, string $reason, ?int $attempt, bool $completion): void
    {
        try {
            $operation();
        } catch (ServerException $error) {
            self::refusal($error, $original, $reason, $attempt, $completion);
            return;
        }
        throw CompletionPollFailure::product('completion_refence_old_authority_accepted');
    }

    /** @param array<string, mixed> $original */
    public static function refusal(ServerException $error, array $original, string $reason, ?int $attempt, bool $completion): void
    {
        $details = $error->details ?? [];
        if ($error->status !== 409 || $error->reason !== $reason || ($details['task_id'] ?? null) !== $original['task_id']
            || ($details['workflow_task_attempt'] ?? null) !== $original['workflow_task_attempt']
            || ($attempt === null ? ($details['task_status'] ?? null) !== 'ready' : ($details['current_attempt'] ?? null) !== $attempt)
            || ($completion && $attempt === null && ($details['recorded'] ?? null) !== false)) {
            throw CompletionPollFailure::product('completion_refence_wrong_refusal');
        }
    }
}
