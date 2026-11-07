<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

require_once __DIR__.'/CompletionPollFailure.php';
require_once __DIR__.'/CompletionPollMatrixEvidence.php';

/** Assertions over actual relay observations, not a producer or source-test acceptance substitute. */
final class CompletionPollEvidence
{
    public const CAPABILITIES = ['workflow_task_completion_activity_poll', 'activity_completion_workflow_poll',
        'workflow_task_completion_workflow_poll'];

    /** @param list<array<string, mixed>> $events
     * @return array<string, mixed>
     */
    public static function verify(array $events, string $scenario, string $worker, string $prefix): array
    {
        if (in_array($scenario, CompletionPollMatrixEvidence::SCENARIOS, true)) {
            return CompletionPollMatrixEvidence::verify($events, $scenario, $worker, $prefix);
        }
        if ($events === [] || !in_array($scenario, CompletionPollSubject::SCENARIOS, true)) {
            throw new RuntimeException('completion_evidence_empty');
        }
        $requests = $answers = $delivered = $deliveryStatus = $ordinary = $callbacks = $current = $callbackLeases = [];
        $advertised = array_fill_keys(self::CAPABILITIES, false);
        foreach ($events as $position => $event) {
            if (in_array($event['kind'], ['worker_error', 'cleanup_error', 'relay_error'], true)) {
                throw new RuntimeException('completion_evidence_process_failure');
            }
            switch ($event['kind']) {
                case 'request':
                    $requests[$event['request']] = $event + ['position' => $position];
                    break;
                case 'upstream':
                    $answers[$event['request']] = $event + ['position' => $position];
                    foreach ($advertised as $key => $value) {
                        $advertised[$key] = $value || ($event['answer']['capabilities'][$key] ?? null) === true;
                    }
                    $request = $requests[$event['request']] ?? throw new RuntimeException('completion_evidence_unbound_answer');
                    if (str_ends_with($request['path'], '/poll') && isset($event['answer']['task']['task_id'])) {
                        $kind = str_contains($request['path'], '/activity-tasks/') ? 'activity' : 'workflow';
                        $ordinary[self::key($event['answer']['task'], $kind)] = $position;
                    }
                    break;
                case 'delivered':
                    $delivered[$event['request']] = $position;
                    $deliveryStatus[$event['request']] = $event['status'];
                    $answer = $answers[$event['request']]['answer'] ?? [];
                    $request = $requests[$event['request']] ?? throw new RuntimeException('completion_evidence_unbound_delivery');
                    $kind = str_contains($request['path'], '/activity-tasks/') ? 'activity' : 'workflow';
                    foreach ([[$kind, $answer['task'] ?? null], ['activity', $answer['activity_poll']['task'] ?? null],
                        ['workflow', $answer['workflow_poll']['task'] ?? null]] as [$taskKind, $task]) {
                        if (is_array($task)) {
                            self::lease($task, $worker, $taskKind);
                            $current[$taskKind === 'workflow' ? $task['workflow_id'] : $task['task_id']] = self::key($task, $taskKind);
                        }
                    }
                    break;
                case 'callback':
                    $callbacks[$event['workflow_id']][] = $position;
                    if (isset($current[$event['workflow_id']])) {
                        $callbackLeases[$current[$event['workflow_id']]][] = $position;
                    }
                    break;
                case 'activity':
                    if (isset($current[$event['task_id']])) {
                        $callbackLeases[$current[$event['task_id']]][] = $position;
                    }
                    break;
            }
        }
        if ($scenario === 'fallback') {
            if (in_array(true, $advertised, true)) {
                throw CompletionPollFailure::product('completion_evidence_fallback_capability');
            }
            if ($ordinary === [] || $callbacks === []) {
                throw CompletionPollFailure::product('completion_evidence_fallback_no_work');
            }
            foreach ($requests as $request) {
                if (isset($request['body']['workflow_poll']) || isset($request['body']['activity_poll'])) {
                    throw CompletionPollFailure::product('completion_evidence_fallback_carrier');
                }
            }
            return ['capabilities_absent' => true, 'carrier_requests' => 0];
        }
        if (in_array(false, $advertised, true)) {
            throw CompletionPollFailure::product('completion_evidence_missing_capability');
        }
        if ($scenario === 'carriers') {
            $paths = array_fill_keys(['workflow-activity', 'activity-workflow', 'workflow-workflow'], 0);
            foreach ($answers as $id => $event) {
                $request = $requests[$id];
                foreach (['activity_poll' => 'activity', 'workflow_poll' => 'workflow'] as $key => $kind) {
                    $task = $event['answer'][$key]['task'] ?? null;
                    if (($event['answer'][$key]['leased'] ?? null) !== true || !is_array($task)) {
                        continue;
                    }
                    self::lease($task, $worker, $kind);
                    $parent = str_contains($request['path'], '/activity-tasks/') ? 'activity' : 'workflow';
                    if (($event['status'] ?? null) !== 200 || !isset($request['body'][$key])
                        || ($request['body']['lease_owner'] ?? null) !== $worker
                        || ($request['body'][$key]['worker_id'] ?? null) !== $worker
                        || !is_string($request['body'][$key]['poll_request_id'] ?? null)
                        || trim($request['body'][$key]['poll_request_id']) === ''
                        || isset($ordinary[self::key($task, $kind)]) || !isset($delivered[$id]) || ($deliveryStatus[$id] ?? null) !== 200) {
                        throw CompletionPollFailure::product('completion_evidence_inline_delivery');
                    }
                    $kindCallbacks = $callbackLeases[self::key($task, $kind)] ?? [];
                    if (array_filter($kindCallbacks, static fn (int $position): bool => $position > $delivered[$id]) === []) {
                        throw CompletionPollFailure::product('completion_evidence_missing_callback');
                    }
                    ++$paths[$parent.'-'.$kind];
                }
            }
            if (min($paths) < 1) {
                throw CompletionPollFailure::product('completion_evidence_missing_carrier_path');
            }
            return ['inline_delivery_counts' => $paths];
        }
        return self::pipeline($events, $requests, $answers, $delivered, $deliveryStatus, $callbacks, $scenario, $worker, $prefix);
    }

    /** @param array<string, mixed> $task */
    private static function lease(array $task, string $worker, string $kind): void
    {
        if (!is_string($task['task_id'] ?? null) || $task['task_id'] === '' || ($task['lease_owner'] ?? null) !== $worker
            || (isset($task['task_kind']) && $task['task_kind'] !== $kind)
            || !is_string($task['workflow_id'] ?? null) || !is_string($task['run_id'] ?? null)
            || !is_string($task['lease_expires_at'] ?? null) || $task['lease_expires_at'] === ''
            || ($kind === 'workflow' && (!is_int($task['workflow_task_attempt'] ?? null) || $task['workflow_task_attempt'] < 1))
            || ($kind === 'activity' && !is_string($task['activity_attempt_id'] ?? null))) {
            throw CompletionPollFailure::product('completion_evidence_lease_identity');
        }
    }

    /** @param array<string, mixed> $task */
    private static function key(array $task, string $kind): string
    {
        return json_encode([$kind, $task['task_id'] ?? null, $kind === 'workflow'
            ? ($task['workflow_task_attempt'] ?? null) : ($task['activity_attempt_id'] ?? null),
            $task['lease_owner'] ?? null, $task['workflow_id'] ?? null, $task['run_id'] ?? null], JSON_THROW_ON_ERROR);
    }

    /** @param list<array<string, mixed>> $events
     * @param array<string, array<string, mixed>> $requests
     * @param array<string, array<string, mixed>> $answers
     * @param array<string, int> $delivered
     * @param array<string, int> $deliveryStatus
     * @param array<string, list<int>> $callbacks
     * @return array<string, mixed>
     */
    private static function pipeline(array $events, array $requests, array $answers, array $delivered, array $deliveryStatus,
        array $callbacks, string $scenario, string $worker, string $prefix): array
    {
        $taskIds = [];
        $leases = [];
        $leaseDeliveries = [];
        $prefetch = null;
        foreach ($answers as $id => $event) {
            if (($event['status'] ?? null) !== 200 || ($deliveryStatus[$id] ?? null) !== 200) {
                continue;
            }
            foreach ([$event['answer']['task'] ?? null, $event['answer']['workflow_poll']['task'] ?? null] as $task) {
                if (!is_array($task)) {
                    continue;
                }
                self::lease($task, $worker, 'workflow');
                $taskIds[$task['workflow_id']] ??= $task['task_id'];
                $leases[$task['workflow_id']] ??= $task;
                $leaseDeliveries[$task['workflow_id']] ??= $delivered[$id];
                if ($task['workflow_id'] === $prefix.'-3' && str_ends_with($requests[$id]['path'], '/workflow-tasks/poll')) {
                    $prefetch ??= $requests[$id] + ['answer_position' => $delivered[$id]];
                }
            }
        }
        $parent = array_values(array_filter($requests, static fn (array $request): bool =>
            str_ends_with($request['path'], '/workflow-tasks/'.($taskIds[$prefix.'-2'] ?? '').'/complete')));
        $child = array_values(array_filter($requests, static fn (array $request): bool =>
            str_ends_with($request['path'], '/workflow-tasks/'.($taskIds[$prefix.'-3'] ?? '').'/complete')));
        $useful = array_keys(array_filter($events, static fn (array $event): bool => $event['kind'] === 'local' && $event['index'] === 3));
        if ($prefetch === null || $parent === [] || $child === [] || !isset($callbacks[$prefix.'-3'][0])
            || ($prefetch['body']['timeout_seconds'] ?? null) !== 0
            || ($prefetch['body']['worker_id'] ?? null) !== $worker
            || !is_string($prefetch['body']['poll_request_id'] ?? null) || trim($prefetch['body']['poll_request_id']) === ''
            || ($answers[$parent[0]['request']]['status'] ?? null) !== 200
            || ($deliveryStatus[$parent[0]['request']] ?? null) !== ($scenario === 'pipeline-loss' ? 502 : 200)
            || ($leaseDeliveries[$prefix.'-2'] ?? PHP_INT_MAX) >= $prefetch['position']
            || $prefetch['answer_position'] >= $parent[0]['position']
            || $parent[0]['position'] >= $callbacks[$prefix.'-3'][0]
            || $callbacks[$prefix.'-3'][0] >= ($delivered[$parent[0]['request']] ?? -1)
            || ($delivered[end($parent)['request']] ?? PHP_INT_MAX) >= $child[0]['position']
            || isset($parent[0]['body']['workflow_poll'])) {
            throw CompletionPollFailure::product('completion_evidence_pipeline_order');
        }
        if (array_filter($useful, static fn (int $position): bool => $position > $parent[0]['position']
            && $position < $delivered[$parent[0]['request']]) === []) {
            throw CompletionPollFailure::product('completion_evidence_no_overlapped_work');
        }
        foreach ([[$parent[0], $prefix.'-2'], [$child[0], $prefix.'-3']] as [$request, $workflow]) {
            if (($request['body']['lease_owner'] ?? null) !== $worker
                || ($request['body']['workflow_task_attempt'] ?? null) !== ($leases[$workflow]['workflow_task_attempt'] ?? null)) {
                throw CompletionPollFailure::product('completion_evidence_publication_fence');
            }
        }
        foreach ($requests as $request) {
            if ($request['position'] < $prefetch['position']) {
                foreach ([$request['body']['poll_request_id'] ?? null, $request['body']['workflow_poll']['poll_request_id'] ?? null] as $prior) {
                    if ($prior === $prefetch['body']['poll_request_id']) {
                        throw CompletionPollFailure::product('completion_evidence_prefetch_identity_reused');
                    }
                }
            }
            if ($request['position'] > $parent[0]['position'] && $request['position'] < $delivered[$parent[0]['request']]
                && (str_ends_with($request['path'], '/workflow-tasks/poll') || isset($request['body']['workflow_poll']))) {
                throw CompletionPollFailure::product('completion_evidence_extra_workflow_reservation');
            }
        }
        if ($scenario === 'pipeline-loss') {
            if (count($parent) !== 2 || $parent[0]['body'] !== $parent[1]['body'] || count($callbacks[$prefix.'-2'] ?? []) !== 1
                || ($answers[$parent[1]['request']]['status'] ?? null) !== 200
                || ($deliveryStatus[$parent[1]['request']] ?? null) !== 200) {
                throw CompletionPollFailure::product('completion_evidence_lost_answer_retry');
            }
            $locals = array_filter($events, static fn (array $event): bool => $event['kind'] === 'local' && $event['index'] === 2);
            if (count($locals) !== 5) {
                throw CompletionPollFailure::product('completion_evidence_local_reexecution');
            }
        }
        return ['controlled_answer_overlap' => true, 'joined_before_next_completion' => true,
            'extra_workflow_reservation_while_pending' => false, 'parent_attempts' => count($parent),
            'performance_scope' => 'controlled reply-delay scenario; not a load or speedup measurement'];
    }
}
