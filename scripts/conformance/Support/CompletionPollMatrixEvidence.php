<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

require_once __DIR__.'/CompletionPollFailure.php';
require_once __DIR__.'/CompletionPollFaults.php';

/** Assertions over recorded real operations; synthetic unit vectors prove only these assertions. */
final class CompletionPollMatrixEvidence
{
    public const SCENARIOS = ['capacity', 'carrier-loss-activity', 'carrier-loss-workflow',
        'carrier-loss-workflow-completion', 'completion-transit', 'carrier-expiry', 'carrier-refence',
        'pipeline-buffer-age', 'prefetch-loss', 'default-off', 'explicit-off'];

    /** @param list<array<string, mixed>> $events
     * @return array<string, mixed>
     */
    public static function verify(array $events, string $scenario, string $worker, string $prefix): array
    {
        $requests = $answers = $deliveries = $current = $callbacks = [];
        foreach ($events as $position => $event) {
            if (in_array($event['kind'], ['relay_error', 'cleanup_error'], true)) throw new RuntimeException('completion_evidence_process_failure');
            if ($event['kind'] === 'worker_error' && !in_array($scenario, ['carrier-expiry', 'carrier-refence'], true)) {
                throw new RuntimeException('completion_evidence_process_failure');
            }
            $event['position'] = $position;
            if ($event['kind'] === 'request') $requests[$event['request']] = $event;
            if ($event['kind'] === 'upstream') $answers[$event['request']] = $event;
            if ($event['kind'] === 'delivered') {
                $deliveries[$event['request']] = $event;
                if ($event['status'] !== 200) continue;
                $answer = $answers[$event['request']]['answer'] ?? [];
                foreach ([$answer['task'] ?? null, $answer['activity_poll']['task'] ?? null, $answer['workflow_poll']['task'] ?? null] as $task) {
                    if (!is_array($task)) continue;
                    $current[$task['workflow_id']] = $task;
                    $current[$task['task_id']] = $task;
                }
            }
            if (in_array($event['kind'], ['callback', 'activity'], true)) {
                $task = $current[$event[$event['kind'] === 'callback' ? 'workflow_id' : 'task_id']] ?? null;
                if (is_array($task)) $callbacks[self::key($task)][] = $event;
            }
        }
        if ($requests === [] || $answers === []) throw new RuntimeException('completion_evidence_empty');
        if ($scenario === 'capacity') {
            CompletionPollEvidence::verify($events, 'carriers', $worker, $prefix);
            foreach ($answers as $id => $answer) {
                $body = $requests[$id]['body'];
                if (!isset($body['activity_poll'], $body['workflow_poll'])
                    || $answer['status'] !== 200 || ($deliveries[$id]['status'] ?? null) !== 200
                    || ($answer['answer']['activity_poll']['leased'] ?? null) !== true
                    || array_key_exists('workflow_poll', $answer['answer'])) continue;
                foreach ($requests as $nextId => $next) {
                    if ($next['position'] > $deliveries[$id]['position']
                        && isset($next['body']['workflow_poll'])
                        && $next['body']['workflow_poll']['poll_request_id'] !== $body['workflow_poll']['poll_request_id']
                        && ($answers[$nextId]['status'] ?? null) === 200 && ($deliveries[$nextId]['status'] ?? null) === 200
                        && ($answers[$nextId]['answer']['workflow_poll']['leased'] ?? null) === true) {
                        return ['missing_key_released_reservation' => true, 'capability_remained_enabled' => true];
                    }
                }
            }
            throw CompletionPollFailure::product('completion_evidence_missing_key_capacity');
        }
        if (in_array($scenario, ['default-off', 'explicit-off'], true)) {
            $option = self::fault($events, 'default_option', null);
            self::require($option['default'] === false && $option['explicit'] === ($scenario === 'explicit-off'),
                'completion_evidence_default_option');
            $parents = $children = [];
            foreach ($answers as $event) {
                foreach ([$event['answer']['task'] ?? null, $event['answer']['workflow_poll']['task'] ?? null] as $task) {
                    if (($task['workflow_id'] ?? null) === $prefix.'-2') $parents[] = $task['task_id'];
                    if (($task['workflow_id'] ?? null) === $prefix.'-3') $children[] = $task;
                }
            }
            foreach ($requests as $id => $request) {
                foreach (array_unique($parents) as $parent) {
                    if (!str_ends_with($request['path'], '/workflow-tasks/'.$parent.'/complete')) continue;
                    foreach ($children as $child) {
                        foreach ($callbacks[self::key($child)] ?? [] as $callback) {
                            self::require(($deliveries[$id]['status'] ?? null) === 200
                                && $deliveries[$id]['position'] < $callback['position'], 'completion_evidence_default_off_order');
                            return ['completion_remained_synchronous' => true, 'option' => $scenario];
                        }
                    }
                }
            }
            throw new RuntimeException('completion_evidence_default_off_target_missing');
        }
        if ($scenario === 'pipeline-buffer-age') {
            CompletionPollEvidence::verify($events, 'pipeline', $worker, $prefix);
            $hold = self::fault($events, 'fault_begin', 'buffer_age');
            $end = self::fault($events, 'fault_end', 'buffer_age');
            self::require(($end['at'] - $hold['at']) / 1e9 >= 1, 'completion_evidence_buffer_age');
            foreach ($answers as $id => $answer) {
                $task = $answer['answer']['task'] ?? null;
                if (($task['workflow_id'] ?? null) !== $prefix.'-3' || !isset($deliveries[$id])) continue;
                self::require($deliveries[$id]['position'] < $hold['position'], 'completion_evidence_buffer_age');
                self::confirmation($task, $callbacks, $requests, $answers, $deliveries, $end['position']);
                return ['actual_buffer_age_seconds' => ($end['at'] - $hold['at']) / 1e9, 'confirmed_before_callback' => true];
            }
            throw new RuntimeException('completion_evidence_buffer_target_missing');
        }
        if ($scenario === 'completion-transit') {
            $fault = self::fault($events, 'fault_begin', 'completion_transit');
            $id = $fault['request'];
            $grant = $answers[$id]['answer']['workflow_poll'] ?? [];
            $window = CompletionPollFaults::window($grant);
            $transit = ($deliveries[$id]['at'] - $requests[$id]['at']) / 1e9;
            self::require(($answers[$id]['status'] ?? null) === 200 && $window >= 12 && $window - $transit < 10,
                'completion_evidence_completion_transit_not_targeted');
            self::confirmation($grant['task'], $callbacks, $requests, $answers, $deliveries, $deliveries[$id]['position']);
            return ['original_completion_transit_seconds' => $transit, 'confirmed_before_callback' => true];
        }
        if ($scenario === 'carrier-expiry') {
            $fault = self::fault($events, 'fault_begin', 'expiry');
            $id = $fault['request'];
            $grant = $answers[$id]['answer']['workflow_poll'] ?? [];
            $task = $grant['task'] ?? throw new RuntimeException('completion_evidence_expiry_target_missing');
            self::require(($deliveries[$id]['at'] - $answers[$id]['at']) / 1e9 >= CompletionPollFaults::window($grant),
                'completion_evidence_expiry_not_reached');
            self::require(($callbacks[self::key($task)] ?? []) === [], 'completion_evidence_expired_callback');
            $refused = false;
            foreach ($requests as $requestId => $request) {
                if (!self::fence($request, $task)) continue;
                $reply = $answers[$requestId] ?? [];
                if (str_ends_with($request['path'], '/heartbeat') && ($reply['status'] ?? null) === 409
                    && in_array($reply['answer']['reason'] ?? null, ['lease_expired', 'task_not_leased', 'workflow_task_attempt_mismatch'], true)
                    && ($reply['answer']['task_id'] ?? null) === $task['task_id']
                    && ($reply['answer']['workflow_task_attempt'] ?? null) === $task['workflow_task_attempt']) $refused = true;
                if (str_ends_with($request['path'], '/complete') || str_ends_with($request['path'], '/fail')) {
                    self::require(($reply['answer']['recorded'] ?? null) !== true, 'completion_evidence_expired_publication');
                }
            }
            self::require($refused, 'completion_evidence_expiry_refusal_missing');
            return ['original_expiry_reached' => true, 'expired_callback_or_publication' => false, 'real_identity_bound_refusal' => true];
        }
        $refence = $scenario === 'carrier-refence';
        $prefetch = $scenario === 'prefetch-loss';
        $fault = self::fault($events, 'fault', $refence ? 'refenced_activity_answer' : ($prefetch ? 'lost_prefetch_answer' : 'lost_carried_answer'));
        $id = $fault['request'];
        $parent = $requests[$id];
        $member = $fault['member'] ?? null;
        $kind = $member === 'activity_poll' ? 'activity' : 'workflow';
        $original = $prefetch ? $answers[$id]['answer'] : ($answers[$id]['answer'][$member] ?? []);
        $task = $original['task'] ?? throw new RuntimeException('completion_evidence_loss_target_missing');
        $poll = $prefetch ? $parent['body'] : ($parent['body'][$member] ?? []);
        $expectedMember = $scenario === 'carrier-loss-activity' || $refence ? 'activity_poll' : 'workflow_poll';
        $expectedPath = $scenario === 'carrier-loss-workflow' ? '/activity-tasks/' : '/workflow-tasks/';
        self::require(($answers[$id]['status'] ?? null) === 200 && ($deliveries[$id]['status'] ?? null) === 502
            && ($prefetch || ($member === $expectedMember && str_contains($parent['path'], $expectedPath)))
            && ($poll['worker_id'] ?? null) === $worker && ($task['lease_owner'] ?? null) === $worker
            && is_string($poll['poll_request_id'] ?? null), 'completion_evidence_loss_not_targeted');
        foreach ($requests as $retryId => $request) {
            if ($request['position'] <= $deliveries[$id]['position']
                || !str_ends_with($request['path'], '/'.$kind.'-tasks/poll')) continue;
            self::require(($request['body']['poll_request_id'] ?? null) === $poll['poll_request_id'], 'completion_evidence_recovery_identity_changed');
            if ($refence) {
                $rotation = self::fault($events, 'refence', null);
                self::require($rotation['old_registration_sha256'] !== $rotation['registration_sha256']
                    && $rotation['position'] < $request['position'] && ($answers[$retryId]['status'] ?? null) === 409
                    && ($deliveries[$retryId]['status'] ?? null) === 409
                    && ($answers[$retryId]['answer']['reason'] ?? null) === 'poll_cached_task_expired'
                    && ($callbacks[self::key($task)] ?? []) === [], 'completion_evidence_refence_recovery');
                return ['registration_replaced' => true, 'original_activity_recovery_refused' => true,
                    'existing_workflow_grant_revocation_asserted' => false];
            }
            $replayed = $answers[$retryId]['answer']['task'] ?? null;
            self::require(($answers[$retryId]['status'] ?? null) === 200 && is_array($replayed)
                && is_string($task['identity_sha256'] ?? null)
                && $task['identity_sha256'] === ($replayed['identity_sha256'] ?? null), 'completion_evidence_replay_authority_changed');
            self::require(($deliveries[$retryId]['status'] ?? null) === 200
                && array_filter($callbacks[self::key($task)] ?? [], static fn (array $event): bool => $event['position'] > $deliveries[$retryId]['position']) !== [],
                'completion_evidence_recovered_callback_missing');
            if (!$prefetch) {
                $sameParent = array_filter($requests, static fn (array $candidate): bool => $candidate['path'] === $parent['path']);
                self::require(count($sameParent) >= 2, 'completion_evidence_completion_retry_missing');
                foreach ($sameParent as $candidate) {
                    self::require($candidate['body'] === $parent['body'], 'completion_evidence_completion_retry_changed');
                }
            } else {
                $parentTasks = [];
                foreach ($answers as $answer) {
                    foreach ([$answer['answer']['task'] ?? null, $answer['answer']['workflow_poll']['task'] ?? null] as $candidate) {
                        if (($candidate['workflow_id'] ?? null) === $prefix.'-2') $parentTasks[] = $candidate['task_id'];
                    }
                }
                $published = false;
                foreach ($requests as $parentId => $candidate) {
                    foreach (array_unique($parentTasks) as $taskId) {
                        if (str_ends_with($candidate['path'], '/workflow-tasks/'.$taskId.'/complete')
                            && $candidate['position'] > $parent['position'] && $candidate['position'] < $request['position']
                            && ($answers[$parentId]['status'] ?? null) === 200 && !isset($candidate['body']['workflow_poll'])) $published = true;
                    }
                }
                $retry = array_filter($events, static fn (array $event): bool => $event['kind'] === 'poll_retry'
                    && $event['task_kind'] === 'workflow' && $event['attempt'] === 1 && $event['delay'] >= 0.1);
                self::require($published && $retry !== []
                    && ($request['at'] - $deliveries[$id]['at']) / 1e9 >= 0.09, 'completion_evidence_prefetch_recovery_policy');
            }
            return ['original_poll_id_recovered' => true, 'original_task_history_deadlines_preserved' => true, 'kind' => $kind];
        }
        throw CompletionPollFailure::product('completion_evidence_recovery_missing');
    }

    /** @param list<array<string, mixed>> $events
     * @return array<string, mixed>
     */
    private static function fault(array $events, string $kind, ?string $mode): array
    {
        foreach ($events as $position => $event) {
            if ($event['kind'] === $kind && ($mode === null || ($event['mode'] ?? null) === $mode)) return $event + ['position' => $position];
        }
        throw new RuntimeException('completion_evidence_fault_not_observed');
    }

    /** @param array<string, mixed> $task */
    private static function key(array $task): string
    {
        return json_encode([$task['task_id'] ?? null, $task['lease_owner'] ?? null,
            $task['workflow_task_attempt'] ?? null, $task['activity_attempt_id'] ?? null], JSON_THROW_ON_ERROR);
    }

    /** @param array<string, mixed> $request
     * @param array<string, mixed> $task
     */
    private static function fence(array $request, array $task): bool
    {
        return str_contains($request['path'], '/workflow-tasks/'.rawurlencode($task['task_id']).'/')
            && ($request['body']['lease_owner'] ?? null) === $task['lease_owner']
            && ($request['body']['workflow_task_attempt'] ?? null) === $task['workflow_task_attempt'];
    }

    /** @param array<string, mixed> $task
     * @param array<string, list<array<string, mixed>>> $callbacks
     * @param array<string, array<string, mixed>> $requests
     * @param array<string, array<string, mixed>> $answers
     * @param array<string, array<string, mixed>> $deliveries
     */
    private static function confirmation(array $task, array $callbacks, array $requests, array $answers, array $deliveries, int $after): void
    {
        foreach ($callbacks[self::key($task)] ?? [] as $callback) {
            foreach ($requests as $id => $request) {
                if (self::fence($request, $task) && str_ends_with($request['path'], '/heartbeat')
                    && $request['position'] > $after && ($answers[$id]['status'] ?? null) === 200
                    && ($answers[$id]['answer']['renewed'] ?? null) === true
                    && ($answers[$id]['answer']['task_id'] ?? null) === $task['task_id']
                    && ($answers[$id]['answer']['lease_owner'] ?? null) === $task['lease_owner']
                    && ($answers[$id]['answer']['workflow_task_attempt'] ?? null) === $task['workflow_task_attempt']
                    && ($deliveries[$id]['status'] ?? null) === 200 && $deliveries[$id]['position'] < $callback['position']) return;
            }
        }
        throw CompletionPollFailure::product('completion_evidence_confirmation_missing');
    }

    private static function require(bool $condition, string $code): void
    {
        if (!$condition) throw CompletionPollFailure::product($code);
    }
}
