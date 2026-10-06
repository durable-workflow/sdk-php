<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use InvalidArgumentException;

/** @internal A frozen unshielded subtree, never authority to redispatch its operations. */
final class CancellationScopeDescendantProjection
{
    /** @return list<array<string, mixed>> */
    public static function normalize(mixed $members): array
    {
        if (!is_array($members) || !array_is_list($members)) {
            throw new InvalidArgumentException('Scope descendants must be a list.');
        }
        $keys = ['scope_id', 'parent_scope_id', 'scope_history_event_id', 'request_history_event_id',
            'request_id', 'propagation_history_event_id', 'authority_deadline_at'];
        $result = []; $ids = [];
        foreach ($members as $member) {
            if (!is_array($member) || count($member) !== 12 || !is_array($member['cancellation'] ?? null)) {
                throw new InvalidArgumentException('Scope descendant omits its original authority.');
            }
            $value = [];
            foreach ($keys as $key) {
                if (!CancellationScopeOperationProjection::identity($member[$key] ?? null)) {
                    throw new InvalidArgumentException('Scope descendant changes an original identity.');
                }
                $value[$key] = $member[$key];
            }
            $context = ScopedCancellationContext::fromArray($member['cancellation']);
            $deadline = CancellationScopeOperationProjection::timestamp($member['authority_deadline_at']);
            if (isset($ids[$member['scope_id']]) || $context->scopeId !== $member['scope_id']
                || $context->requestId !== $member['request_id'] || $deadline > $context->deadline()) {
                throw new InvalidArgumentException('Scope descendant changes its accepted address or deadline.');
            }
            $ids[$member['scope_id']] = true;
            $result[] = [...$value, 'cancellation' => $context->toArray(),
                'activity_members' => CancellationScopeActivityProjection::normalize($member['activity_members'] ?? null),
                'timer_members' => CancellationScopeOperationProjection::normalize('timer_members', $member['timer_members'] ?? null),
                'wait_members' => CancellationScopeOperationProjection::normalize('wait_members', $member['wait_members'] ?? null),
                'child_members' => CancellationScopeOperationProjection::normalize('child_members', $member['child_members'] ?? null)];
        }

        return $result;
    }

    /**
     * @param list<array<string, mixed>> $prefix Events strictly before the original preparation.
     * @return list<array<string, mixed>>
     */
    public static function fromHistoryPrefix(array $prefix, string $scopeId, string $runId, string $authorityDeadline): array
    {
        $openings = []; $requests = [];
        foreach ($prefix as $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            if ($kind === 'CancellationScopeOpened') { $openings[$event['payload']['scope_id']] = $event; }
            if ($kind === 'CancellationScopeRequested') {
                $id = $event['payload']['scope_id'];
                if (isset($requests[$id])) { throw new InvalidArgumentException('Scope descendant repeats its accepted request.'); }
                $requests[$id] = $event;
            }
        }
        $included = [$scopeId => true]; $members = [];
        foreach ($openings as $id => $opening) {
            $address = $opening['payload']; $parentId = $address['parent_scope_id'];
            if ($id === $scopeId || $address['shield_parent'] || !isset($included[$parentId])) { continue; }
            $included[$id] = true;
            $request = $requests[$id] ?? null; $parentRequest = $requests[$parentId] ?? null;
            if ($request === null || $parentRequest === null) {
                throw new InvalidArgumentException('Scope descendant lacks its accepted propagation.');
            }
            $context = ScopedCancellationContext::fromArray($request['payload']['cancellation']);
            $parent = ScopedCancellationContext::fromArray($parentRequest['payload']['cancellation']);
            $propagation = $request;
            if ($context->rootRequestId === $parent->rootRequestId) {
                if (($request['payload']['parent_scope_id'] ?? null) !== $parentId) {
                    throw new InvalidArgumentException('Scope descendant changes its original parent.');
                }
            } else {
                $propagation = null;
                foreach ($prefix as $event) {
                    $payload = $event['payload'];
                    if (($event['event_type'] ?? $event['type'] ?? null) !== 'CancellationScopeRequestConflicted'
                        || ($payload['scope_id'] ?? null) !== $id || ($payload['parent_scope_id'] ?? null) !== $parentId) { continue; }
                    if (($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope-request/v1'
                        || ($payload['workflow_run_id'] ?? null) !== $runId || ($payload['reason'] ?? null) !== 'cancellation_root_conflict'
                        || $event['sequence'] <= $request['sequence'] || $event['sequence'] <= $parentRequest['sequence']) {
                        throw new InvalidArgumentException('Scope descendant changes its original conflict boundary.');
                    }
                    $incoming = ScopedCancellationContext::fromArray($payload['incoming_cancellation'] ?? []);
                    $accepted = ScopedCancellationContext::fromArray($payload['accepted_cancellation'] ?? []);
                    $last = $incoming->lineage[count($incoming->lineage) - 1];
                    if ($incoming->rootContext->toArray() === $parent->rootContext->toArray()
                        && array_slice($incoming->lineage, 0, -1) === $parent->lineage
                        && $incoming->deadline() == $parent->deadline() && $incoming->scopeId === $id
                        && $incoming->workflowRunId === $runId && $incoming->workflowInstanceId === $context->workflowInstanceId
                        && $last['cleanup_deadline_at'] === $parent->deadline()->format('Y-m-d\TH:i:s.u\Z')
                        && $accepted->toArray() === $context->toArray()) {
                        $propagation = $event; break;
                    }
                }
                if ($propagation === null) {
                    throw new InvalidArgumentException('Scope descendant lacks its original competing-root conflict.');
                }
            }
            $deadline = CancellationScopeOperationProjection::timestamp($authorityDeadline);
            $ancestor = $id;
            while ($ancestor !== 'root') {
                if (isset($requests[$ancestor])) {
                    $ceiling = ScopedCancellationContext::fromArray($requests[$ancestor]['payload']['cancellation'])->deadline();
                    if ($ceiling < $deadline) { $deadline = $ceiling; }
                }
                $ancestor = $openings[$ancestor]['payload']['parent_scope_id'];
            }
            $members[] = ['scope_id' => $id, 'parent_scope_id' => $parentId, 'scope_history_event_id' => $opening['id'],
                'request_history_event_id' => $request['id'], 'request_id' => $context->requestId,
                'propagation_history_event_id' => $propagation['id'], 'authority_deadline_at' => $deadline->format('Y-m-d\TH:i:s.u\Z'),
                'cancellation' => $context->toArray(), 'activity_members' => CancellationScopeActivityProjection::fromHistoryPrefix($prefix, $id),
                'timer_members' => CancellationScopeOperationProjection::fromHistoryPrefix('timer_members', $prefix, $id, $runId),
                'wait_members' => CancellationScopeOperationProjection::fromHistoryPrefix('wait_members', $prefix, $id, $runId),
                'child_members' => CancellationScopeOperationProjection::fromHistoryPrefix('child_members', $prefix, $id, $runId)];
        }

        return self::normalize($members);
    }
}
