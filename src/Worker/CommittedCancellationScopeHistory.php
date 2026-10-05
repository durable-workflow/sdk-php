<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use InvalidArgumentException;

/**
 * @internal Immutable receipts for committed scope boundaries and their frozen subtree.
 * These facts never grant authority to prepare, stop or publish an operation.
 */
final class CommittedCancellationScopeHistory
{
    /** @var array<int, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> */
    public readonly array $deliveries;

    /** @var array<string, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> */
    public readonly array $preparations;

    /** @var array<string, array{context: ScopedCancellationContext, time: DateTimeImmutable, index: int}> */
    private readonly array $pendingRequests;

    /** @param list<array<string, mixed>> $history */
    public function __construct(array $history, string $runId, string $workflowId, CancellationScopeHistory $scopes, bool $requireCommittedDelivery = true, bool $inspectActivityProjections = false, bool $inspectOperationProjections = false, bool $allowPreparedLocalBoundary = false)
    {
        $addresses = [];
        foreach ($scopes->openings as $sequence => $opening) {
            $addresses[$opening['scope_id']] = [...$opening, 'sequence' => $sequence];
        }
        $requests = [];
        $runCancellation = CancellationHistory::fromEvents($history, $runId);
        $runRoot = $runCancellation->request?->context === null ? null
            : ScopedCancellationContext::fromRunContext($runCancellation->request->context);
        $requestIds = [];
        $preparations = [];
        $verifiedPreparations = [];
        /** @var array<int, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> $deliveries */
        $deliveries = [];
        $opened = [];
        $admissions = [];
        $localActivities = [];
        /** @var array<string, true> $deliveredScopes */
        $deliveredScopes = [];
        foreach ($history as $historyIndex => $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $eventPayload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            if ($kind === 'CancellationScopeOpened') {
                $opened[$eventPayload['scope_id']] = true;
            }
            if (in_array($kind, ['ActivityScheduled', 'TimerScheduled', 'ChildWorkflowScheduled', 'ConditionWaitOpened', 'SignalWaitOpened'], true)
                && is_int($eventPayload['sequence'] ?? null)) {
                $admissions[$kind][$eventPayload['sequence']] = $scopes->memberships[$eventPayload['sequence']] ?? 'root';
                if ($kind === 'ActivityScheduled') {
                    $localActivities[$eventPayload['sequence']] = ($eventPayload['local_activity'] ?? false) === true;
                }
            }
            if (!in_array($kind, ['CancellationScopeRequested', 'CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)) {
                continue;
            }
            try {
                $payload = $event['payload'] ?? null;
                if (!is_array($payload) || array_is_list($payload) || !is_array($payload['cancellation'] ?? null)) {
                    throw new InvalidArgumentException('Scope cancellation must carry its canonical context.');
                }
                $context = ScopedCancellationContext::fromArray($payload['cancellation']);
                $scopeId = $context->scopeId;
                $address = $addresses[$scopeId] ?? null;
                if ($address === null || !isset($opened[$scopeId]) || $context->workflowRunId !== $runId || $runId === ''
                    || $context->workflowInstanceId !== $workflowId || $workflowId === ''
                    || ($payload['workflow_run_id'] ?? null) !== $runId
                    || ($payload['scope_id'] ?? null) !== $scopeId || ($payload['request_id'] ?? null) !== $context->requestId) {
                    throw new InvalidArgumentException('Scope cancellation changes its canonical run, request or authored address.');
                }
                $recordedAt = self::timestamp($event['timestamp'] ?? $event['recorded_at'] ?? null);
                if ($recordedAt < $context->requestedAt()) {
                    throw new InvalidArgumentException('Scope cancellation predates its original request.');
                }
                if ($kind === 'CancellationScopeRequested') {
                    if (($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope-request/v1'
                        || !array_key_exists('parent_scope_id', $payload) || isset($requests[$scopeId])
                        || isset($requestIds[$context->requestId])) {
                        throw new InvalidArgumentException('Scope cancellation requires one accepted original request.');
                    }
                    $parent = $payload['parent_scope_id'];
                    if ($parent === null) {
                        if (count($context->lineage) !== 1 || $context->requestId !== $context->rootContext->requestId) {
                            throw new InvalidArgumentException('Direct scope cancellation cannot substitute an inherited lineage.');
                        }
                    } else {
                        $parentRequest = $parent === 'root' && $runCancellation->requestIndex < $historyIndex
                            && $runRoot?->workflowRunId === $runId && $runRoot->workflowInstanceId === $workflowId
                            ? $runRoot : (is_string($parent) ? ($requests[$parent]['context'] ?? null) : null);
                        if (!$parentRequest instanceof ScopedCancellationContext || $address['shield_parent']
                            || $parent !== $address['parent_scope_id']
                            || $context->rootContext->toArray() !== $parentRequest->rootContext->toArray()
                            || array_slice($context->lineage, 0, -1) !== $parentRequest->lineage
                            || $context->deadline() != $parentRequest->deadline()) {
                            throw new InvalidArgumentException('Inherited scope cancellation changes its accepted parent or crosses a shield.');
                        }
                    }
                    $requests[$scopeId] = ['context' => $context, 'time' => $recordedAt, 'index' => $historyIndex];
                    $requestIds[$context->requestId] = true;
                    continue;
                }
                $request = $requests[$scopeId] ?? null;
                if ($request === null || $context->toArray() !== $request['context']->toArray()
                    || $recordedAt < $request['time']) {
                    throw new InvalidArgumentException('Scope boundary lacks its earlier immutable accepted request.');
                }
                foreach (['sequence_span', 'operation_sequence', 'operation_sequence_span'] as $field) {
                    if (!array_key_exists($field, $payload)) {
                        throw new InvalidArgumentException('Scope boundary omits its original operation range.');
                    }
                }
                $boundary = CancellationDelivery::fromPayload([...$payload, 'workflow_command_id' => $context->requestId]);
                if ($boundary->sequence <= $address['sequence']) {
                    throw new InvalidArgumentException('Scope delivery must follow its original opening.');
                }
                $deadline = self::timestamp($payload['authority_deadline_at'] ?? null);
                if ($deadline < $context->requestedAt() || $deadline > $context->deadline() || $recordedAt > $deadline) {
                    throw new InvalidArgumentException('Scope boundary changes or exceeds its original authority ceiling.');
                }
                if ($kind === 'CancellationScopeDeliveryPrepared') {
                    if (($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope-preparation/v5'
                        || isset($preparations[$scopeId]) || isset($deliveredScopes[$scopeId])) {
                        throw new InvalidArgumentException('Scope delivery requires one original v5 preparation.');
                    }
                    foreach (['activity_members', 'timer_members', 'wait_members', 'child_members', 'descendant_members'] as $field) {
                        if (!is_array($payload[$field] ?? null) || !array_is_list($payload[$field])) {
                            throw new InvalidArgumentException('Scope preparation omits its frozen member projection.');
                        }
                        if ($payload[$field] !== [] && !$inspectOperationProjections && ($field !== 'activity_members' || !$inspectActivityProjections)) {
                            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: committed scope member projection replay is not yet qualified by this PHP worker.');
                        }
                    }
                    $members = [];
                    if ($inspectActivityProjections || $inspectOperationProjections) {
                        $members = CancellationScopeActivityProjection::normalize($payload['activity_members']);
                        if ($members !== CancellationScopeActivityProjection::fromHistoryPrefix(array_slice($history, 0, $historyIndex), $scopeId)) {
                            throw new InvalidArgumentException('Scope preparation changes its original Activity projection.');
                        }
                    }
                    $projections = ['ActivityScheduled' => $members];
                    if ($inspectOperationProjections) {
                        $prefix = array_slice($history, 0, $historyIndex);
                        foreach (['timer_members' => 'TimerScheduled', 'wait_members' => 'wait', 'child_members' => 'ChildWorkflowScheduled'] as $field => $admission) {
                            $projection = CancellationScopeOperationProjection::normalize($field, $payload[$field]);
                            if ($projection !== CancellationScopeOperationProjection::fromHistoryPrefix($field, $prefix, $scopeId, $runId)) {
                                throw new InvalidArgumentException('Scope preparation changes its original operation projection.');
                            }
                            $projections[$admission] = $projection;
                        }
                        $projections['ConditionWaitOpened'] = array_values(array_filter($projections['wait'], static fn (array $member): bool => $member['kind'] === 'condition'));
                        $projections['SignalWaitOpened'] = array_values(array_filter($projections['wait'], static fn (array $member): bool => $member['kind'] === 'signal'));
                        if (CancellationScopeDescendantProjection::normalize($payload['descendant_members'])
                            !== CancellationScopeDescendantProjection::fromHistoryPrefix($prefix, $scopeId, $runId, $payload['authority_deadline_at'])) {
                            throw new InvalidArgumentException('Scope preparation changes its original descendant projection.');
                        }
                    }
                    $projectionsByScope = [$scopeId => $projections];
                    foreach ($payload['descendant_members'] as $member) {
                        $waits = $member['wait_members'];
                        $projectionsByScope[$member['scope_id']] = [
                            'ActivityScheduled' => $member['activity_members'], 'TimerScheduled' => $member['timer_members'],
                            'ChildWorkflowScheduled' => $member['child_members'], 'wait' => $waits,
                            'ConditionWaitOpened' => array_values(array_filter($waits, static fn (array $wait): bool => $wait['kind'] === 'condition')),
                            'SignalWaitOpened' => array_values(array_filter($waits, static fn (array $wait): bool => $wait['kind'] === 'signal')),
                        ];
                    }
                    $operationScope = $scopes->memberships[$boundary->sequence] ?? $scopeId;
                    if (!isset($projectionsByScope[$operationScope])) {
                        throw new InvalidArgumentException('Scope boundary cannot consume an operation outside its frozen subtree.');
                    }
                    if ($boundary->callKind === 'parallel') {
                        self::assertCompleteGroup($boundary, $operationScope, array_slice($history, 0, $historyIndex), $scopes);
                    } elseif (!in_array($boundary->callKind, ['activity', 'timer', 'condition', 'child'], true)
                        && !($allowPreparedLocalBoundary && $boundary->callKind === 'local_activity')) {
                        throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker only supports committed single activity, timer, condition and child scope boundaries.');
                    }
                    // Every admitted operation must be accounted for by its qualified projection.
                    foreach ($projectionsByScope as $memberScope => $projections) {
                        foreach ($admissions as $admission => $addressesBySequence) {
                            $sequences = array_column($projections[$admission] ?? [], 'sequence');
                            $unsupported = array_diff_key(array_filter($addressesBySequence, static fn (string $address): bool => $address === $memberScope), array_flip($sequences));
                            $callKind = match ($admission) { 'ActivityScheduled' => $boundary->callKind === 'local_activity'
                                && ($localActivities[$boundary->sequence] ?? false) ? 'local_activity' : 'activity', 'TimerScheduled' => 'timer',
                                'ConditionWaitOpened' => 'condition', 'SignalWaitOpened' => 'signal', default => 'child' };
                            // A condition/signal timeout shares its wait's authored position.
                            $timeout = $admission === 'TimerScheduled' && in_array($boundary->sequence,
                                array_column($projections['wait'] ?? [], 'sequence'), true);
                            if ($unsupported !== [] || (($addressesBySequence[$boundary->sequence] ?? null) === $memberScope
                                && (!in_array($boundary->sequence, $sequences, true) || (!$timeout && $boundary->callKind !== 'parallel' && $boundary->callKind !== $callKind)))) {
                                throw new InvalidArgumentException('Scope preparation cannot omit or replace an admitted operation.');
                            }
                        }
                    }
                    $preparations[$scopeId] = ['id' => $event['id'], 'boundary' => $boundary, 'deadline' => $deadline, 'time' => $recordedAt];
                    $verifiedPreparations[$scopeId] = ['context' => $context, 'boundary' => $boundary, 'event' => $event];
                    continue;
                }
                $preparation = $preparations[$scopeId] ?? null;
                if (($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope-delivery/v1'
                    || $preparation === null || ($payload['preparation_history_event_id'] ?? null) !== $preparation['id']
                    || $boundary != $preparation['boundary'] || $deadline != $preparation['deadline']
                    || $recordedAt < $preparation['time'] || isset($deliveries[$boundary->sequence])) {
                    throw new InvalidArgumentException('Scope delivery changes or duplicates its original prepared boundary.');
                }
                $deliveries[$boundary->sequence] = ['context' => $context, 'boundary' => $boundary, 'event' => $event];
                $deliveredScopes[$scopeId] = true;
                // A scope cannot deliver twice at different positions either.
                unset($preparations[$scopeId]);
            } catch (InvalidArgumentException $error) {
                throw new NonDeterministicWorkflow($error->getMessage(), reason: 'invalid_cancellation_scope_history');
            }
        }
        $coveredRequests = [];
        foreach ($deliveries as $delivery) {
            $coveredRequests[$delivery['context']->scopeId] = true;
            foreach ($verifiedPreparations[$delivery['context']->scopeId]['event']['payload']['descendant_members'] as $member) {
                $coveredRequests[$member['scope_id']] = true;
            }
        }
        if ($requireCommittedDelivery && ($preparations !== [] || array_diff_key($requests, $coveredRequests) !== [])) {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker requires committed scope delivery before replaying cleanup.');
        }
        ksort($deliveries);
        $this->deliveries = $deliveries;
        $this->preparations = $verifiedPreparations;
        $this->pendingRequests = array_diff_key($requests, $coveredRequests);
    }

    /** Select an accepted original ancestor without crossing a shield or borrowing another root.
     * @return array{context: ScopedCancellationContext, time: DateTimeImmutable, index: int}|null
     */
    public function pendingRequestForScope(string $scopeId, CancellationScopeHistory $scopes): ?array
    {
        $addresses = [];
        foreach ($scopes->openings as $opening) { $addresses[$opening['scope_id']] = $opening; }
        $request = $this->pendingRequests[$scopeId] ?? null;
        $active = $request;
        while (isset($addresses[$scopeId]) && !$addresses[$scopeId]['shield_parent']) {
            $scopeId = $addresses[$scopeId]['parent_scope_id'];
            $ancestor = $this->pendingRequests[$scopeId] ?? null;
            if ($ancestor === null) { continue; }
            if ($active === null || $ancestor['context']->rootContext->toArray() !== $active['context']->rootContext->toArray()
                || array_slice($active['context']->lineage, 0, count($ancestor['context']->lineage)) !== $ancestor['context']->lineage) {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: pending scope selection requires its accepted original ancestor lineage.');
            }
            $request = $ancestor;
        }

        return $request;
    }

    /**
     * @param array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>} $delivery
     * @return array<string, array{context: ScopedCancellationContext, authority_deadline_at: string}>
     */
    public function scopeStatesForDelivery(array $delivery): array
    {
        $context = $delivery['context'];
        $payload = $this->preparations[$context->scopeId]['event']['payload'];
        $states = [$context->scopeId => ['context' => $context, 'authority_deadline_at' => $payload['authority_deadline_at']]];
        foreach ($payload['descendant_members'] as $member) {
            $descendant = ScopedCancellationContext::fromArray($member['cancellation']);
            if ($descendant->rootContext->toArray() !== $context->rootContext->toArray()
                || array_slice($descendant->lineage, 0, count($context->lineage)) !== $context->lineage) {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not qualified competing descendant roots.');
            }
            $states[$descendant->scopeId] = ['context' => $descendant, 'authority_deadline_at' => $member['authority_deadline_at']];
        }

        return $states;
    }

    /** @param list<array<string, mixed>> $prefix */
    private static function assertCompleteGroup(CancellationDelivery $boundary, string $scopeId, array $prefix, CancellationScopeHistory $scopes): void
    {
        $members = [];
        foreach ($prefix as $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $sequence = $payload['sequence'] ?? null;
            if (!in_array($kind, ['ActivityScheduled', 'TimerScheduled', 'ChildWorkflowScheduled', 'ConditionWaitOpened', 'SignalWaitOpened'], true)
                || !is_int($sequence) || !$boundary->interrupts($sequence)
                || ($kind === 'TimerScheduled' && in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true))) {
                continue;
            }
            if (($scopes->memberships[$sequence] ?? 'root') !== $scopeId || isset($members[$sequence])) {
                throw new InvalidArgumentException('Scope group changes an original member address or duplicates its authored position.');
            }
            if ($kind === 'SignalWaitOpened' || ($payload['local_activity'] ?? false) === true || ($payload['execution_mode'] ?? null) === 'local') {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not qualified scoped signal or local group execution.');
            }
            $path = $payload['parallel_group_path'] ?? [$payload];
            if (!is_array($path) || !array_is_list($path) || $path === [] || !is_array($path[0])) {
                throw new InvalidArgumentException('Scope group requires its original admitted group path.');
            }
            foreach ($path as $entry) {
                if (!is_array($entry)) {
                    throw new InvalidArgumentException('Scope group has a malformed original group path.');
                }
                if (($entry['parallel_group_mode'] ?? 'all') !== 'all'
                    || (is_string($entry['parallel_group_id'] ?? null) && str_starts_with($entry['parallel_group_id'], 'select-calls:'))) {
                    throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker has not qualified scoped selection groups.');
                }
            }
            if (($path[0]['parallel_group_base_sequence'] ?? null) !== $boundary->sequence
                || ($path[0]['parallel_group_size'] ?? null) !== $boundary->sequenceSpan
                || ($path[0]['parallel_group_index'] ?? null) !== $sequence - $boundary->sequence) {
                throw new InvalidArgumentException('Scope group changes its original range or member index.');
            }
            $members[$sequence] = true;
        }
        if (count($members) !== $boundary->sequenceSpan) {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker requires every original scoped group member admitted before replay.');
        }
    }

    private static function timestamp(mixed $value): DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Scope cancellation requires a canonical timestamp with timezone.');
        }
        try {
            $time = new DateTimeImmutable($value);
        } catch (\Exception $error) {
            throw new InvalidArgumentException('Scope cancellation timestamp is invalid.', previous: $error);
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Scope cancellation timestamp is invalid.');
        }

        return $time;
    }
}
