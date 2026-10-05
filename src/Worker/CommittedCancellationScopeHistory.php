<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use InvalidArgumentException;

/**
 * @internal Cold replay of committed, unscheduled single-call scope boundaries.
 * These facts never grant authority to prepare, stop or publish an operation.
 */
final class CommittedCancellationScopeHistory
{
    /** @var array<int, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> */
    public readonly array $deliveries;

    /** @var array<string, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> */
    public readonly array $preparations;

    /** @param list<array<string, mixed>> $history */
    public function __construct(array $history, string $runId, string $workflowId, CancellationScopeHistory $scopes, bool $requireCommittedDelivery = true, bool $inspectActivityProjections = false, bool $inspectOperationProjections = false)
    {
        $addresses = [];
        foreach ($scopes->openings as $sequence => $opening) {
            $addresses[$opening['scope_id']] = [...$opening, 'sequence' => $sequence];
        }
        $requests = [];
        $requestIds = [];
        $preparations = [];
        $verifiedPreparations = [];
        /** @var array<int, array{context: ScopedCancellationContext, boundary: CancellationDelivery, event: array<string, mixed>}> $deliveries */
        $deliveries = [];
        $opened = [];
        $admissions = [];
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
                        $parentRequest = is_string($parent) ? ($requests[$parent]['context'] ?? null) : null;
                        if (!$parentRequest instanceof ScopedCancellationContext || $address['shield_parent']
                            || $parent !== $address['parent_scope_id']
                            || $context->rootContext->toArray() !== $parentRequest->rootContext->toArray()
                            || array_slice($context->lineage, 0, -1) !== $parentRequest->lineage
                            || $context->deadline() != $parentRequest->deadline()) {
                            throw new InvalidArgumentException('Inherited scope cancellation changes its accepted parent or crosses a shield.');
                        }
                    }
                    $requests[$scopeId] = ['context' => $context, 'time' => $recordedAt];
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
                    if (!in_array($boundary->callKind, ['activity', 'timer', 'condition', 'child'], true)) {
                        throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker only supports committed single activity, timer, condition and child scope boundaries.');
                    }
                    // Every admitted operation must be accounted for by its qualified projection.
                    foreach ($admissions as $admission => $addressesBySequence) {
                        $sequences = array_column($projections[$admission] ?? [], 'sequence');
                        $unsupported = array_diff_key(array_filter($addressesBySequence, static fn (string $address): bool => $address === $scopeId), array_flip($sequences));
                        $callKind = match ($admission) { 'ActivityScheduled' => 'activity', 'TimerScheduled' => 'timer',
                            'ConditionWaitOpened' => 'condition', 'SignalWaitOpened' => 'signal', default => 'child' };
                        // A condition/signal timeout shares its wait's authored position.
                        $timeout = $admission === 'TimerScheduled' && in_array($boundary->sequence,
                            array_column($projections['wait'] ?? [], 'sequence'), true);
                        if ($unsupported !== [] || (isset($addressesBySequence[$boundary->sequence])
                            && (!in_array($boundary->sequence, $sequences, true) || (!$timeout && $boundary->callKind !== $callKind)))) {
                            throw new InvalidArgumentException('Scope preparation cannot omit or replace an admitted operation.');
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
        if ($requireCommittedDelivery && ($preparations !== [] || count($deliveries) !== count($requests))) {
            throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: this PHP worker requires committed scope delivery before replaying cleanup.');
        }
        ksort($deliveries);
        $this->deliveries = $deliveries;
        $this->preparations = $verifiedPreparations;
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
