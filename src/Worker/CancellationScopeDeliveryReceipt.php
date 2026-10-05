<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use InvalidArgumentException;

/** @internal Verified committed scope facts, never permission to execute cleanup. */
final class CancellationScopeDeliveryReceipt
{
    /**
     * @param list<array{sequence: int, activity_execution_id: string, descriptor_hash: string}> $activityMembers
     * @param list<array<string, mixed>> $history
     */
    private function __construct(
        public readonly ScopedCancellationContext $context,
        public readonly CancellationDelivery $boundary,
        public readonly string $preparationHistoryEventId,
        public readonly ?string $deliveryHistoryEventId,
        public readonly DateTimeImmutable $authorityDeadline,
        public readonly array $activityMembers,
        public readonly array $history,
    ) {
    }

    /**
     * @param array<string, mixed> $receipt
     * @param array<string, mixed> $expected
     * @return non-empty-string Server-issued cursor for the original claim.
     */
    public static function assertAcknowledgement(array $receipt, array $expected, bool $delivering): string
    {
        foreach ($expected as $key => $value) {
            if (!in_array($key, ['namespace', 'workflow_instance_id'], true)
                && (!array_key_exists($key, $receipt) || $receipt[$key] !== $value)) {
                throw new WorkflowClaimAborted('Scope boundary acknowledgement changes its original claim or authored range.');
            }
        }
        $token = $receipt['history_refresh_page_token'] ?? null;
        if (($receipt['prepared'] ?? null) !== true || ($receipt['delivered'] ?? null) !== $delivering
            || ($receipt['claim_released'] ?? null) !== false || ($receipt['created_task_ids'] ?? null) !== []
            || !array_key_exists('reason', $receipt) || $receipt['reason'] !== null
            || !self::identity($receipt['history_event_id'] ?? null)
            || !self::identity($receipt['preparation_history_event_id'] ?? null)
            || (!$delivering && $receipt['history_event_id'] !== $receipt['preparation_history_event_id'])
            || ($delivering && $receipt['history_event_id'] === $receipt['preparation_history_event_id'])
            || !is_string($token) || trim($token) === '') {
            throw new WorkflowClaimAborted('Scope boundary lacks a complete retained-claim acknowledgement.');
        }
        try {
            CancellationScopeActivityProjection::normalize($receipt['activity_members'] ?? null);
        } catch (InvalidArgumentException $error) {
            throw new WorkflowClaimAborted('Scope receipt has an invalid frozen Activity projection.', previous: $error);
        }
        foreach (['timer_members', 'wait_members', 'child_members'] as $field) {
            if (($receipt[$field] ?? null) !== []) {
                throw new WorkflowClaimAborted('cancellation_scope_execution_not_supported: populated receipt projections are not qualified.');
            }
        }
        try {
            $context = ScopedCancellationContext::fromArray($receipt['cancellation'] ?? []);
            if ($context->workflowRunId !== $expected['workflow_run_id']
                || $context->workflowInstanceId !== $expected['workflow_instance_id']
                || $context->scopeId !== $expected['scope_id'] || $context->requestId !== $expected['request_id']) {
                throw new InvalidArgumentException('Scope receipt changes its original cancellation address.');
            }
            $deadline = self::timestamp($receipt['authority_deadline_at'] ?? null);
            if ($deadline < $context->requestedAt() || $deadline > $context->deadline()) {
                throw new InvalidArgumentException('Scope receipt changes its original authority ceiling.');
            }
        } catch (InvalidArgumentException|\TypeError $error) {
            throw new WorkflowClaimAborted('Scope receipt has invalid cancellation authority.', previous: $error);
        }

        return $token;
    }

    /**
     * @param array<string, mixed> $receipt
     * @param list<mixed> $history Complete original-claim history, with a terminal paging cursor.
     * @param array<string, mixed> $expected
     */
    public static function fromCanonicalHistory(array $receipt, array $history, array $expected, bool $delivering): self
    {
        self::assertAcknowledgement($receipt, $expected, $delivering);
        if (!StickyWorkflowCache::startsWithWorkflowStart($history)) {
            throw new WorkflowClaimAborted('Scope boundary requires complete history from the original workflow start.');
        }
        $starts = [];
        foreach ($history as $event) {
            if (!is_array($event) || array_is_list($event) || ($event['namespace'] ?? null) !== $expected['namespace']
                || !is_string($event['event_type'] ?? $event['type'] ?? null)
                || trim($event['event_type'] ?? $event['type']) === '' || !is_array($event['payload'] ?? null)) {
                throw new WorkflowClaimAborted('Scope boundary history changes its canonical namespace or event shape.');
            }
            $kind = $event['event_type'] ?? $event['type'];
            if (in_array($kind, ['StartAccepted', 'WorkflowStarted'], true)) {
                if (isset($starts[$kind]) || ($event['payload']['workflow_run_id'] ?? null) !== $expected['workflow_run_id']
                    || ($event['payload']['workflow_instance_id'] ?? null) !== $expected['workflow_instance_id']) {
                    throw new WorkflowClaimAborted('Scope boundary history changes its original workflow start.');
                }
                $starts[$kind] = true;
            }
        }
        /** @var list<array<string, mixed>> $history */
        try {
            $scopes = new CancellationScopeHistory($history, $expected['workflow_run_id']);
            $committed = new CommittedCancellationScopeHistory($history, $expected['workflow_run_id'],
                $expected['workflow_instance_id'], $scopes, requireCommittedDelivery: false, inspectActivityProjections: true);
            $prepared = $committed->preparations[$expected['scope_id']] ?? null;
            $context = ScopedCancellationContext::fromArray($receipt['cancellation']);
            $boundary = CancellationDelivery::fromPayload([...$receipt, 'workflow_command_id' => $expected['request_id']]);
            $deadline = self::timestamp($receipt['authority_deadline_at']);
            $activityMembers = CancellationScopeActivityProjection::normalize($receipt['activity_members']);
            if ($prepared === null || $prepared['event']['id'] !== $receipt['preparation_history_event_id']
                || $prepared['context']->toArray() !== $context->toArray() || $prepared['boundary'] != $boundary
                || self::timestamp($prepared['event']['payload']['authority_deadline_at']) != $deadline
                || CancellationScopeActivityProjection::normalize($prepared['event']['payload']['activity_members']) !== $activityMembers) {
                throw new WorkflowClaimAborted('Scope receipt differs from its original committed preparation.');
            }
            $deliveryId = null;
            if ($delivering) {
                $delivery = $committed->deliveries[$boundary->sequence] ?? null;
                if ($delivery === null || $delivery['event']['id'] !== $receipt['history_event_id']
                    || $delivery['context']->toArray() !== $context->toArray() || $delivery['boundary'] != $boundary) {
                    throw new WorkflowClaimAborted('Scope delivery is absent from its original committed boundary.');
                }
                $deliveryId = $receipt['history_event_id'];
            }

            return new self($context, $boundary, $receipt['preparation_history_event_id'], $deliveryId, $deadline, $activityMembers, $history);
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw new WorkflowClaimAborted('Scope boundary canonical authority could not be proved.', previous: $error);
        }
    }

    public function assertOriginalPreparation(self $original): void
    {
        if ($this->preparationHistoryEventId !== $original->preparationHistoryEventId
            || $this->context->toArray() !== $original->context->toArray() || $this->boundary != $original->boundary
            || $this->authorityDeadline != $original->authorityDeadline || $this->activityMembers !== $original->activityMembers) {
            throw new WorkflowClaimAborted('Scope delivery changes its previously verified original preparation.');
        }
    }

    private static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '' && strlen($value) <= 255 && preg_match('//u', $value) === 1;
    }

    private static function timestamp(mixed $value): DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Scope receipt needs a timestamp with timezone.');
        }
        try { $time = new DateTimeImmutable($value); } catch (\Exception $error) {
            throw new InvalidArgumentException('Scope receipt deadline is invalid.', previous: $error);
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Scope receipt deadline is invalid.');
        }

        return $time;
    }
}
