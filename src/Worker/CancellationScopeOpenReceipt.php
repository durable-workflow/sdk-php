<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** @internal Verified opening identity, not a scope execution capability. */
final class CancellationScopeOpenReceipt
{
    /** @param list<array<string, mixed>> $history */
    private function __construct(
        public readonly string $scopeId,
        public readonly string $historyEventId,
        public readonly int $sequence,
        public readonly string $parentScopeId,
        public readonly bool $shieldParent,
        public readonly bool $duplicate,
        public readonly array $history,
    ) {
    }

    /**
     * @param array<string, mixed> $receipt
     * @param array{task_id: string, workflow_run_id: string, lease_owner: string, workflow_task_attempt: int,
     *     sequence: int, parent_scope_id: string, shield_parent: bool, namespace: string} $expected
     * @return non-empty-string Server-issued opaque cursor, never inferred locally.
     */
    public static function assertAcknowledgement(array $receipt, array $expected): string
    {
        foreach ($expected as $key => $value) {
            if ($key !== 'namespace' && ($receipt[$key] ?? null) !== $value) {
                throw new WorkflowClaimAborted('Scope opening acknowledgement changes its original claim or authored boundary.');
            }
        }
        $scopeId = $receipt['scope_id'] ?? null;
        $eventId = $receipt['history_event_id'] ?? null;
        $token = $receipt['history_refresh_page_token'] ?? null;
        if (($receipt['opened'] ?? null) !== true || !is_bool($receipt['duplicate'] ?? null)
            || ($receipt['claim_released'] ?? null) !== false || ($receipt['created_task_ids'] ?? null) !== []
            || !array_key_exists('reason', $receipt) || $receipt['reason'] !== null
            || !self::isIdentity($scopeId) || $scopeId === 'root' || !self::isIdentity($eventId)
            || !is_string($token) || trim($token) === '') {
            throw new WorkflowClaimAborted('Scope opening lacks a complete retained-claim acknowledgement.');
        }

        return $token;
    }

    /**
     * @param array<string, mixed> $receipt
     * @param list<mixed> $history
     * @param array{task_id: string, workflow_run_id: string, lease_owner: string, workflow_task_attempt: int,
     *     sequence: int, parent_scope_id: string, shield_parent: bool, namespace: string} $expected
     */
    public static function fromCanonicalHistory(array $receipt, array $history, array $expected): self
    {
        self::assertAcknowledgement($receipt, $expected);
        if (!StickyWorkflowCache::startsWithWorkflowStart($history)) {
            throw new WorkflowClaimAborted('Scope opening requires complete history from the original workflow start.');
        }
        $eventIds = [];
        $scopes = [];
        $lastHistorySequence = 0;
        $lastScopeSequence = 0;
        $matching = null;
        foreach ($history as $event) {
            if (!is_array($event) || array_is_list($event) || !self::isIdentity($event['id'] ?? null)
                || isset($eventIds[$event['id']]) || !is_int($event['sequence'] ?? null)
                || $event['sequence'] <= $lastHistorySequence || ($event['namespace'] ?? null) !== $expected['namespace']
                || !is_string($event['event_type'] ?? $event['type'] ?? null)
                || trim($event['event_type'] ?? $event['type']) === '' || !is_array($event['payload'] ?? null)) {
                throw new WorkflowClaimAborted('Scope opening history changes canonical event identity, order or namespace.');
            }
            $eventIds[$event['id']] = true;
            $lastHistorySequence = $event['sequence'];
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'CancellationScopeOpened') {
                continue;
            }
            $payload = $event['payload'];
            if (array_is_list($payload)
                || ($payload['schema'] ?? null) !== 'durable-workflow.cancellation-scope/v1'
                || ($payload['workflow_run_id'] ?? null) !== $expected['workflow_run_id']
                || !self::isIdentity($payload['scope_id'] ?? null) || $payload['scope_id'] === 'root'
                || isset($scopes[$payload['scope_id']]) || !self::isIdentity($payload['parent_scope_id'] ?? null)
                || !is_bool($payload['shield_parent'] ?? null) || !is_int($payload['sequence'] ?? null)
                || $payload['sequence'] <= $lastScopeSequence
                || ($payload['parent_scope_id'] !== 'root' && !isset($scopes[$payload['parent_scope_id']]))) {
                throw new WorkflowClaimAborted('Scope opening history changes its canonical run, parent tree or authored boundary.');
            }
            $scopes[$payload['scope_id']] = true;
            $lastScopeSequence = $payload['sequence'];
            if ($event['id'] === $receipt['history_event_id']) {
                foreach (['scope_id', 'sequence', 'parent_scope_id', 'shield_parent'] as $field) {
                    if ($payload[$field] !== $receipt[$field]) {
                        throw new WorkflowClaimAborted('Scope acknowledgement differs from its committed history event.');
                    }
                }
                $matching = $payload;
            }
        }
        if ($matching === null) {
            throw new WorkflowClaimAborted('Scope opening is absent from the complete original-claim history.');
        }
        /** @var list<array<string, mixed>> $history */
        return new self($matching['scope_id'], $receipt['history_event_id'], $matching['sequence'],
            $matching['parent_scope_id'], $matching['shield_parent'], $receipt['duplicate'], $history);
    }

    private static function isIdentity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '' && strlen($value) <= 255 && preg_match('//u', $value) === 1;
    }
}
