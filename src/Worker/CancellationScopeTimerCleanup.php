<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;

/** @internal Original committed authority for explicitly shielded cleanup timers. */
final class CancellationScopeTimerCleanup
{
    /** @param array<string, mixed> $delivery
     * @return array<string, string>
     */
    public static function snapshot(ScopedCancellationContext $context, string $authorityDeadline, array $delivery): array
    {
        $ancestor = ScopedCancellationContext::fromArray($delivery['payload']['cancellation']);
        return [
            'scope_id' => $ancestor->scopeId, 'operation_scope_id' => $context->scopeId,
            'request_id' => $ancestor->requestId, 'root_request_id' => $ancestor->rootRequestId,
            'delivery_history_event_id' => $delivery['id'],
            'preparation_history_event_id' => $delivery['payload']['preparation_history_event_id'],
            'cleanup_deadline_at' => $ancestor->deadline()->format('Y-m-d\TH:i:s.u\Z'),
            'authority_deadline_at' => $authorityDeadline,
        ];
    }

    /** @param array<string, mixed> $event
     * @param list<array<string, mixed>> $prefix Events strictly before the timer admission.
     * @return array<string, string>|null
     */
    public static function fromHistory(array $event, array $prefix): ?array
    {
        $payload = $event['payload'];
        if (!array_key_exists('cancellation_cleanup', $payload)) {
            $scopeId = $payload['cancellation_scope_id'] ?? 'root';
            if ($scopeId === 'root') { return null; }
            foreach ($prefix as $delivery) {
                if (($delivery['event_type'] ?? $delivery['type'] ?? null) !== 'CancellationScopeDelivered') { continue; }
                $preparedId = $delivery['payload']['preparation_history_event_id'];
                foreach ($prefix as $preparation) {
                    if (($preparation['event_type'] ?? $preparation['type'] ?? null) !== 'CancellationScopeDeliveryPrepared'
                        || ($preparation['id'] ?? null) !== $preparedId) { continue; }
                    $frozenScopes = [$delivery['payload']['scope_id'],
                        ...array_column($preparation['payload']['descendant_members'], 'scope_id')];
                    if (in_array($scopeId, $frozenScopes, true)) {
                        throw new InvalidArgumentException('Scope cleanup timer omits its original delivery snapshot.');
                    }
                }
            }
            return null;
        }
        $stored = $payload['cancellation_cleanup'];
        if (!is_array($stored)) {
            throw new InvalidArgumentException('Scope cleanup timer requires its original delivery snapshot.');
        }
        $deliveries = array_values(array_filter($prefix, static fn (array $row): bool =>
            ($row['event_type'] ?? $row['type'] ?? null) === 'CancellationScopeDelivered'
            && ($row['id'] ?? null) === ($stored['delivery_history_event_id'] ?? null)));
        if (count($deliveries) !== 1) {
            throw new InvalidArgumentException('Scope cleanup timer lacks its earlier canonical delivery.');
        }
        $delivery = $deliveries[0];
        $preparations = array_values(array_filter($prefix, static fn (array $row): bool =>
            ($row['event_type'] ?? $row['type'] ?? null) === 'CancellationScopeDeliveryPrepared'
            && ($row['id'] ?? null) === $delivery['payload']['preparation_history_event_id']));
        if (count($preparations) !== 1 || $preparations[0]['sequence'] >= $delivery['sequence']) {
            throw new InvalidArgumentException('Scope cleanup timer lacks its original preparation.');
        }
        $context = ScopedCancellationContext::fromArray($delivery['payload']['cancellation']);
        $authority = $delivery['payload']['authority_deadline_at'];
        $scopeId = $payload['cancellation_scope_id'] ?? null;
        if ($scopeId !== $context->scopeId) {
            $members = array_values(array_filter($preparations[0]['payload']['descendant_members'],
                static fn (array $member): bool => $member['scope_id'] === $scopeId));
            if (count($members) !== 1) {
                throw new InvalidArgumentException('Scope cleanup timer changes its frozen subtree membership.');
            }
            $context = ScopedCancellationContext::fromArray($members[0]['cancellation']);
            $authority = $members[0]['authority_deadline_at'];
        }
        $expected = self::snapshot($context, $authority, $delivery);
        ksort($expected); ksort($stored);
        $sequence = $payload['sequence'] ?? null;
        $deadline = self::timestamp($authority);
        $recorded = self::timestamp($event['timestamp'] ?? $event['recorded_at'] ?? null);
        $fireAt = self::timestamp($payload['fire_at'] ?? null);
        if ($stored !== $expected || $scopeId !== $context->scopeId
            || !is_int($sequence) || $sequence < $delivery['payload']['sequence'] + $delivery['payload']['sequence_span']
            || !is_int($event['sequence'] ?? null) || $delivery['sequence'] >= $event['sequence']
            || $recorded < self::timestamp($delivery['timestamp'] ?? $delivery['recorded_at'] ?? null)
            || $recorded >= $deadline || $fireAt < $recorded || $fireAt >= $deadline
            || ($payload['timer_kind'] ?? null) !== null) {
            throw new InvalidArgumentException('Scope cleanup timer changes its original delivery or authority ceiling.');
        }

        return $expected;
    }

    private static function timestamp(mixed $value): DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Scope cleanup timer requires a valid authority timestamp.');
        }
        try {
            return (new DateTimeImmutable($value))->setTimezone(new DateTimeZone('UTC'));
        } catch (\Exception $error) {
            throw new InvalidArgumentException('Scope cleanup timer requires a valid authority timestamp.', previous: $error);
        }
    }
}
