<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use InvalidArgumentException;

/** @internal Canonical selection awaiting claim-bound preparation/delivery, never cleanup authority. */
final class CancellationScopeDeliveryIntent
{
    public function __construct(
        public readonly ScopedCancellationContext $context,
        public readonly CancellationDelivery $boundary,
        public readonly ?string $preparationHistoryEventId = null,
        public readonly ?DateTimeImmutable $authorityDeadline = null,
    ) {
        if ($boundary->requestId !== $context->requestId
            || !in_array($boundary->callKind, ['activity', 'local_activity', 'timer', 'condition', 'child'], true)
            || ($preparationHistoryEventId === null) !== ($authorityDeadline === null)
            || ($preparationHistoryEventId !== null && trim($preparationHistoryEventId) === '')
            || ($authorityDeadline !== null && ($authorityDeadline < $context->requestedAt() || $authorityDeadline > $context->deadline()))) {
            throw new InvalidArgumentException('Scope delivery intent must retain its original request, authored call and preparation authority.');
        }
    }
}
