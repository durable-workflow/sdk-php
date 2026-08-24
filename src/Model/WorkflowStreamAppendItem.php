<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use DurableWorkflow\Codec\PayloadCodec;

/** One typed item supplied to a Workflow Streams append operation. */
final class WorkflowStreamAppendItem
{
    public function __construct(
        public readonly mixed $payload = null,
        public readonly ?string $payloadReference = null,
        public readonly ?string $itemType = null,
        public readonly ?string $contentType = null,
        public readonly ?string $idempotencyKey = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toWire(PayloadCodec $codec, ?string $derivedIdempotencyKey = null): array
    {
        return array_filter([
            'payload' => $this->payload === null ? null : $codec->envelope($this->payload),
            'payload_reference' => $this->payloadReference,
            'payload_codec' => $this->payload === null ? null : $codec->name(),
            'idempotency_key' => $this->idempotencyKey ?? $derivedIdempotencyKey,
            'item_type' => $this->itemType,
            'content_type' => $this->contentType,
        ], static fn (mixed $value): bool => $value !== null);
    }
}
