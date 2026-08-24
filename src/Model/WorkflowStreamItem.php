<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** One offset-addressed item read from a Workflow Stream. */
final class WorkflowStreamItem
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly int $offset,
        public readonly mixed $payload,
        public readonly mixed $payloadEnvelope,
        public readonly ?string $payloadReference,
        public readonly ?string $payloadCodec,
        public readonly ?string $idempotencyKey,
        public readonly ?string $origin,
        public readonly ?string $originReference,
        public readonly ?string $itemType,
        public readonly ?string $contentType,
        public readonly ?string $emittedAt,
        public readonly array $raw,
    ) {
    }
}
