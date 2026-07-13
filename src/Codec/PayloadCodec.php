<?php

declare(strict_types=1);

namespace DurableWorkflow\Codec;

interface PayloadCodec
{
    public function name(): string;

    public function encode(mixed $value): string;

    public function decode(string $blob): mixed;

    /** @return array{codec: string, blob: string} */
    public function envelope(mixed $value): array;

    /** @param array{codec?: mixed, blob?: mixed}|string|null $envelope */
    public function decodeEnvelope(array|string|null $envelope): mixed;
}
