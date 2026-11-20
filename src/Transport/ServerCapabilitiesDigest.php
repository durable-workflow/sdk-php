<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;

/**
 * @internal Worker answers without repeated `server_capabilities`.
 *
 * A worker request sends the digest of the capabilities this transport holds
 * (or `none`). A server that supports the digest answers with each copy of
 * its current capabilities either tagged with a `digest` member, which is
 * kept here, or replaced by the digest string, which is restored here before
 * any caller sees the answer. Older servers ignore the header and answer in
 * full, so callers always observe complete capabilities.
 */
final class ServerCapabilitiesDigest
{
    public const HEADER = 'X-Durable-Workflow-Server-Capabilities-Digest';
    private const MAX_DEPTH = 3;

    private ?string $digest = null;
    /** @var array<string, mixed>|null */
    private ?array $capabilities = null;

    public function header(): string
    {
        return $this->digest ?? 'none';
    }

    /**
     * Restore elided copies and keep a tagged one, in place.
     *
     * @param array<mixed> $answer
     */
    public function restore(array &$answer, int $depth = 1): void
    {
        if (array_is_list($answer)) {
            return;
        }
        foreach ($answer as $key => &$value) {
            if ($key === 'server_capabilities') {
                if (is_string($value) && str_starts_with($value, 'sha256:')) {
                    if ($value !== $this->digest || $this->capabilities === null) {
                        throw new TransportException('The server omitted capabilities this worker does not hold.');
                    }
                    $value = $this->capabilities;
                } elseif (is_array($value) && !array_is_list($value) && is_string($value['digest'] ?? null)
                    && str_starts_with($value['digest'], 'sha256:')) {
                    $digest = $value['digest'];
                    unset($value['digest']);
                    $this->digest = $digest;
                    $this->capabilities = $value;
                }
            } elseif (is_array($value) && $depth < self::MAX_DEPTH) {
                $this->restore($value, $depth + 1);
            }
        }
        unset($value);
    }
}
