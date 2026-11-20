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
 * kept here for later requests, or replaced by the digest string, which is
 * restored here before any caller sees the answer. Older servers ignore the
 * header and answer in full, so callers always observe complete capabilities.
 *
 * The server only ever replaces copies with the digest that request sent, so
 * an answer is resolved against the pair captured when its request was sent,
 * not the pair held when it arrives: overlapping requests across a change of
 * capabilities (a rolling deploy) each restore their own object.
 */
final class ServerCapabilitiesDigest
{
    public const HEADER = 'X-Durable-Workflow-Server-Capabilities-Digest';
    private const MAX_DEPTH = 3;

    private ?string $digest = null;
    /** @var array<string, mixed>|null */
    private ?array $capabilities = null;

    /**
     * The pair a request is sent with; pass it back to restore() for its answer.
     *
     * @return array{0: ?string, 1: array<string, mixed>|null}
     */
    public function capture(): array
    {
        return [$this->digest, $this->capabilities];
    }

    /** Forget the held pair; the next request asks for full capabilities again. */
    public function forget(): void
    {
        $this->digest = null;
        $this->capabilities = null;
    }

    /** @param array{0: ?string, 1: array<string, mixed>|null}|null $captured */
    public function header(?array $captured = null): string
    {
        return ($captured ?? $this->capture())[0] ?? 'none';
    }

    /**
     * Restore elided copies and keep a tagged one, in place.
     *
     * @param array<mixed> $answer
     * @param array{0: ?string, 1: array<string, mixed>|null}|null $captured The pair its request was sent with.
     */
    public function restore(array &$answer, ?array $captured = null): void
    {
        $learned = [null, null];
        $this->restoreLevel($answer, $captured ?? $this->capture(), $learned, 1);
    }

    /**
     * @param array<mixed> $answer
     * @param array{0: ?string, 1: array<string, mixed>|null} $captured
     * @param array{0: ?string, 1: array<string, mixed>|null} $learned A tagged copy seen in this answer.
     * @param-out array{0: ?string, 1: array<string, mixed>|null} $learned
     */
    private function restoreLevel(array &$answer, array $captured, array &$learned, int $depth): void
    {
        if (array_is_list($answer)) {
            return;
        }
        foreach ($answer as $key => &$value) {
            if ($key === 'server_capabilities') {
                if (is_string($value) && str_starts_with($value, 'sha256:')) {
                    $value = match (true) {
                        $value === $captured[0] && $captured[1] !== null => $captured[1],
                        $value === $learned[0] && $learned[1] !== null => $learned[1],
                        default => throw new UnknownServerCapabilitiesDigest(),
                    };
                } elseif (is_array($value) && !array_is_list($value) && is_string($value['digest'] ?? null)
                    && str_starts_with($value['digest'], 'sha256:')) {
                    $digest = $value['digest'];
                    unset($value['digest']);
                    /** @var array<string, mixed> $value */
                    $learned = [$digest, $value];
                    // Later requests send the newest digest this transport has seen.
                    $this->digest = $digest;
                    $this->capabilities = $value;
                }
            } elseif (is_array($value) && $depth < self::MAX_DEPTH) {
                $this->restoreLevel($value, $captured, $learned, $depth + 1);
            }
        }
        unset($value);
    }
}
