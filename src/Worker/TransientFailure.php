<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use Throwable;

/**
 * @internal Untyped transient failures a managed Worker outlives.
 *
 * The Server or the network was briefly unavailable, and the request is safe
 * to repeat with the same identity:
 * - a connection that could not be made, was reset or timed out;
 * - an upstream 502/503/504 with no protocol body;
 * - a 503 whose reason is `storage_unavailable` or `backend_unavailable`
 *   (for example native Server while its database warms up);
 * - any 5xx or 429 whose body says `retryable: true`.
 *
 * An explicit `retryable: false`, authentication and authorization failures,
 * and other protocol refusals are never transient.
 */
final class TransientFailure
{
    /** Shared by every SDK: 0.25 s doubling to a 4 s cap, with full random jitter. */
    private const FIRST_SECONDS = 0.25;
    private const MAX_SECONDS = 4.0;
    private const REASONS = ['storage_unavailable', 'backend_unavailable'];

    public static function is(Throwable $error): bool
    {
        if ($error instanceof ServerException) {
            if ($error->isTransientConnectionFailure() || $error->isTransientUpstreamFailure()) {
                return true;
            }

            return self::transientResponse($error->status, $error->reason, $error->details);
        }
        if ($error instanceof TransportException) {
            if ($error->transientConnectionFailure) {
                return true;
            }
            $reason = is_string($error->response['reason'] ?? null) ? $error->response['reason'] : null;

            return $error->status !== null && self::transientResponse($error->status, $reason, $error->response);
        }

        return false;
    }

    /** Full-jitter delay for the given 1-based attempt. */
    public static function delay(int $attempt): float
    {
        $ceiling = min(self::MAX_SECONDS, self::FIRST_SECONDS * 2 ** min(max(0, $attempt - 1), 16));

        return $ceiling * random_int(0, 1_000_000) / 1_000_000;
    }

    /** @param array<array-key, mixed>|null $details */
    private static function transientResponse(int $status, ?string $reason, ?array $details): bool
    {
        $retryable = is_array($details) ? ($details['retryable'] ?? null) : null;
        if ($retryable === false || !($status === 429 || ($status >= 500 && $status <= 599))) {
            return false;
        }

        return $retryable === true || ($status === 503 && in_array($reason, self::REASONS, true));
    }
}
