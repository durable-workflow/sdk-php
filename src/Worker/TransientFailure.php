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
 * - a generic Server error: HTTP 500, 502, 503 or 504 with any body that
 *   names no reason, or only the generic `server_error` reason (for example
 *   native Server while a database failover makes runtime credentials
 *   briefly unreadable, cloud#752). This applies to acknowledgements too: the
 *   Worker repeats them with the same task, attempt and lease, so the Server's
 *   lease fence decides a request it may already have committed, exactly as
 *   for a reset connection;
 * - any other 5xx or 429 whose body says `retryable: true` and names no other
 *   reason. This one does not apply to acknowledgements (completions).
 *
 * An explicit `retryable: false`, authentication and authorization failures,
 * and other protocol refusals are never transient. A typed refusal (one that
 * names its own reason, such as `backend_lock_pressure`) keeps its protocol
 * classification, including its validation of `retry_after_seconds`.
 */
final class TransientFailure
{
    /** Shared by every SDK: 0.25 s doubling to a 4 s cap, with full random jitter. */
    private const FIRST_SECONDS = 0.25;
    private const MAX_SECONDS = 4.0;
    private const REASONS = ['storage_unavailable', 'backend_unavailable'];
    /** Generic Server errors: an outage, not a refusal of this request. */
    private const SERVER_ERROR_STATUSES = [500, 502, 503, 504];
    private const SERVER_ERROR_REASONS = [null, '', 'server_error'];
    /** The longest Server-suggested delay honoured for an untyped outage. */
    private const MAX_RETRY_AFTER_SECONDS = 60;

    public static function is(Throwable $error, bool $acknowledgement = false): bool
    {
        if ($error instanceof ServerException && $error->isPayloadDiscoveryUnavailable()) {
            // Discovery failed before the request was sent: repeating it is safe.
            return true;
        }
        if ($error instanceof ServerException) {
            if ($error->isTransientConnectionFailure() || $error->isTransientUpstreamFailure()) {
                return true;
            }

            return self::transientResponse($error->status, $error->reason, $error->details, $acknowledgement);
        }
        if ($error instanceof TransportException) {
            if ($error->transientConnectionFailure) {
                return true;
            }
            $reason = is_string($error->response['reason'] ?? null) ? $error->response['reason'] : null;

            return $error->status !== null && self::transientResponse($error->status, $reason, $error->response, $acknowledgement);
        }

        return false;
    }

    /**
     * An outage of a read-only request (cluster info), which is always safe to
     * repeat: a connection that could not be made, was reset or timed out, or
     * any 5xx or 429 answer unless it says `retryable: false`. Authentication,
     * authorization and other 4xx refusals are not outages.
     */
    public static function isReadOutage(Throwable $error): bool
    {
        if ($error instanceof ServerException) {
            if ($error->isTransientConnectionFailure() || $error->isPayloadDiscoveryUnavailable()) {
                return true;
            }
            $status = $error->status;
            $details = $error->details;
        } elseif ($error instanceof TransportException) {
            if ($error->transientConnectionFailure) {
                return true;
            }
            $status = $error->status;
            $details = $error->response;
        } else {
            return false;
        }

        return $status !== null && ($status === 429 || ($status >= 500 && $status <= 599))
            && (!is_array($details) || ($details['retryable'] ?? null) !== false);
    }

    /**
     * The Server's suggested wait for an untyped outage, in whole seconds,
     * capped at 60: the body's `retry_after_seconds`, else a `Retry-After`
     * header as delta-seconds or an HTTP-date (read against $now, Unix
     * seconds). Null when neither gives a positive wait.
     */
    public static function retryAfterSeconds(Throwable $error, float $now): ?int
    {
        $details = $error instanceof ServerException ? $error->details
            : ($error instanceof TransportException ? $error->response : null);
        $seconds = is_array($details) ? ($details['retry_after_seconds'] ?? null) : null;
        if (!is_int($seconds) || $seconds <= 0) {
            $seconds = null;
            for ($cause = $error; $cause !== null && $seconds === null; $cause = $cause->getPrevious()) {
                if ($cause instanceof TransportException) {
                    $seconds = self::retryAfterHeader($cause->headers, $now);
                }
            }
        }

        return $seconds === null ? null : min($seconds, self::MAX_RETRY_AFTER_SECONDS);
    }

    /** @param array<string, string> $headers */
    private static function retryAfterHeader(array $headers, float $now): ?int
    {
        $value = null;
        foreach ($headers as $name => $header) {
            if (strtolower((string) $name) === 'retry-after') {
                $value = trim($header);
            }
        }
        if ($value === null || $value === '') {
            return null;
        }
        if (preg_match('/\A\d{1,10}\z/', $value) === 1) {
            $seconds = (int) $value;

            return $seconds > 0 ? $seconds : null;
        }
        // RFC 9110 HTTP-date in its preferred IMF-fixdate form, always GMT.
        $at = \DateTimeImmutable::createFromFormat('!D, d M Y H:i:s \\G\\M\\T', $value, new \DateTimeZone('UTC'));
        $errors = \DateTimeImmutable::getLastErrors();
        if ($at === false || ($errors !== false && ($errors['warning_count'] > 0 || $errors['error_count'] > 0))) {
            return null;
        }
        $seconds = (int) ceil($at->getTimestamp() - $now);

        return $seconds > 0 ? $seconds : null;
    }

    /** Full-jitter delay for the given 1-based attempt, never shorter than a bounded retry_after. */
    public static function delay(int $attempt, mixed $retryAfterSeconds = null): float
    {
        $ceiling = min(self::MAX_SECONDS, self::FIRST_SECONDS * 2 ** min(max(0, $attempt - 1), 16));
        $delay = $ceiling * random_int(0, 1_000_000) / 1_000_000;

        return is_int($retryAfterSeconds) && $retryAfterSeconds > 0
            ? max($delay, (float) min($retryAfterSeconds, self::MAX_RETRY_AFTER_SECONDS)) : $delay;
    }

    /** @param array<array-key, mixed>|null $details */
    private static function transientResponse(int $status, ?string $reason, ?array $details, bool $acknowledgement): bool
    {
        $retryable = is_array($details) ? ($details['retryable'] ?? null) : null;
        if ($retryable === false || !($status === 429 || ($status >= 500 && $status <= 599))) {
            return false;
        }

        if ($status === 503 && in_array($reason, self::REASONS, true)) {
            return true;
        }
        if (in_array($status, self::SERVER_ERROR_STATUSES, true) && in_array($reason, self::SERVER_ERROR_REASONS, true)) {
            return true;
        }

        return !$acknowledgement && $retryable === true && ($reason === null || $reason === '');
    }
}
