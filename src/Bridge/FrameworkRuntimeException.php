<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge;

use DurableWorkflow\Exception\ServerException;
use RuntimeException;
use Throwable;

/** Actionable console-facing error for a framework-managed worker. */
final class FrameworkRuntimeException extends RuntimeException
{
    public static function fromThrowable(Throwable $exception): self
    {
        if (!$exception instanceof ServerException) {
            return new self(
                'Durable Workflow worker configuration or handler registration failed: '.$exception->getMessage(),
                0,
                $exception,
            );
        }

        if (in_array($exception->status, [401, 403], true)) {
            $message = 'Durable Workflow authentication failed. Verify the configured shared token or scoped worker token and its namespace access.';
        } elseif (in_array($exception->status, [409, 426], true)
            || str_contains(strtolower((string) $exception->reason), 'protocol')
            || str_contains(strtolower((string) $exception->reason), 'contract')
        ) {
            $message = 'The Durable Workflow worker contract is incompatible with the runtime. Verify the SDK and Server or Cloud compatibility versions before retrying.';
        } elseif ($exception->status === 0) {
            $message = 'The Durable Workflow runtime is unreachable. Verify the endpoint, DNS, TLS, and network access from this worker host.';
        } else {
            $message = "Durable Workflow runtime request failed with HTTP {$exception->status}: {$exception->getMessage()}";
        }

        return new self($message, $exception->status, $exception);
    }
}
