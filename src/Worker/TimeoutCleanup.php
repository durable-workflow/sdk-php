<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\WorkflowTimeoutCleanupRequested;
use DurableWorkflow\Version;
use Throwable;

/**
 * @internal Worker side of deadline-triggered timeout cleanup (server#1201, workflow#863).
 *
 * A worker that delivers cooperative cancellation advertises `timeout_cleanup`
 * version 1, so the Server admits starts that reserve cleanup time on its
 * queue. When the engine's timeout request reaches workflow code as
 * {@see WorkflowTimeoutCleanupRequested} and the workflow lets it escape, the
 * `fail_workflow` command carries `failure.interruption: "timeout_cleanup"`.
 * The Server then records the cleanup as completed, and the run still closes
 * as timed out. Any other failure, including one that wraps the interruption,
 * is reported as an ordinary failure, which the Server records as failed
 * cleanup. The marker never starts cleanup; the Server accepts it only while
 * the run's own engine timeout request is active.
 */
final class TimeoutCleanup
{
    public const CAPABILITY = 'timeout_cleanup';
    public const INTERRUPTION = 'timeout_cleanup';

    /** @return array{supported: true, minimum_protocol_version: string, version: 1} */
    public static function manifestEntry(): array
    {
        return [
            'supported' => true,
            'minimum_protocol_version' => Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
            'version' => 1,
        ];
    }

    /**
     * The portable failure marker for an uncaught timeout-cleanup interruption,
     * or null for any other failure. The delivered exception carries the
     * engine's root cancellation context with `cause: timeout`; an
     * application-constructed exception without that context is not marked.
     *
     * @return array{interruption: string}|null
     */
    public static function failureMarker(Throwable $failure): ?array
    {
        if (!$failure instanceof WorkflowTimeoutCleanupRequested) {
            return null;
        }
        $context = $failure->context;
        if (!$context instanceof CancellationContext || !$context->isTimeout()
            || $failure->requestId === null || $failure->requestId !== $context->requestId) {
            return null;
        }

        return ['interruption' => self::INTERRUPTION];
    }

    private function __construct()
    {
    }
}
