<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\WorkflowCancelled;
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

    /** @var \WeakMap<WorkflowTimeoutCleanupRequested, true>|null Interruptions the runtime raised into workflow code. */
    private static ?\WeakMap $delivered = null;

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
     * @internal Records an interruption the runtime raises into workflow code,
     * so only that exact object can carry the marker.
     *
     * @template T of WorkflowCancelled
     * @param T $interruption
     * @return T
     */
    public static function delivered(WorkflowCancelled $interruption): WorkflowCancelled
    {
        if ($interruption instanceof WorkflowTimeoutCleanupRequested) {
            self::$delivered ??= new \WeakMap();
            self::$delivered[$interruption] = true;
        }

        return $interruption;
    }

    /**
     * The portable failure marker for an uncaught timeout-cleanup interruption,
     * or null for any other failure. Only the delivered interruption for the
     * run's root request is marked: the exact object the runtime raised, whose
     * context has `cause: timeout`, no parent request and no scope, and names
     * this run (`$runId`, the claimed task's run; without it nothing is
     * marked). An exception the application constructs (even with a copied
     * context), a wrapped interruption and a scoped child are not marked.
     *
     * @return array{interruption: string}|null
     */
    public static function failureMarker(Throwable $failure, ?string $runId = null): ?array
    {
        if (!$failure instanceof WorkflowTimeoutCleanupRequested || self::$delivered === null || !isset(self::$delivered[$failure])) {
            return null;
        }
        $context = $failure->context;
        if (!$context instanceof CancellationContext || !$context->isTimeout() || $context->scopeOrigin !== null
            || $context->parentRequestId !== null || $failure->requestId === null
            || $failure->requestId !== $context->requestId || $context->requestId !== $context->rootRequestId
            || $runId === null || $context->rootWorkflowRunId !== $runId) {
            return null;
        }

        return ['interruption' => self::INTERRUPTION];
    }

    private function __construct()
    {
    }
}
