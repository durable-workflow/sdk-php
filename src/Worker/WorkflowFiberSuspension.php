<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\WorkflowFiberDiscarded;
use Fiber;
use FiberError;

/** @internal The common suspension boundary for workflow commands and handles. */
final class WorkflowFiberSuspension
{
    public static function suspend(mixed $command): mixed
    {
        try {
            return Fiber::suspend($command);
        } catch (FiberError $error) {
            // PHP unwinds finally during disposal but exposes no closing-state
            // query. Only the native error from this call identifies teardown.
            $origin = $error->getTrace()[0] ?? [];
            if ($error->getMessage() !== 'Cannot suspend in a force-closed fiber'
                || ($origin['class'] ?? null) !== Fiber::class
                || $origin['function'] !== 'suspend'
                || ($origin['file'] ?? null) !== __FILE__) {
                throw $error;
            }

            throw new WorkflowFiberDiscarded(previous: $error);
        }
    }
}
