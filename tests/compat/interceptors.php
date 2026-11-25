<?php

declare(strict_types=1);

use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Interceptor\ActivityInterceptorContext;
use DurableWorkflow\Worker\Interceptor\PassthroughWorkerInterceptor;

/** A container-owned dependency the configured interceptor must receive. */
final class CompatAuditSink
{
    /** @var list<string> */
    public array $events = [];
}

/** Proves configured interceptor class names are resolved and autowired by the framework container. */
final class CompatAuditInterceptor extends PassthroughWorkerInterceptor
{
    public function __construct(public readonly CompatAuditSink $sink)
    {
    }

    public function executeActivity(ActivityInterceptorContext $context, callable $next): mixed
    {
        $this->sink->events[] = 'activity:'.$context->activityType;

        return $next($context);
    }
}

function verifyConfiguredInterceptors(Worker $worker, CompatAuditSink $sink): void
{
    $chain = (new ReflectionClass($worker))->getProperty('interceptors')->getValue($worker);
    $interceptors = (new ReflectionClass($chain))->getProperty('interceptors')->getValue($chain);
    if (!is_array($interceptors) || count($interceptors) !== 1
        || !$interceptors[0] instanceof CompatAuditInterceptor || $interceptors[0]->sink !== $sink) {
        throw new RuntimeException('Configured worker interceptors were not resolved from the framework container.');
    }
}
