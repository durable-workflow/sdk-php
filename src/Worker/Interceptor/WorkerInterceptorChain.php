<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Exception\WorkflowTaskInterceptorViolation;
use DurableWorkflow\Worker\HandlerResolver;
use Throwable;

/**
 * @internal Runs the Worker's ordered interceptors around its task cores.
 */
final class WorkerInterceptorChain
{
    /** @param list<WorkerInterceptor> $interceptors */
    private function __construct(private readonly array $interceptors)
    {
    }

    /**
     * Resolve interceptor instances and class names, keeping their order.
     * Class names resolve through the PSR-11 container when one is given.
     *
     * @param iterable<WorkerInterceptor|class-string<WorkerInterceptor>> $interceptors
     */
    public static function resolve(iterable $interceptors, HandlerResolver $resolver): self
    {
        $resolved = [];
        foreach ($interceptors as $interceptor) {
            if (is_string($interceptor)) {
                $interceptor = $resolver->resolve($interceptor);
            }
            if (!$interceptor instanceof WorkerInterceptor) {
                throw new InvalidWorkerDefinition(
                    get_debug_type($interceptor),
                    'Worker interceptors must implement '.WorkerInterceptor::class.'.',
                );
            }
            $resolved[] = $interceptor;
        }

        return new self($resolved);
    }

    public function isEmpty(): bool
    {
        return $this->interceptors === [];
    }

    /**
     * Run the workflow-task core inside the chain and return the core's own commands.
     *
     * Hooks see a copy of the commands. A core failure is rethrown unchanged even
     * if a hook catches it; a hook failure after the core succeeded fails the task;
     * a hook that skips or repeats `$next` fails the task. None of them can
     * change the commands the Worker submits.
     *
     * @param \Closure(): WorkflowTaskInterceptorContext $context built only when interceptors are configured
     * @param \Closure(): list<array<string, mixed>> $core
     * @return list<array<string, mixed>>
     */
    public function workflowTask(\Closure $context, \Closure $core): array
    {
        if ($this->interceptors === []) {
            return $core();
        }
        $context = $context();
        $run = new class () {
            public int $calls = 0;
            /** @var list<array<string, mixed>>|null */
            public ?array $commands = null;
            public ?Throwable $coreFailure = null;
            public ?WorkflowTaskInterceptorViolation $violation = null;
        };
        $terminal = static function () use ($core, $run): WorkflowTaskOutcome {
            if (++$run->calls > 1) {
                throw $run->violation ??= new WorkflowTaskInterceptorViolation('', 'A workflow task interceptor called $next more than once.');
            }
            try {
                $run->commands = $core();
            } catch (Throwable $failure) {
                $run->coreFailure = $failure;

                throw $failure;
            }

            return new WorkflowTaskOutcome($run->commands);
        };
        $handler = $this->wrap($terminal, static fn (WorkerInterceptor $interceptor, WorkflowTaskInterceptorContext $ctx, callable $next): WorkflowTaskOutcome
            => self::guardWorkflowHook($interceptor, $ctx, $next, $run));
        try {
            $handler($context);
        } catch (Throwable $failure) {
            throw $run->coreFailure ?? $run->violation ?? $failure;
        }
        // A violation stays fatal even when an outer hook caught it and returned.
        if ($run->coreFailure !== null) {
            throw $run->coreFailure;
        }
        if ($run->violation !== null) {
            throw $run->violation;
        }
        if ($run->commands === null) {
            throw new WorkflowTaskInterceptorViolation('', 'A workflow task interceptor returned without calling $next.');
        }

        return $run->commands;
    }

    /**
     * @param \Closure(ActivityInterceptorContext): mixed $core
     */
    public function activity(ActivityInterceptorContext $context, \Closure $core): mixed
    {
        if ($this->interceptors === []) {
            return $core($context);
        }

        return $this->wrap($core, static fn (WorkerInterceptor $interceptor, ActivityInterceptorContext $ctx, callable $next): mixed
            => $interceptor->executeActivity($ctx, $next))($context);
    }

    /**
     * @param \Closure(QueryTaskInterceptorContext): mixed $core
     */
    public function queryTask(QueryTaskInterceptorContext $context, \Closure $core): mixed
    {
        if ($this->interceptors === []) {
            return $core($context);
        }

        return $this->wrap($core, static fn (WorkerInterceptor $interceptor, QueryTaskInterceptorContext $ctx, callable $next): mixed
            => $interceptor->executeQueryTask($ctx, $next))($context);
    }

    /**
     * The workflow core always runs with the Worker's own context, so a hook
     * cannot swap the task it executes. Each hook must call `$next` exactly once.
     */
    private static function guardWorkflowHook(WorkerInterceptor $interceptor, WorkflowTaskInterceptorContext $context, callable $next, object $run): WorkflowTaskOutcome
    {
        $called = false;
        $outcome = $interceptor->executeWorkflowTask($context, static function () use ($interceptor, $context, $next, $run, &$called): WorkflowTaskOutcome {
            if ($called) {
                throw $run->violation ??= new WorkflowTaskInterceptorViolation($interceptor::class,
                    'Workflow task interceptor '.$interceptor::class.' called $next more than once.');
            }
            $called = true;

            return $next($context);
        });
        if (!$called) {
            throw $run->violation ??= new WorkflowTaskInterceptorViolation($interceptor::class,
                'Workflow task interceptor '.$interceptor::class.' returned without calling $next.');
        }

        return $outcome;
    }

    /**
     * Build the handler chain; the first interceptor is the outermost wrapper.
     *
     * @param \Closure(object): mixed $core
     * @param \Closure(WorkerInterceptor, object, callable(object): mixed): mixed $invoke
     * @return \Closure(object): mixed
     */
    private function wrap(\Closure $core, \Closure $invoke): \Closure
    {
        $handler = $core;
        foreach (array_reverse($this->interceptors) as $interceptor) {
            $next = $handler;
            $handler = static fn (object $ctx): mixed => $invoke($interceptor, $ctx, $next);
        }

        return $handler;
    }
}
