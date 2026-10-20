<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use Throwable;
use WeakMap;

/** Creates an attributed workflow instance while retaining application-owned dependencies. */
final class WorkflowFactory
{
    /** @var Closure(): mixed */
    private readonly Closure $factory;
    /** @var WeakMap<object, true> */
    private readonly WeakMap $instances;
    /** @var class-string|null */
    private ?string $class = null;

    /** @param callable(): mixed $factory */
    public function __construct(callable $factory)
    {
        $this->factory = Closure::fromCallable($factory);
        $this->instances = new WeakMap();
    }

    /** @internal Used by attributed handler discovery and invocation. */
    public function create(): object
    {
        try {
            $instance = ($this->factory)();
        } catch (Throwable $exception) {
            throw new InvalidWorkerDefinition(
                $this->class ?? 'workflow factory',
                'The factory could not construct a handler: '.$exception->getMessage(),
            );
        }
        if (!is_object($instance)) {
            throw new InvalidWorkerDefinition('workflow factory', 'Return a new attributed handler object.');
        }
        $this->class ??= $instance::class;
        if ($instance::class !== $this->class) {
            throw new InvalidWorkerDefinition($this->class, 'Return the same handler class on every factory invocation.');
        }
        if (isset($this->instances[$instance])) {
            throw new InvalidWorkerDefinition(
                $this->class,
                'Create a new workflow instance on every factory invocation. Use a transient workflow container binding, or pass new WorkflowFactory(fn () => new YourWorkflow($sharedDependencies)) to register().',
            );
        }
        $this->instances[$instance] = true;

        return $instance;
    }
}
