<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\InvalidWorkerDefinition;
use Psr\Container\ContainerInterface;
use ReflectionClass;
use Throwable;

/** Resolves class-oriented handlers through PSR-11 or a no-container default. */
final class HandlerResolver
{
    public function __construct(private readonly ?ContainerInterface $container = null)
    {
    }

    /** @param class-string $class */
    public function resolve(string $class): object
    {
        if ($this->container !== null) {
            if (!$this->container->has($class)) {
                throw new InvalidWorkerDefinition(
                    $class,
                    "Register {$class} in the PSR-11 container passed to Worker::create().",
                );
            }

            try {
                $handler = $this->container->get($class);
            } catch (Throwable $exception) {
                throw new InvalidWorkerDefinition(
                    $class,
                    "The PSR-11 container could not resolve it: {$exception->getMessage()}",
                );
            }

            if (!$handler instanceof $class) {
                throw new InvalidWorkerDefinition(
                    $class,
                    "The PSR-11 container must return an instance of {$class}.",
                );
            }

            return $handler;
        }

        if (!class_exists($class)) {
            throw new InvalidWorkerDefinition($class, 'Pass an existing handler class or an object instance.');
        }

        $reflection = new ReflectionClass($class);
        if (!$reflection->isInstantiable()) {
            throw new InvalidWorkerDefinition($class, 'Use a concrete, instantiable handler class.');
        }
        $constructor = $reflection->getConstructor();
        if ($constructor !== null && $constructor->getNumberOfRequiredParameters() > 0) {
            throw new InvalidWorkerDefinition(
                $class.'::__construct()',
                'Pass a PSR-11 container to Worker::create() to resolve constructor dependencies, or register an object instance.',
            );
        }

        return $reflection->newInstance();
    }
}
