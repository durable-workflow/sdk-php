<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use ReflectionClass;
use ReflectionMethod;
use ReflectionNamedType;

/** @internal Reflects and validates attribute-based handler classes. */
final class HandlerDiscovery
{
    private const ATTRIBUTES = [
        Workflow::class,
        Activity::class,
        Query::class,
        Signal::class,
        Update::class,
    ];

    public function __construct(private readonly HandlerResolver $resolver)
    {
    }

    /** @param class-string|object $service */
    public function discover(string|object $service): DiscoveredHandlers
    {
        $factory = $service instanceof WorkflowFactory ? $service : (
            is_string($service) ? new WorkflowFactory(fn (): object => $this->resolver->resolve($service)) : null
        );
        $handler = $factory === null ? $service : $factory->create();
        $reflection = new ReflectionClass($handler);
        $annotated = [];
        $workflowMethods = [];

        foreach ($reflection->getMethods() as $method) {
            $attributes = array_values(array_filter(
                $method->getAttributes(),
                static fn (\ReflectionAttribute $attribute): bool => in_array(
                    $attribute->getName(),
                    self::ATTRIBUTES,
                    true,
                ),
            ));
            if ($attributes === []) {
                continue;
            }
            if (count($attributes) !== 1) {
                throw $this->invalid(
                    $method,
                    'Apply exactly one Durable Workflow handler attribute to the method.',
                );
            }
            if (!$method->isPublic() || $method->isStatic()) {
                throw $this->invalid($method, 'Make the attributed handler a public, non-static method.');
            }

            $attribute = $attributes[0]->newInstance();
            $annotated[] = [$method, $attribute];
            if ($attribute instanceof Workflow) {
                $workflowMethods[] = $method;
            }
        }

        if ($annotated === []) {
            throw new InvalidWorkerDefinition(
                $reflection->getName(),
                'Add #[Workflow], #[Activity], #[Query], #[Signal], or #[Update] to a public handler method.',
            );
        }
        if (count($workflowMethods) > 1) {
            throw new InvalidWorkerDefinition(
                $reflection->getName(),
                'Define one #[Workflow] entry method per workflow class; split additional workflows into separate classes.',
            );
        }

        if ($workflowMethods !== []) {
            if ($factory === null) {
                throw new InvalidWorkerDefinition(
                    $reflection->getName(),
                    'Register the workflow class or a WorkflowFactory that creates a new workflow instance. An existing object cannot describe how to reconstruct its execution-local state and injected dependencies.',
                );
            }
            // Detect a singleton binding or invalid factory before registering or polling work.
            $factory->create();
        }

        $workflowType = null;
        $workflows = [];
        $activities = [];
        $queries = [];
        $signals = [];
        $updates = [];

        foreach ($annotated as [$method, $attribute]) {
            if ($attribute instanceof Workflow) {
                $workflowType = $this->name($attribute->name, $method, 'workflow');
                $this->assertContext($method, WorkflowContext::class, 'workflow');
                $workflows[$workflowType] = HandlerDefinition::replaySafe(
                    $handler,
                    $method->getName(),
                    $factory,
                );
            } elseif ($attribute instanceof Activity) {
                $name = $this->name($attribute->name ?? $method->getName(), $method, 'activity');
                $this->assertContext($method, ActivityContext::class, 'activity');
                $this->assertLocalUnique($activities, $name, $method, 'activity');
                $activities[$name] = HandlerDefinition::shared([$handler, $method->getName()]);
            }
        }

        foreach ($annotated as [$method, $attribute]) {
            if (!$attribute instanceof Query && !$attribute instanceof Signal && !$attribute instanceof Update) {
                continue;
            }
            if ($workflowType === null) {
                throw $this->invalid(
                    $method,
                    'Put workflow query, signal, and update contracts on the same class as its #[Workflow] entry method.',
                );
            }

            $name = $this->name($attribute->name ?? $method->getName(), $method, strtolower((new ReflectionClass($attribute))->getShortName()));
            if ($attribute instanceof Query) {
                $this->assertContext($method, QueryContext::class, 'query');
                $this->assertLocalUnique($queries[$workflowType] ?? [], $name, $method, 'query');
                $queries[$workflowType][$name] = HandlerDefinition::replaySafe(
                    $handler,
                    $method->getName(),
                    $factory,
                );
            } elseif ($attribute instanceof Signal) {
                $this->assertNotContextual($method, 'signal');
                $this->assertLocalUnique($signals[$workflowType] ?? [], $name, $method, 'signal');
                $signals[$workflowType][$name] = [$handler, $method->getName()];
            } else {
                $this->assertContext($method, QueryContext::class, 'update');
                $this->assertLocalUnique($updates[$workflowType] ?? [], $name, $method, 'update');
                $updates[$workflowType][$name] = HandlerDefinition::replaySafe(
                    $handler,
                    $method->getName(),
                    $factory,
                );
            }
        }

        return new DiscoveredHandlers(
            $reflection->getName(),
            $workflows,
            $activities,
            $queries,
            $signals,
            $updates,
        );
    }

    private function assertContext(ReflectionMethod $method, string $context, string $kind): void
    {
        $parameter = $method->getParameters()[0] ?? null;
        $type = $parameter?->getType();
        if ($type instanceof ReflectionNamedType && !$type->isBuiltin() && $type->getName() === $context) {
            return;
        }

        throw $this->invalid(
            $method,
            "Make the first {$kind} parameter {$context}; workflow input follows that context parameter.",
        );
    }

    private function assertNotContextual(ReflectionMethod $method, string $kind): void
    {
        $type = ($method->getParameters()[0] ?? null)?->getType();
        if (!$type instanceof ReflectionNamedType || $type->isBuiltin()) {
            return;
        }
        if (!in_array($type->getName(), [WorkflowContext::class, ActivityContext::class, QueryContext::class], true)) {
            return;
        }

        throw $this->invalid(
            $method,
            "Remove the context parameter from the {$kind} declaration; its parameters describe only the accepted arguments.",
        );
    }

    /** @param array<string, mixed> $handlers */
    private function assertLocalUnique(
        array $handlers,
        string $name,
        ReflectionMethod $method,
        string $kind,
    ): void {
        if (isset($handlers[$name])) {
            throw $this->invalid($method, "Give every {$kind} contract in the class a unique name.");
        }
    }

    private function name(string $name, ReflectionMethod $method, string $kind): string
    {
        if ($name === '' || trim($name) !== $name) {
            throw $this->invalid($method, "Give the {$kind} contract a non-empty name without surrounding whitespace.");
        }

        return $name;
    }

    private function invalid(ReflectionMethod $method, string $remediation): InvalidWorkerDefinition
    {
        return new InvalidWorkerDefinition(
            $method->getDeclaringClass()->getName().'::'.$method->getName().'()',
            $remediation,
        );
    }
}
