<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Bridge\ServiceConfiguration;
use InvalidArgumentException;
use ReflectionAttribute;
use ReflectionClass;

/** @internal Resolves configured Laravel workflow services to protocol types. */
final class WorkflowServiceResolver
{
    public function __construct(private readonly ServiceConfiguration $configuration)
    {
    }

    /** @param class-string $workflowService */
    public function workflowType(string $workflowService, ?string $override = null): string
    {
        if (!class_exists($workflowService)) {
            throw new InvalidArgumentException("Laravel workflow service {$workflowService} does not exist.");
        }
        if (!in_array($workflowService, $this->configuration->handlers, true)) {
            throw new InvalidArgumentException(
                "Laravel workflow service {$workflowService} is not registered. Add it to durable-workflow.handlers before starting it.",
            );
        }

        $types = [];
        foreach ((new ReflectionClass($workflowService))->getMethods() as $method) {
            foreach ($method->getAttributes(Workflow::class, ReflectionAttribute::IS_INSTANCEOF) as $attribute) {
                $types[] = $attribute->newInstance()->name;
            }
        }
        if (count($types) !== 1 || !$this->validType($types[0])) {
            throw new InvalidArgumentException(
                "Laravel workflow service {$workflowService} must declare exactly one non-empty #[Workflow] type.",
            );
        }
        if ($override !== null && !$this->validType($override)) {
            throw new InvalidArgumentException(
                'The Laravel workflow type override must be non-empty without surrounding whitespace.',
            );
        }

        return $override ?? $types[0];
    }

    private function validType(string $type): bool
    {
        return $type !== '' && trim($type) === $type;
    }
}
