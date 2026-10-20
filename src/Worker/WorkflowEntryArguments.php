<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Exception\WorkflowEntryArgumentsMismatch;
use ReflectionClass;
use ReflectionFunction;
use ReflectionIntersectionType;
use ReflectionNamedType;
use ReflectionParameter;
use ReflectionType;
use ReflectionUnionType;

/** @internal Match PHP's strict entry invocation without executing the handler. */
final class WorkflowEntryArguments
{
    /** @param list<mixed> $input */
    public static function validate(callable $handler, array $input, string $workflowType): void
    {
        $contract = $handler instanceof HandlerDefinition ? $handler->contract() : Closure::fromCallable($handler);
        $entry = new ReflectionFunction($contract);
        $parameters = $entry->getParameters();
        if (($parameters[0] ?? null)?->isVariadic()) {
            foreach ($input as $value) {
                self::validateType($parameters[0], $value, $entry, $workflowType);
            }

            return;
        }
        // The first parameter is the SDK context, validated at registration.
        foreach (array_slice($parameters, 1) as $index => $parameter) {
            if ($parameter->isVariadic()) {
                foreach (array_slice($input, $index) as $value) {
                    self::validateType($parameter, $value, $entry, $workflowType);
                }

                return;
            }
            if (!array_key_exists($index, $input)) {
                if (!$parameter->isOptional()) {
                    throw new WorkflowEntryArgumentsMismatch($workflowType, 'required_argument_missing', $parameter->getName());
                }
                continue;
            }
            self::validateType($parameter, $input[$index], $entry, $workflowType);
        }
        // PHP permits extra trailing positional arguments. Preserve that contract.
    }

    private static function validateType(ReflectionParameter $parameter, mixed $value, ReflectionFunction $entry, string $workflowType): void
    {
        $type = $parameter->getType();
        if ($type !== null && !self::matches($type, $value, $entry->getClosureScopeClass())) {
            throw new WorkflowEntryArgumentsMismatch($workflowType, 'argument_type_mismatch', $parameter->getName(), (string) $type, get_debug_type($value));
        }
    }

    /** @param ReflectionClass<object>|null $scope */
    private static function matches(ReflectionType $type, mixed $value, ?ReflectionClass $scope): bool
    {
        if ($value === null && $type->allowsNull()) {
            return true;
        }
        if ($type instanceof ReflectionUnionType) {
            foreach ($type->getTypes() as $member) {
                if (self::matches($member, $value, $scope)) {
                    return true;
                }
            }

            return false;
        }
        if ($type instanceof ReflectionIntersectionType) {
            foreach ($type->getTypes() as $member) {
                if (!self::matches($member, $value, $scope)) {
                    return false;
                }
            }

            return true;
        }

        return $type instanceof ReflectionNamedType && self::matchesNamed($type, $value, $scope);
    }

    /** @param ReflectionClass<object>|null $scope */
    private static function matchesNamed(ReflectionNamedType $type, mixed $value, ?ReflectionClass $scope): bool
    {
        if (!$type->isBuiltin()) {
            $parent = $scope?->getParentClass();
            $name = match ($type->getName()) {
                'self', 'static' => $scope?->getName(),
                'parent' => $parent === false ? null : $parent?->getName(),
                default => $type->getName(),
            };

            return is_string($name) && $value instanceof $name;
        }

        return match ($type->getName()) {
            'mixed' => true,
            'int' => is_int($value),
            'float' => is_float($value) || is_int($value),
            'string' => is_string($value),
            'bool' => is_bool($value),
            'true' => $value === true,
            'false' => $value === false,
            'null' => $value === null,
            'array' => is_array($value),
            'object' => is_object($value),
            'callable' => is_callable($value),
            'iterable' => is_iterable($value),
            default => false,
        };
    }
}
