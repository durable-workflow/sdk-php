<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use ReflectionClass;
use ReflectionFunction;
use Throwable;

/** @internal Source identity for worker registration and safe failed-run redrive. */
final class WorkflowDefinitionFingerprint
{
    public static function forHandler(string $workflowType, HandlerDefinition $handler): ?string
    {
        try {
            $reflection = new ReflectionFunction($handler->contract());
            $sources = [];
            $scope = $reflection->getClosureScopeClass();

            if ($reflection->getName() !== '{closure}' && $scope instanceof ReflectionClass) {
                if (!self::collectClassSources($scope, $sources)) {
                    return null;
                }
            } else {
                $source = self::source($reflection);

                if ($source === null) {
                    return null;
                }

                $sources['closure'] = $source;
            }

            ksort($sources);

            return 'sha256:'.hash('sha256', json_encode([
                'domain' => 'durable-workflow-php.workflow-definition.v1',
                'workflow_type' => $workflowType,
                'sources' => $sources,
            ], JSON_THROW_ON_ERROR));
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * @param ReflectionClass<object> $class
     * @param array<string, string> $sources
     */
    private static function collectClassSources(ReflectionClass $class, array &$sources): bool
    {
        if ($class->isInternal() || isset($sources[$class->getName()])) {
            return true;
        }

        $source = self::source($class);

        if ($source === null) {
            return false;
        }

        $sources[$class->getName()] = $source;

        foreach ($class->getTraits() as $trait) {
            if (!self::collectClassSources($trait, $sources)) {
                return false;
            }
        }

        $parent = $class->getParentClass();

        return !($parent instanceof ReflectionClass) || self::collectClassSources($parent, $sources);
    }

    /** @param ReflectionClass<object>|ReflectionFunction $reflection */
    private static function source(ReflectionClass|ReflectionFunction $reflection): ?string
    {
        $file = $reflection->getFileName();
        $start = $reflection->getStartLine();
        $end = $reflection->getEndLine();

        if (!is_string($file) || $file === '' || !is_file($file) || !is_int($start) || !is_int($end) || $end < $start) {
            return null;
        }

        $lines = file($file);

        return is_array($lines) ? implode('', array_slice($lines, $start - 1, $end - $start + 1)) : null;
    }
}
