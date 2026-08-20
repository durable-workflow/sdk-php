<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use ReflectionFunction;
use Throwable;

/** @internal Produces the durable identity used to reject changed condition predicates during replay. */
final class ConditionWaitDefinition
{
    public static function fingerprint(Closure $predicate): ?string
    {
        $source = self::source($predicate);

        return $source === null
            ? null
            : 'sha256:'.hash('sha256', $source);
    }

    private static function source(Closure $predicate): ?string
    {
        try {
            $reflection = new ReflectionFunction($predicate);
            $file = $reflection->getFileName();
            $startLine = $reflection->getStartLine();
            $endLine = $reflection->getEndLine();

            if (!is_string($file) || $file === '' || !is_file($file) || $startLine < 1 || $endLine < $startLine) {
                return null;
            }

            $lines = file($file, FILE_IGNORE_NEW_LINES);
            if (!is_array($lines)) {
                return null;
            }

            $source = trim(implode("\n", array_slice($lines, $startLine - 1, $endLine - $startLine + 1)));

            return $source === '' ? null : $source;
        } catch (Throwable) {
            return null;
        }
    }
}
