<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\WorkflowFiberDiscarded;
use Fiber;
use FiberError;
use ReflectionFiber;

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

    /**
     * @param Fiber<mixed, mixed, mixed, mixed> $fiber
     * @return array{function: string, file: string, line: int}|null
     */
    public static function workflowSource(Fiber $fiber): ?array
    {
        if (!$fiber->isSuspended()) {
            return null;
        }

        $trace = (new ReflectionFiber($fiber))->getTrace(DEBUG_BACKTRACE_IGNORE_ARGS);
        $sdkDirectory = str_replace('\\', '/', dirname(__DIR__)).'/';
        foreach ($trace as $index => $frame) {
            // A frame gives the location of the call. The next frame names its caller.
            $source = self::sourceFromFrame($frame, $trace[$index + 1] ?? [], $sdkDirectory);
            if ($source !== null) {
                return $source;
            }
        }

        return null;
    }

    /**
     * @param array<string, mixed> $frame
     * @param array<string, mixed> $caller
     * @return array{function: string, file: string, line: int}|null
     */
    private static function sourceFromFrame(array $frame, array $caller, string $sdkDirectory): ?array
    {
        $file = $frame['file'] ?? null;
        $line = $frame['line'] ?? 0;
        if (!is_string($file) || !is_int($line) || $line < 1) {
            return null;
        }
        $file = str_replace('\\', '/', $file);
        if (str_starts_with($file, $sdkDirectory)) {
            return null;
        }
        $file = basename($file);
        if (!str_ends_with($file, '.php') || preg_match('/[[:cntrl:]]/', $file) !== 0) {
            return null;
        }

        $function = $caller['function'] ?? '';
        if (!is_string($function) || $function === '') {
            return null;
        }
        if (str_contains($function, '{closure')) {
            $function = strstr($function, '{closure', true).'{closure}';
        }
        $class = $caller['class'] ?? '';
        if (!is_string($class) || str_contains($class, '@anonymous')) {
            $class = '{anonymous class}';
        }

        return ['function' => $class === '' ? $function : $class.'::'.$function, 'file' => $file, 'line' => $line];
    }
}
