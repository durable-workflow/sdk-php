<?php

declare(strict_types=1);

namespace DurableWorkflow\Telemetry;

use Closure;
use Composer\InstalledVersions;
use Fiber;
use OpenTelemetry\API\Globals;
use OpenTelemetry\API\Instrumentation\Configurator;
use OpenTelemetry\API\Trace\NoopTracerProvider;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerProviderInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\Context\ContextInterface;
use OpenTelemetry\Context\ContextKeyInterface;
use OpenTelemetry\Context\ScopeInterface;
use Throwable;
use WeakMap;

/** Optional execution instrumentation using application-owned OpenTelemetry providers. */
final class ExecutionTelemetry
{
    private const FIELDS = [
        'workflow_instance_id', 'workflow_run_id', 'workflow_type', 'namespace',
        'task_id', 'activity_id', 'activity_type', 'activity_attempt_id',
        'activity_attempt_number', 'request_id', 'task_queue',
    ];

    /** @var ContextKeyInterface<array<string, string|int>>|null */
    private static ?ContextKeyInterface $key = null;
    /** @var WeakMap<object, int>|null */
    private static ?WeakMap $fibers = null;
    private readonly int $ownerPid;
    private readonly ?TracerProviderInterface $provider;
    private readonly ?ContextInterface $applicationContext;

    /**
     * A cooperative callback must create its own provider after fork. Use a
     * synchronous processor and bounded exporter: child shutdown hooks do not run.
     * Without a factory, child tracing is disabled rather than reusing sockets.
     *
     * @param (Closure(): TracerProviderInterface)|null $childProviderFactory
     */
    public function __construct(
        ?TracerProviderInterface $tracerProvider = null,
        private readonly bool $enabled = true,
        private readonly ?Closure $childProviderFactory = null,
    ) {
        $this->ownerPid = getmypid();
        $provider = null;
        $applicationContext = null;
        try {
            if ($enabled && class_exists(Globals::class)) {
                $provider = $tracerProvider ?? Globals::tracerProvider();
                // Capture providers when the worker is configured, without capturing its caller's span or baggage.
                $applicationContext = Configurator::create()->withTracerProvider($provider)
                    ->withMeterProvider(Globals::meterProvider())->withLoggerProvider(Globals::loggerProvider())
                    ->withPropagator(Globals::propagator())->withResponsePropagator(Globals::responsePropagator())
                    ->storeInContext(Context::getRoot());
            }
        } catch (Throwable) {
            // Optional provider discovery must not prevent constructing a worker.
        }
        $this->provider = $provider;
        $this->applicationContext = $applicationContext;
    }

    /**
     * Uses only existing poll identities. Durable wire carriers are not adopted here.
     *
     * @param array<string, mixed> $task
     */
    public function task(string $kind, array $task, string $namespace, string $queue, callable $operation): mixed
    {
        $fields = self::taskFields($task, $namespace, $queue);
        if ($kind !== 'activity') {
            // Workflow resume metadata may describe an earlier activity, not this callback.
            unset($fields['activity_id'], $fields['activity_type'], $fields['activity_attempt_id'], $fields['activity_attempt_number']);
        }

        return $this->run('durable.'.$kind.'.task', $fields, $operation);
    }

    /** @param array<string, mixed> $fields */
    public function activity(array $fields, callable $operation, bool $local = false): mixed
    {
        return $this->run($local ? 'durable.local_activity.attempt' : 'durable.activity.attempt',
            $fields + self::current(), $operation, inherit: true);
    }

    /** @return array<string, string|int> */
    public static function current(): array
    {
        if (self::$key === null || !class_exists(Context::class)) {
            return [];
        }
        $fiber = Fiber::getCurrent();
        if ($fiber !== null && !isset(self::$fibers[$fiber])) {
            return [];
        }
        try {
            $fields = Context::getCurrent()->get(self::$key);
            if (!is_array($fields)) {
                return [];
            }
            $span = Span::getCurrent()->getContext();
            if ($span->isValid()) {
                $fields['trace_id'] = $span->getTraceId();
                $fields['span_id'] = $span->getSpanId();
            }

            return $fields;
        } catch (Throwable) {
            return [];
        }
    }

    /** Bind only SDK-created workflow Fibers; unrelated Fibers inherit no durable identities. */
    public static function bind(callable $operation): callable
    {
        if (self::current() === []) {
            return $operation;
        }
        try {
            $context = Context::getCurrent();
        } catch (Throwable) {
            return $operation;
        }

        return static function (mixed ...$arguments) use ($operation, $context): mixed {
            $scope = null;
            try {
                $scope = $context->activate();
                self::enterFiber();
            } catch (Throwable) {
                // Instrumentation setup cannot prevent the callback from running.
            }
            try {
                return $operation(...$arguments);
            } finally {
                self::close($scope, null);
            }
        };
    }

    /** Record a failure already classified by the worker, without exception messages or stack arguments. */
    public static function failure(Throwable $failure): void
    {
        if (self::current() === []) {
            return;
        }
        try {
            Span::getCurrent()->setStatus(StatusCode::STATUS_ERROR);
            Span::getCurrent()->setAttribute('error.type', $failure::class);
        } catch (Throwable) {
            // The original failure and its durable handling remain authoritative.
        }
    }

    /** @param array<string, mixed> $task
     *  @return array<string, mixed>
     */
    public static function taskFields(array $task, string $namespace, string $queue): array
    {
        return [
            'workflow_instance_id' => $task['workflow_id'] ?? null,
            'workflow_run_id' => $task['run_id'] ?? null,
            'workflow_type' => $task['workflow_type'] ?? null,
            'namespace' => $namespace,
            'task_id' => $task['task_id'] ?? $task['query_task_id'] ?? null,
            'activity_id' => $task['activity_execution_id'] ?? null,
            'activity_type' => $task['activity_type'] ?? null,
            'activity_attempt_id' => $task['activity_attempt_id'] ?? $task['attempt_id'] ?? null,
            'activity_attempt_number' => $task['attempt_number'] ?? null,
            'request_id' => $task['request_id'] ?? null,
            'task_queue' => $queue,
        ];
    }

    /** @param array<string, mixed> $attributes */
    private function run(string $name, array $attributes, callable $operation, bool $inherit = false): mixed
    {
        if (!$this->enabled || $this->provider === null || $this->applicationContext === null) {
            return $operation();
        }
        $span = null;
        $scope = null;
        $childShield = null;
        try {
            $child = getmypid() !== $this->ownerPid;
            $provider = $this->provider;
            $parent = $inherit ? Context::getCurrent() : $this->applicationContext;
            if ($child) {
                // Shield even setup failures from inherited exporter connections.
                $safe = Configurator::create()->withTracerProvider(new NoopTracerProvider())
                    ->withMeterProvider(new \OpenTelemetry\API\Metrics\Noop\NoopMeterProvider())
                    ->withLoggerProvider(\OpenTelemetry\API\Logs\NoopLoggerProvider::getInstance())
                    ->storeInContext($this->applicationContext);
                $childShield = $safe->activate();
                self::enterFiber();
                $provider = new NoopTracerProvider();
                if ($this->childProviderFactory !== null) {
                    try {
                        $provider = ($this->childProviderFactory)();
                    } catch (Throwable) {
                        $parent = $safe;
                    }
                } else {
                    $parent = $safe;
                }
                $parent = Configurator::create()->withTracerProvider($provider)
                    ->withMeterProvider(new \OpenTelemetry\API\Metrics\Noop\NoopMeterProvider())
                    ->withLoggerProvider(\OpenTelemetry\API\Logs\NoopLoggerProvider::getInstance())
                    ->storeInContext($parent);
            }
            $fields = self::fields($attributes);
            $version = InstalledVersions::isInstalled('durable-workflow/sdk')
                ? InstalledVersions::getPrettyVersion('durable-workflow/sdk') : null;
            $span = $provider->getTracer('durable-workflow/sdk', $version)->spanBuilder($name)
                ->setSpanKind($inherit ? SpanKind::KIND_INTERNAL : SpanKind::KIND_CONSUMER)
                ->setParent($parent)->setAttributes($fields)->startSpan();
            self::$key ??= Context::createKey('durable-workflow.sdk.correlation');
            $scope = $span->storeInContext($parent)->with(self::$key, $fields)->activate();
            self::enterFiber();
        } catch (Throwable) {
            self::close($scope, $span);

            try {
                return $operation();
            } finally {
                self::close($childShield, null);
            }
        }

        try {
            return $operation();
        } catch (Throwable $failure) {
            self::failure($failure);
            throw $failure;
        } finally {
            self::close($scope, $span);
            self::close($childShield, null);
        }
    }

    /** @param array<string, mixed> $attributes
     *  @return array<string, string|int>
     */
    private static function fields(array $attributes): array
    {
        $fields = [];
        foreach (self::FIELDS as $name) {
            $value = $attributes[$name] ?? null;
            if ($name === 'activity_attempt_number') {
                if (is_int($value) && $value > 0) {
                    $fields[$name] = $value;
                }
            } elseif (is_string($value) && $value !== '' && strlen($value) <= 256
                && !preg_match('/[\x00-\x1f\x7f]/', $value)) {
                $fields[$name] = $value;
            }
        }

        return $fields;
    }

    private static function enterFiber(): void
    {
        $fiber = Fiber::getCurrent();
        if ($fiber !== null) {
            self::$fibers ??= new WeakMap();
            self::$fibers[$fiber] = (self::$fibers[$fiber] ?? 0) + 1;
        }
    }

    private static function close(?ScopeInterface $scope, ?SpanInterface $span): void
    {
        try {
            $scope?->detach();
        } catch (Throwable) {
            // Telemetry cleanup cannot replace a business result or exception.
        }
        $fiber = Fiber::getCurrent();
        if ($scope !== null && $fiber !== null && isset(self::$fibers[$fiber])
            && --self::$fibers[$fiber] === 0) {
            unset(self::$fibers[$fiber]);
        }
        try {
            // The application owns provider buffering and shutdown. Never flush per task.
            $span?->end();
        } catch (Throwable) {
            // Export failures must not affect execution or acknowledgement.
        }
    }
}
