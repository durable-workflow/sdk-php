<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge;

use DurableWorkflow\Client;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker\StickyWorkflowCache;
use InvalidArgumentException;

/** Validated service-mode settings shared by the optional framework bridges. */
final class ServiceConfiguration
{
    /**
     * @param list<string> $handlers
     */
    public function __construct(
        public readonly string $endpoint,
        public readonly string $namespace,
        public readonly string $taskQueue,
        public readonly array $handlers,
        public readonly ?string $token = null,
        public readonly ?string $controlToken = null,
        public readonly ?string $workerToken = null,
        public readonly int $pollTimeoutSeconds = 5,
        public readonly int $stickyCacheCapacity = 100,
        public readonly int $stickyCacheTtlSeconds = 300,
        public readonly ?float $requestTimeoutSeconds = null,
    ) {
        $this->validate();
    }

    /** @param array<string, mixed> $values */
    public static function fromArray(array $values): self
    {
        $credentials = $values['credentials'] ?? [];
        if (!is_array($credentials)) {
            throw new InvalidArgumentException(
                'Durable Workflow credentials must be an array containing token, control_token, or worker_token.',
            );
        }

        $handlers = $values['handlers'] ?? [];
        if (!is_array($handlers) || !array_is_list($handlers)) {
            throw new InvalidArgumentException('Durable Workflow handlers must be a list of service class names.');
        }

        $worker = array_key_exists('worker', $values) ? $values['worker'] : [];
        if (!is_array($worker)) {
            throw new InvalidArgumentException('Durable Workflow worker settings must be an array.');
        }
        $stickyCache = array_key_exists('sticky_cache', $worker) ? $worker['sticky_cache'] : [];
        if (!is_array($stickyCache)) {
            throw new InvalidArgumentException('Durable Workflow worker.sticky_cache settings must be an array.');
        }

        return new self(
            self::string($values, 'endpoint'),
            self::string($values, 'namespace', 'default'),
            self::string($values, 'task_queue'),
            self::handlers($handlers),
            self::nullableString($credentials, 'token'),
            self::nullableString($credentials, 'control_token'),
            self::nullableString($credentials, 'worker_token'),
            self::integer($values, 'poll_timeout_seconds', 5),
            self::cacheInteger($stickyCache, 'capacity', 100),
            self::cacheInteger($stickyCache, 'ttl_seconds', 300),
            self::requestTimeout($values),
        );
    }

    /** Build the application-facing client without access to a scoped worker credential. */
    public function client(?Transport $transport = null): Client
    {
        return $this->controlClient($transport);
    }

    /** Build the application-facing client without access to a scoped worker credential. */
    public function controlClient(?Transport $transport = null): Client
    {
        if ($this->token === null && $this->controlToken === null && $this->workerToken !== null) {
            throw new InvalidArgumentException(
                'A control credential is required for the application client. Configure the shared token or scoped control token.',
            );
        }

        return new Client(
            $this->endpoint,
            namespace: $this->namespace,
            transport: $transport,
            token: $this->token,
            controlToken: $this->controlToken,
            requestTimeoutSeconds: $this->requestTimeoutSeconds,
        );
    }

    /** Build the worker client without access to a scoped control credential. */
    public function workerClient(?Transport $transport = null): Client
    {
        if ($this->token === null && $this->workerToken === null && $this->controlToken !== null) {
            throw new InvalidArgumentException(
                'A worker credential is required for the worker client. Configure the shared token or scoped worker token.',
            );
        }

        return new Client(
            $this->endpoint,
            namespace: $this->namespace,
            transport: $transport,
            token: $this->token,
            workerToken: $this->workerToken,
        );
    }

    public function taskQueue(?string $override = null): string
    {
        if ($override === null) {
            return $this->taskQueue;
        }
        if ($override === '' || trim($override) !== $override) {
            throw new InvalidArgumentException(
                'The Durable Workflow task queue override must be non-empty without surrounding whitespace.',
            );
        }

        return $override;
    }

    public static function assertWorkerExtensions(): void
    {
        if (!function_exists('pcntl_async_signals') || !function_exists('pcntl_signal')) {
            throw new FrameworkRuntimeException(
                'The Durable Workflow console worker requires ext-pcntl for graceful SIGINT and SIGTERM handling. Install or enable ext-pcntl for the worker PHP runtime.',
            );
        }
    }

    private function validate(): void
    {
        if ($this->endpoint === '') {
            throw new InvalidArgumentException(
                'Durable Workflow endpoint is required. Set it to the self-hosted Server origin or the complete Cloud runtime base URI.',
            );
        }
        if (trim($this->endpoint) !== $this->endpoint) {
            throw new InvalidArgumentException('Durable Workflow endpoint must not contain surrounding whitespace.');
        }
        $parts = parse_url($this->endpoint);
        if (!is_array($parts)
            || !in_array($parts['scheme'] ?? null, ['http', 'https'], true)
            || !isset($parts['host'])
        ) {
            throw new InvalidArgumentException(
                'Durable Workflow endpoint must be an absolute http:// or https:// Server or Cloud runtime URI.',
            );
        }
        if (str_ends_with(rtrim((string) ($parts['path'] ?? ''), '/'), '/api')) {
            throw new InvalidArgumentException(
                'Durable Workflow endpoint must omit the SDK-owned /api suffix; use the Server or Cloud runtime base URI.',
            );
        }
        foreach (['namespace' => $this->namespace, 'task queue' => $this->taskQueue] as $name => $value) {
            if ($value === '' || trim($value) !== $value) {
                throw new InvalidArgumentException(
                    "Durable Workflow {$name} must be non-empty without surrounding whitespace.",
                );
            }
        }
        if ($this->token !== null && ($this->controlToken !== null || $this->workerToken !== null)) {
            throw new InvalidArgumentException(
                'Configure either the shared Durable Workflow token or scoped control and worker tokens, not both.',
            );
        }
        if ($this->pollTimeoutSeconds < 0 || $this->pollTimeoutSeconds > 60) {
            throw new InvalidArgumentException('Durable Workflow poll_timeout_seconds must be between 0 and 60.');
        }
        if ($this->requestTimeoutSeconds !== null && (!is_finite($this->requestTimeoutSeconds)
            || $this->requestTimeoutSeconds < 0.001 || $this->requestTimeoutSeconds > intdiv(PHP_INT_MAX, 1000))) {
            throw new InvalidArgumentException('Durable Workflow request_timeout_seconds must be a finite positive duration with millisecond precision.');
        }
        if ($this->stickyCacheCapacity < 1) {
            throw new InvalidArgumentException('Durable Workflow worker.sticky_cache.capacity must be a positive integer.');
        }
        if ($this->stickyCacheTtlSeconds < 1 || $this->stickyCacheTtlSeconds > StickyWorkflowCache::MAX_TTL_SECONDS) {
            throw new InvalidArgumentException('Durable Workflow worker.sticky_cache.ttl_seconds must be between 1 and 3600.');
        }
    }

    /** @param array<string, mixed> $values */
    private static function string(array $values, string $key, ?string $default = null): string
    {
        $value = $values[$key] ?? $default;
        if (!is_string($value)) {
            throw new InvalidArgumentException("Durable Workflow {$key} must be a string.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function nullableString(array $values, string $key): ?string
    {
        $value = $values[$key] ?? null;
        if ($value === null || $value === '') {
            return null;
        }
        if (!is_string($value)) {
            throw new InvalidArgumentException("Durable Workflow credential {$key} must be a string or null.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function integer(array $values, string $key, int $default): int
    {
        $value = $values[$key] ?? $default;
        if (!is_int($value)) {
            throw new InvalidArgumentException("Durable Workflow {$key} must be an integer.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function requestTimeout(array $values): ?float
    {
        $value = $values['request_timeout_seconds'] ?? null;
        if ($value === null) {
            return null;
        }
        if ((!is_int($value) && !is_float($value) && !is_string($value)) || !is_numeric($value)) {
            throw new InvalidArgumentException('Durable Workflow request_timeout_seconds must be a number or null.');
        }

        return (float) $value;
    }

    /** @param array<string, mixed> $values */
    private static function cacheInteger(array $values, string $key, int $default): int
    {
        $value = array_key_exists($key, $values) ? $values[$key] : $default;
        if (!is_int($value)) {
            throw new InvalidArgumentException("Durable Workflow worker.sticky_cache.{$key} must be an integer.");
        }

        return $value;
    }

    /**
     * @param list<mixed> $handlers
     * @return list<string>
     */
    private static function handlers(array $handlers): array
    {
        $seen = [];
        foreach ($handlers as $handler) {
            if (!is_string($handler) || $handler === '' || trim($handler) !== $handler) {
                throw new InvalidArgumentException(
                    'Each Durable Workflow handler must be a non-empty service class name without surrounding whitespace.',
                );
            }
            if (isset($seen[$handler])) {
                throw new InvalidArgumentException("Durable Workflow handler {$handler} is configured more than once.");
            }
            $seen[$handler] = true;
        }

        return $handlers;
    }
}
