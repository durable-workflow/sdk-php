<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

/** Public compatibility and capability manifest returned by cluster discovery. */
final class ClusterInfo
{
    /** Per-task-queue activity dispatch pause and resume (native Server 3.8.0 and later). */
    public const TASK_QUEUE_ACTIVITY_DISPATCH_PAUSE = 'task_queue_activity_dispatch_pause';

    /** Signal and update `request_id` is a durable command key (native Server 3.9.0 and later). */
    public const COMMAND_REQUEST_KEYS = 'command_request_keys';

    /**
     * @param array<string, mixed> $namespace
     * @param array<string, mixed> $capabilities
     * @param array<string, mixed> $limits
     * @param array<string, mixed> $controlPlane
     * @param array<string, mixed> $raw
     */
    public function __construct(
        public readonly ?string $serverId,
        public readonly ?string $version,
        public readonly ?string $defaultNamespace,
        public readonly array $namespace,
        public readonly array $capabilities,
        public readonly array $limits,
        public readonly array $controlPlane,
        public readonly array $raw,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        return new self(
            isset($value['server_id']) ? (string) $value['server_id'] : null,
            isset($value['version']) ? (string) $value['version'] : null,
            isset($value['default_namespace']) ? (string) $value['default_namespace'] : null,
            self::map($value['namespace'] ?? null),
            self::map($value['capabilities'] ?? null),
            self::map($value['limits'] ?? null),
            self::map($value['control_plane'] ?? null),
            $value,
        );
    }

    /**
     * Whether the Server can pause and resume activity dispatch on a task queue. A Server that
     * doesn't advertise it has no pause routes, so check it before offering a pause.
     */
    public function supportsTaskQueueActivityDispatchPause(): bool
    {
        return ($this->capabilities[self::TASK_QUEUE_ACTIVITY_DISPATCH_PAUSE] ?? null) === true;
    }

    /**
     * Whether a signal or update `request_id` is a durable command key on the workflow instance.
     *
     * When true, a retry with the same name, arguments and run target returns the original
     * receipt, and the same key with different input is refused with 409 `request_id_conflict`.
     * Without it, a repeated signal key can record a second signal and a changed update reuses
     * the original receipt, so keep business-event handling idempotent.
     */
    public function supportsCommandRequestKeys(): bool
    {
        return ($this->capabilities[self::COMMAND_REQUEST_KEYS] ?? null) === true;
    }

    /** @return array<string, mixed> */
    private static function map(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }

        $result = [];
        foreach ($value as $key => $entry) {
            if (is_string($key)) {
                $result[$key] = $entry;
            }
        }

        return $result;
    }
}
