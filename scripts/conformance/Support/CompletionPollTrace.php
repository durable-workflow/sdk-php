<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

/** Bounded synthetic wire metadata only. Credential headers and payloads are never retained. */
final class CompletionPollTrace
{
    public function __construct(private readonly string $file)
    {
    }

    /** @param array<string, mixed> $event */
    public function record(array $event): void
    {
        $this->change(static function (array &$state) use ($event): void {
            if (count($state['events']) >= 2048) {
                throw new RuntimeException('completion_trace_limit');
            }
            $state['events'][] = ['at' => hrtime(true)] + $event;
        });
    }

    /** @return array<string, mixed> */
    public function state(): array
    {
        return $this->change(static function (array &$state): void {});
    }

    /** @param callable(array<string, mixed>&): void $operation
     * @return array<string, mixed>
     */
    public function change(callable $operation): array
    {
        $stream = fopen($this->file, 'r+');
        if ($stream === false) {
            throw new RuntimeException('completion_trace_file');
        }
        try {
            if (!flock($stream, LOCK_EX)) {
                throw new RuntimeException('completion_trace_lock');
            }
            $raw = stream_get_contents($stream, 2097153);
            if ($raw === false || strlen($raw) > 2097152) {
                throw new RuntimeException('completion_trace_limit');
            }
            $state = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
            if (!is_array($state) || !is_array($state['events'] ?? null)) {
                throw new RuntimeException('completion_trace_shape');
            }
            $operation($state);
            $bytes = json_encode($state, JSON_THROW_ON_ERROR);
            if (strlen($bytes) > 2097152 || fseek($stream, 0) !== 0 || !ftruncate($stream, 0)
                || fwrite($stream, $bytes) !== strlen($bytes) || !fflush($stream)) {
                throw new RuntimeException('completion_trace_write');
            }
            return $state;
        } finally {
            flock($stream, LOCK_UN);
            fclose($stream);
        }
    }

    /** @param array<string, mixed>|null $task
     * @return array<string, mixed>|null
     */
    public static function task(?array $task): ?array
    {
        return $task === null ? null : array_intersect_key($task, array_flip([
            'task_id', 'task_kind', 'workflow_id', 'run_id', 'lease_owner', 'workflow_task_attempt',
            'activity_attempt_id', 'lease_expires_at',
        ]));
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public static function request(array $body, ?string $wire = null): array
    {
        $result = array_intersect_key($body, array_flip(['worker_id', 'lease_owner', 'workflow_task_attempt',
            'activity_attempt_id', 'poll_request_id', 'timeout_seconds', 'task_slots']));
        foreach (['activity_poll', 'workflow_poll'] as $key) {
            if (array_key_exists($key, $body)) {
                $result[$key] = array_intersect_key($body[$key], array_flip(['worker_id', 'poll_request_id', 'timeout_seconds', 'task_kinds']));
            }
        }
        $result['command_types'] = array_column($body['commands'] ?? [], 'type');
        // This digest proves identical retry bytes without retaining outcome payloads.
        $result['body_sha256'] = hash('sha256', $wire ?? json_encode($body, JSON_THROW_ON_ERROR));
        return $result;
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    public static function answer(array $body): array
    {
        $result = array_intersect_key($body, array_flip(['poll_status', 'reason', 'server_time', 'protocol_version']));
        $result['task'] = self::task(is_array($body['task'] ?? null) ? $body['task'] : null);
        $result['capabilities'] = array_intersect_key($body['server_capabilities'] ?? [], array_flip([
            'workflow_task_completion_activity_poll', 'activity_completion_workflow_poll', 'workflow_task_completion_workflow_poll',
        ]));
        foreach (['activity_poll', 'workflow_poll'] as $key) {
            if (!array_key_exists($key, $body)) {
                continue;
            }
            $answer = $body[$key]['answer'] ?? null;
            $result[$key] = ['leased' => $body[$key]['leased'] ?? null,
                'task' => self::task(is_array($answer['task'] ?? null) ? $answer['task'] : null)];
        }
        return $result;
    }
}
