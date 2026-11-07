<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

/** Fixed workload; backend/conditions are caller bindings, never runtime attestations. */
final class CompletionPollLoadProfile
{
    public const SCENARIOS = ['load-local-off', 'load-local-on', 'load-remote-off', 'load-remote-on'];
    public const WORKERS = 6;
    public const WORKFLOWS = 200;
    public const STEPS = 5;
    public const STEP_MICROSECONDS = 20_000;

    /** @param array<string, mixed> $data */
    public static function validate(array $data): void
    {
        $load = $data['load'] ?? null;
        if (!in_array($data['scenario'] ?? null, self::SCENARIOS, true)) {
            if (array_key_exists('load', $data)) throw new RuntimeException('completion_subject_load');
            return;
        }
        $keys = ['backend', 'database_identity', 'repetition', 'conditions_sha256'];
        if (!is_array($load) || array_diff(array_keys($load), $keys) !== [] || array_diff($keys, array_keys($load)) !== []
            || !in_array($load['backend'] ?? null, ['sqlite', 'mysql', 'postgresql'], true)
            || !is_string($load['database_identity'] ?? null)
            || preg_match('/\A[A-Za-z0-9][A-Za-z0-9_-]{7,63}\z/', $load['database_identity']) !== 1
            || !is_int($load['repetition'] ?? null) || $load['repetition'] < 1 || $load['repetition'] > 3
            || !is_string($load['conditions_sha256'] ?? null) || preg_match('/\A[a-f0-9]{64}\z/', $load['conditions_sha256']) !== 1) {
            throw new RuntimeException('completion_subject_load');
        }
    }

    /** @return array<string, int|string> */
    public static function workload(string $scenario): array
    {
        if (!in_array($scenario, self::SCENARIOS, true)) throw new RuntimeException('completion_subject_load');
        return ['profile' => 'six-workers-200-workflows-five-20ms-steps/v1', 'workers' => self::WORKERS,
            'workflows' => self::WORKFLOWS, 'steps_per_workflow' => self::STEPS, 'step_microseconds' => self::STEP_MICROSECONDS,
            'activity_kind' => str_contains($scenario, '-local-') ? 'local' : 'remote'];
    }
}
