<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;

/**
 * @internal The Server's `replay_clock` entry carrier on a workflow task
 * (`durable-workflow.worker-replay-clock/v1`, server#1156, .github#215).
 *
 * Its presence is the only availability signal. Anything that is not exactly
 * the v1 shape for this task's run is treated as absent; no other time is
 * substituted for it.
 */
final class WorkerReplayClock
{
    public const SCHEMA = 'durable-workflow.worker-replay-clock/v1';
    public const RUN_STARTED = 'run_started_at';
    public const REDRIVE_ANCHOR = 'redrive_replayed_started_at';

    /** The exact v1 key set, sorted. */
    private const FIELDS = ['schema', 'source', 'started_at', 'workflow_run_id'];

    private function __construct(public readonly DateTimeImmutable $startedAt, public readonly string $source)
    {
    }

    /** The validated carrier for this run, or null when it is absent or not provable. */
    public static function fromTask(mixed $carrier, string $runId): ?self
    {
        if (!is_array($carrier) || array_is_list($carrier) || $runId === '') {
            return null;
        }
        // WorkerReplayClock v1 has additionalProperties: false, so any other key set is not v1.
        $keys = array_keys($carrier);
        sort($keys, SORT_STRING);
        if ($keys !== self::FIELDS) {
            return null;
        }
        if (($carrier['schema'] ?? null) !== self::SCHEMA || ($carrier['workflow_run_id'] ?? null) !== $runId
            || !in_array($carrier['source'] ?? null, [self::RUN_STARTED, self::REDRIVE_ANCHOR], true)) {
            return null;
        }
        $startedAt = self::instant($carrier['started_at'] ?? null);

        return $startedAt === null ? null : new self($startedAt, $carrier['source']);
    }

    private static function instant(mixed $value): ?DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z\z/', $value) !== 1) {
            return null;
        }
        $time = DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s.u\Z', $value, new DateTimeZone('UTC'));
        $errors = DateTimeImmutable::getLastErrors();
        if ($time === false || ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0))) {
            return null;
        }

        // Rejects rolled-over calendar values such as February 30 or hour 24.
        return $time->format('Y-m-d\TH:i:s.u\Z') === $value ? $time : null;
    }
}
