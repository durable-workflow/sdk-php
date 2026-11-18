<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use UnexpectedValueException;

/** A recorded version decision, or an explicit unknown, for one open run. */
final class WorkflowVersionUsageRun
{
    private function __construct(
        public readonly string $instanceId,
        public readonly string $runId,
        public readonly string $status,
        public readonly string $state,
        public readonly ?int $recordedVersion,
        public readonly ?string $unknownReason,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value, int $minimum): self
    {
        $keys = array_keys($value);
        sort($keys);
        if ($keys !== ['instance_id', 'recorded_version', 'run_id', 'state', 'status', 'unknown_reason']) {
            throw new UnexpectedValueException('Invalid workflow version-usage run fields.');
        }
        foreach (['instance_id', 'run_id'] as $field) {
            if (!is_string($value[$field]) || $value[$field] === '' || preg_match('//u', $value[$field]) !== 1) {
                throw new UnexpectedValueException('Invalid workflow version-usage run identity.');
            }
        }
        if (!in_array($value['status'], ['pending', 'running', 'waiting'], true)
            || !in_array($value['state'], ['legacy', 'current', 'unknown'], true)) {
            throw new UnexpectedValueException('Invalid workflow version-usage run state.');
        }
        $version = $value['recorded_version'];
        $reason = $value['unknown_reason'];
        if ($value['state'] === 'unknown') {
            if ($version !== null || !in_array($reason, ['marker_absent', 'history_unavailable', 'history_invalid'], true)) {
                throw new UnexpectedValueException('Unknown version usage requires its explicit unknown reason.');
            }
        } elseif (!is_int($version) || $version < -2147483648 || $version > 2147483647 || $reason !== null
            || (($version < $minimum) !== ($value['state'] === 'legacy'))) {
            throw new UnexpectedValueException('Recorded version usage does not match the proposed minimum.');
        }

        return new self($value['instance_id'], $value['run_id'], $value['status'], $value['state'], $version, $reason);
    }
}
