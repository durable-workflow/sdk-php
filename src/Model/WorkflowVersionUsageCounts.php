<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use UnexpectedValueException;

/** All-state counts for the live, upper-bounded open-run cohort before filtering. */
final class WorkflowVersionUsageCounts
{
    private function __construct(
        public readonly int $total,
        public readonly int $legacy,
        public readonly int $current,
        public readonly int $unknown,
    ) {
    }

    /** @param array<string, mixed> $value */
    public static function fromArray(array $value): self
    {
        $keys = array_keys($value);
        sort($keys);
        if ($keys !== ['current', 'legacy', 'total', 'unknown']) {
            throw new UnexpectedValueException('Invalid workflow version-usage counts.');
        }
        foreach ($value as $count) {
            if (!is_int($count) || $count < 0) {
                throw new UnexpectedValueException('Workflow version-usage counts must be non-negative integers.');
            }
        }
        if ($value['total'] !== $value['legacy'] + $value['current'] + $value['unknown']) {
            throw new UnexpectedValueException('Workflow version-usage counts do not reconcile.');
        }

        return new self($value['total'], $value['legacy'], $value['current'], $value['unknown']);
    }
}
