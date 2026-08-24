<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

use Throwable;

/** A saga forward failure was followed by a terminal compensation activity failure. */
final class SagaCompensationFailed extends DurableWorkflowException
{
    public readonly string $forwardStep;

    public function __construct(
        public readonly Throwable $forwardFailure,
        public readonly ActivityFailed $compensationFailure,
        public readonly string $compensationActivityType,
        public readonly int $compensationRegistrationOrder,
    ) {
        $this->forwardStep = self::forwardStep($forwardFailure);

        parent::__construct(sprintf(
            'Saga forward step %s failed; compensation activity %s (registration %d) also failed: %s',
            $this->forwardStep,
            $this->compensationActivityType,
            $this->compensationRegistrationOrder,
            $this->compensationFailure->getMessage(),
        ), previous: $compensationFailure);
    }

    /** @return array<string, mixed> */
    public function diagnosticContext(): array
    {
        return [
            'forward_step' => $this->forwardStep,
            'forward_exception_type' => $this->forwardFailure::class,
            'forward_message' => $this->forwardFailure->getMessage(),
            'compensation_activity_type' => $this->compensationActivityType,
            'compensation_registration_order' => $this->compensationRegistrationOrder,
            'compensation_exception_type' => $this->compensationFailure->failureType
                ?? $this->compensationFailure::class,
            'compensation_message' => $this->compensationFailure->getMessage(),
        ];
    }

    private static function forwardStep(Throwable $failure): string
    {
        if ($failure instanceof self) {
            return $failure->forwardStep;
        }
        if ($failure instanceof ActivityFailed) {
            return $failure->activityType ?? '<unknown activity>';
        }
        if ($failure instanceof WorkflowCancelled) {
            return '<workflow cancellation>';
        }

        return $failure::class;
    }
}
