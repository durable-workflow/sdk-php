<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\SagaCompensationFailed;
use LogicException;
use Throwable;

/**
 * Registers activity compensations and replays them in deterministic reverse order.
 *
 * Registration is workflow-local state. Durable activity history reconstructs that
 * state on every replay and records each compensation through the ordinary activity
 * command path.
 */
final class Saga
{
    /**
     * @var list<array{
     *     activity_type: string,
     *     arguments: list<mixed>,
     *     options: array<string, mixed>,
     *     registration_order: int
     * }>
     */
    private array $compensations = [];

    private bool $compensating = false;

    private bool $compensated = false;

    private ?SagaCompensationFailed $failure = null;

    /** @internal Create sagas through {@see WorkflowContext::saga()}. */
    public function __construct(private readonly WorkflowContext $context)
    {
    }

    /**
     * Register an activity to compensate a forward step that has completed.
     *
     * Call this only after the forward activity returns. Arguments and options use
     * the same typed command shape as {@see WorkflowContext::activity()}.
     *
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public function addCompensation(
        string $activityType,
        array $arguments = [],
        array $options = [],
    ): self {
        if ($this->compensating || $this->compensated || $this->failure !== null) {
            throw new LogicException('A saga cannot register compensation after compensation has started.');
        }
        if ($activityType === '' || trim($activityType) !== $activityType) {
            throw new LogicException('A saga compensation activity type must be non-empty without surrounding whitespace.');
        }

        $this->compensations[] = [
            'activity_type' => $activityType,
            'arguments' => $arguments,
            'options' => $options,
            'registration_order' => count($this->compensations) + 1,
        ];

        return $this;
    }

    /**
     * Run a forward path and automatically compensate any thrown failure.
     *
     * The original failure is rethrown after successful compensation. If a
     * compensation fails, a typed SagaCompensationFailed preserves both failures.
     * Nested Saga::run() calls remain isolated; an escaping inner failure triggers
     * the outer saga's own compensation list.
     *
     * @template TResult
     * @param callable(self): TResult $forward
     * @return TResult
     */
    public function run(callable $forward): mixed
    {
        try {
            return $forward($this);
        } catch (Throwable $forwardFailure) {
            $this->compensate($forwardFailure);

            throw $forwardFailure;
        }
    }

    /**
     * Execute registered compensation activities in reverse order.
     *
     * Repeated calls after success are no-ops. Compensations stop at the first
     * terminal ActivityFailed. That failure is stable for the rest of the current
     * replay and is rethrown instead of starting a second compensation pass.
     */
    public function compensate(Throwable $forwardFailure): void
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }
        if ($this->compensated) {
            return;
        }
        if ($this->compensating) {
            throw new LogicException('A saga cannot start a re-entrant compensation pass.');
        }

        $this->compensating = true;
        foreach (array_reverse($this->compensations) as $compensation) {
            try {
                $this->context->activity(
                    $compensation['activity_type'],
                    $compensation['arguments'],
                    $compensation['options'],
                );
            } catch (ActivityFailed $compensationFailure) {
                $this->failure = new SagaCompensationFailed(
                    $forwardFailure,
                    $compensationFailure,
                    $compensation['activity_type'],
                    $compensation['registration_order'],
                );

                throw $this->failure;
            }
        }

        $this->compensating = false;
        $this->compensated = true;
    }
}
