<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Codec\PayloadCodec;
use LogicException;

/** A replayable command emitted when straight-line workflow code suspends. */
final class WorkflowCommand
{
    /**
     * @param array<string, mixed> $attributes
     * @param (Closure(): mixed)|null $sideEffect
     */
    public function __construct(
        public readonly string $type,
        public readonly string $historyShape,
        public readonly array $attributes = [],
        public readonly mixed $localResult = null,
        private readonly ?Closure $sideEffect = null,
        private readonly ?Closure $conditionPredicate = null,
    ) {
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public static function activity(string $activityType, array $arguments, array $options = []): self
    {
        return new self('schedule_activity', 'activity', array_merge($options, [
            'activity_type' => $activityType,
            'arguments_value' => $arguments,
        ]));
    }

    public static function timer(int $seconds): self
    {
        return new self('start_timer', 'timer', ['delay_seconds' => max(0, $seconds)]);
    }

    /** @param Closure(): bool $predicate */
    public static function conditionWait(
        Closure $predicate,
        ?string $key,
        ?string $definitionFingerprint,
        ?int $timeoutSeconds,
    ): self {
        return new self('open_condition_wait', 'condition_wait', array_filter([
            'condition_key' => $key,
            'condition_definition_fingerprint' => $definitionFingerprint,
            'timeout_seconds' => $timeoutSeconds,
        ], static fn (mixed $value): bool => $value !== null), conditionPredicate: $predicate);
    }

    /** @internal Condition predicates are evaluated by the replayer after durable history matching. */
    public function conditionSatisfied(): bool
    {
        if ($this->type !== 'open_condition_wait' || $this->conditionPredicate === null) {
            throw new LogicException('Only a condition-wait command has a predicate.');
        }

        return ($this->conditionPredicate)() === true;
    }

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     */
    public static function childWorkflow(string $workflowType, array $arguments, array $options = []): self
    {
        return new self('start_child_workflow', 'child_workflow', array_merge($options, [
            'workflow_type' => $workflowType,
            'arguments_value' => $arguments,
        ]));
    }

    /** @param callable(): mixed $operation */
    public static function sideEffect(callable $operation): self
    {
        return new self(
            'record_side_effect',
            'side_effect',
            sideEffect: Closure::fromCallable($operation),
        );
    }

    /** @internal Side effects are evaluated by the replayer only after history matching. */
    public function resolveSideEffect(): self
    {
        if ($this->type !== 'record_side_effect' || $this->sideEffect === null) {
            throw new LogicException('Only a deferred side-effect command can be resolved.');
        }

        $value = ($this->sideEffect)();

        return new self($this->type, $this->historyShape, ['result_value' => $value], $value);
    }

    /** @param list<mixed> $arguments */
    public static function continueAsNew(array $arguments, ?string $workflowType = null, ?string $taskQueue = null): self
    {
        return new self('continue_as_new', 'terminal', [
            'arguments_value' => $arguments,
            'workflow_type' => $workflowType,
            'queue' => $taskQueue,
        ]);
    }

    /** @param array<string, mixed> $attributes */
    public static function upsertSearchAttributes(array $attributes): self
    {
        return new self('upsert_search_attributes', 'search_attributes', ['attributes' => $attributes]);
    }

    /** @return array<string, mixed> */
    public function toWire(PayloadCodec $codec, string $defaultTaskQueue): array
    {
        $wire = ['type' => $this->type];
        foreach ($this->attributes as $key => $value) {
            if ($value === null) {
                continue;
            }
            if ($key === 'arguments_value') {
                $wire['arguments'] = $codec->envelope($value);
                continue;
            }
            if ($key === 'result_value') {
                $wire['result'] = $this->type === 'record_side_effect'
                    ? $codec->encode($value)
                    : $codec->envelope($value);
                continue;
            }
            $wire[$key] = $value;
        }
        if (in_array($this->type, ['schedule_activity', 'start_child_workflow', 'continue_as_new'], true)
            && !isset($wire['queue'])) {
            $wire['queue'] = $defaultTaskQueue;
        }

        return $wire;
    }
}
