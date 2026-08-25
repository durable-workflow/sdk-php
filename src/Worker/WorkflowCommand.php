<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Codec\PayloadCodec;
use LogicException;

/** A replayable command emitted when straight-line workflow code suspends. */
final class WorkflowCommand
{
    private const MAX_MEMO_ENTRIES = 100;
    private const MAX_MEMO_VALUE_SIZE_BYTES = 10_240;
    private const MAX_MEMO_TOTAL_SIZE_BYTES = 65_536;
    private const MEMO_KEY_PATTERN = '/^(?!-?[0-9]+$)[A-Za-z0-9_.:-]{1,64}$/D';

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
        public readonly ?string $versionResultKind = null,
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

        return new self(
            $this->type,
            $this->historyShape,
            array_merge($this->attributes, ['result_value' => $value]),
            $value,
        );
    }

    /** @param array<string, mixed> $directive */
    public static function workflowStream(array $directive): self
    {
        return new self(
            'record_side_effect',
            'side_effect',
            ['workflow_stream' => $directive],
            sideEffect: static fn (): mixed => null,
        );
    }

    /** @param array<string, mixed> $attributes */
    public function withAttributes(array $attributes): self
    {
        return new self(
            $this->type,
            $this->historyShape,
            array_merge($this->attributes, $attributes),
            $this->localResult,
            $this->sideEffect,
            $this->conditionPredicate,
            $this->versionResultKind,
        );
    }

    public static function versionMarker(
        string $changeId,
        int $minSupported,
        int $maxSupported,
        string $resultKind,
    ): self {
        return new self(
            'record_version_marker',
            'version_marker',
            [
                'change_id' => $changeId,
                'version' => $maxSupported,
                'min_supported' => $minSupported,
                'max_supported' => $maxSupported,
            ],
            $maxSupported,
            versionResultKind: $resultKind,
        );
    }

    /** @internal Resolve the public helper's return type from one durable version decision. */
    public function versionResult(int $version): int|bool|null
    {
        if ($this->type !== 'record_version_marker' || $this->versionResultKind === null) {
            throw new LogicException('Only a version-marker command has a version result.');
        }

        return match ($this->versionResultKind) {
            'patched' => $version === 1,
            'deprecate_patch' => null,
            default => $version,
        };
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

    /** @param array<string, mixed> $entries */
    public static function upsertMemo(array $entries): self
    {
        return new self('upsert_memo', 'memo', ['entries' => self::canonicalMemoEntries($entries)]);
    }

    /**
     * Validate and canonicalize the language-neutral memo patch. The wire
     * serializer wraps the whole map in the public Avro payload envelope.
     *
     * @param array<array-key, mixed> $entries
     * @return array<string, mixed>
     */
    public static function canonicalMemoEntries(array $entries): array
    {
        if ($entries === []) {
            throw new LogicException('Workflow memo updates require at least one entry.');
        }
        if (count($entries) > self::MAX_MEMO_ENTRIES) {
            throw new LogicException(sprintf(
                'Workflow memo updates may contain at most %d entries.',
                self::MAX_MEMO_ENTRIES,
            ));
        }

        $canonical = [];
        foreach ($entries as $key => $value) {
            if (!is_string($key) || preg_match(self::MEMO_KEY_PATTERN, $key) !== 1) {
                throw new LogicException(
                    'Workflow memo keys must match ^(?!-?[0-9]+$)[A-Za-z0-9_.:-]{1,64}$.',
                );
            }
            $value = self::canonicalMemoValue($value);
            if (self::avroEncodedSize($value) > self::MAX_MEMO_VALUE_SIZE_BYTES) {
                throw new LogicException(sprintf(
                    'Workflow memo value %s exceeds the %d-byte limit.',
                    $key,
                    self::MAX_MEMO_VALUE_SIZE_BYTES,
                ));
            }
            $canonical[$key] = $value;
        }
        ksort($canonical, SORT_STRING);

        if (self::avroEncodedSize($canonical) > self::MAX_MEMO_TOTAL_SIZE_BYTES) {
            throw new LogicException(sprintf(
                'Workflow memo update exceeds the %d-byte total limit.',
                self::MAX_MEMO_TOTAL_SIZE_BYTES,
            ));
        }

        return $canonical;
    }

    private static function canonicalMemoValue(mixed $value): mixed
    {
        if ($value instanceof AvroBinaryValue) {
            return $value;
        }
        if ($value instanceof AvroMapValue) {
            $pairs = array_map(
                static fn (array $pair): array => [$pair[0], self::canonicalMemoValue($pair[1])],
                $value->pairs,
            );
            usort($pairs, static fn (array $left, array $right): int => strcmp($left[0], $right[0]));

            return AvroMapValue::fromPairs($pairs);
        }
        if (!is_array($value)) {
            return $value;
        }

        $canonical = [];
        foreach ($value as $key => $nested) {
            $canonical[$key] = self::canonicalMemoValue($nested);
        }
        if (!array_is_list($canonical)) {
            ksort($canonical, SORT_STRING);
        }

        return $canonical;
    }

    private static function avroEncodedSize(mixed $value): int
    {
        $bytes = base64_decode((new AvroPayloadCodec())->encode($value), true);
        if ($bytes === false) {
            throw new LogicException('Workflow memo Avro encoding did not return strict base64.');
        }

        return strlen($bytes);
    }

    /** @return array<string, mixed> */
    public function toWire(PayloadCodec $codec, string $defaultTaskQueue): array
    {
        $wire = ['type' => $this->type];
        foreach ($this->attributes as $key => $value) {
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
            if ($this->type === 'upsert_memo' && $key === 'entries') {
                $wire['entries'] = $codec->envelope($value);
                continue;
            }
            if ($value === null) {
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
