<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\InvalidLocalActivityReport;
use InvalidArgumentException;
use LogicException;
use Throwable;

/** A replayable command emitted when straight-line workflow code suspends. */
final class WorkflowCommand
{
    private const MAX_MEMO_ENTRIES = 100;
    private const MAX_MEMO_VALUE_SIZE_BYTES = 10_240;
    private const MAX_MEMO_TOTAL_SIZE_BYTES = 65_536;
    private const MEMO_KEY_PATTERN = '/^(?!-?[0-9]+$)[A-Za-z0-9_.:-]{1,64}$/D';
    private const MAX_LOCAL_ACTIVITY_ATTEMPTS = 100;
    private const MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES = 255;
    private const LOCAL_ACTIVITY_IDENTITY_FIELDS = ['activity_type', 'arguments_value', 'execution_mode'];
    private const LOCAL_ACTIVITY_OPTION_FIELDS = [
        'retry_policy',
        'start_to_close_timeout',
        'schedule_to_close_timeout',
        'heartbeat_timeout',
    ];
    private const LOCAL_ACTIVITY_RETRY_POLICY_FIELDS = [
        'max_attempts',
        'backoff_seconds',
        'non_retryable_error_types',
    ];

    /**
     * @param array<string, mixed> $attributes
     * @param (Closure(): mixed)|null $sideEffect
     * @param array{codec: string, blob: string}|null $localActivityArgumentsEnvelope
     * @param array{codec: string, blob: string}|null $localActivityResultEnvelope
     */
    public function __construct(
        public readonly string $type,
        public readonly string $historyShape,
        public readonly array $attributes = [],
        public readonly mixed $localResult = null,
        private readonly ?Closure $sideEffect = null,
        private readonly ?Closure $conditionPredicate = null,
        private readonly ?Closure $localActivity = null,
        public readonly ?string $versionResultKind = null,
        private readonly ?array $localActivityArgumentsEnvelope = null,
        private readonly ?array $localActivityResultEnvelope = null,
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

    /**
     * @param list<mixed> $arguments
     * @param array<string, mixed> $options
     * @param callable(string, list<mixed>, array<string, mixed>): array<string, mixed> $executor
     */
    public static function localActivity(
        string $activityType,
        array $arguments,
        array $options,
        callable $executor,
    ): self {
        $activityType = trim($activityType);
        if ($activityType === '') {
            throw new InvalidArgumentException('Local activity type must be a non-empty string.');
        }

        return new self(
            'record_local_activity',
            'activity',
            [
                'activity_type' => $activityType,
                'arguments_value' => $arguments,
                'execution_mode' => 'local',
                ...self::canonicalLocalActivityOptions($options),
            ],
            localActivity: Closure::fromCallable($executor),
        );
    }

    /**
     * Validate and canonicalize the bounded record_local_activity authoring options.
     *
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    public static function canonicalLocalActivityOptions(array $options): array
    {
        foreach ($options as $field => $_value) {
            if (in_array($field, self::LOCAL_ACTIVITY_IDENTITY_FIELDS, true)) {
                throw new InvalidArgumentException("Local activity option {$field} is fixed by the SDK and cannot be overridden.");
            }
            if (!in_array($field, self::LOCAL_ACTIVITY_OPTION_FIELDS, true)) {
                throw new InvalidArgumentException("Local activities do not accept the {$field} option.");
            }
        }

        $canonical = [];
        if (array_key_exists('retry_policy', $options) && $options['retry_policy'] !== null) {
            $canonicalRetryPolicy = self::canonicalLocalActivityRetryPolicy($options['retry_policy']);
            if ($canonicalRetryPolicy !== []) {
                $canonical['retry_policy'] = $canonicalRetryPolicy;
            }
        }

        foreach (['start_to_close_timeout', 'schedule_to_close_timeout', 'heartbeat_timeout'] as $field) {
            if (!array_key_exists($field, $options) || $options[$field] === null) {
                continue;
            }
            if (!is_int($options[$field]) || $options[$field] < 1) {
                throw new InvalidArgumentException("Local activity {$field} must be a positive integer when provided.");
            }
            $canonical[$field] = $options[$field];
        }

        $startToClose = $canonical['start_to_close_timeout'] ?? null;
        $scheduleToClose = $canonical['schedule_to_close_timeout'] ?? null;
        $heartbeat = $canonical['heartbeat_timeout'] ?? null;
        if ($startToClose !== null && $scheduleToClose !== null && $scheduleToClose < $startToClose) {
            throw new InvalidArgumentException(
                'Local activity schedule_to_close_timeout must be greater than or equal to start_to_close_timeout.',
            );
        }
        if ($startToClose !== null && $heartbeat !== null && $heartbeat > $startToClose) {
            throw new InvalidArgumentException(
                'Local activity heartbeat_timeout must be less than or equal to start_to_close_timeout.',
            );
        }

        return $canonical;
    }

    /** @return array<string, mixed> */
    private static function canonicalLocalActivityRetryPolicy(mixed $retryPolicy): array
    {
        if (!is_array($retryPolicy) || ($retryPolicy !== [] && array_is_list($retryPolicy))) {
            throw new InvalidArgumentException('Local activity retry_policy must be an object when provided.');
        }
        foreach ($retryPolicy as $field => $_value) {
            if (!is_string($field) || !in_array($field, self::LOCAL_ACTIVITY_RETRY_POLICY_FIELDS, true)) {
                $name = is_string($field) ? $field : get_debug_type($field);

                throw new InvalidArgumentException("Local activity retry_policy does not accept the {$name} field.");
            }
        }

        $canonical = [];
        $maxAttempts = 1;
        if (array_key_exists('max_attempts', $retryPolicy)) {
            $maxAttempts = $retryPolicy['max_attempts'];
            if (!is_int($maxAttempts) || $maxAttempts < 1 || $maxAttempts > self::MAX_LOCAL_ACTIVITY_ATTEMPTS) {
                throw new InvalidArgumentException(sprintf(
                    'Local activity retry_policy.max_attempts must be an integer between 1 and %d.',
                    self::MAX_LOCAL_ACTIVITY_ATTEMPTS,
                ));
            }
            $canonical['max_attempts'] = $maxAttempts;
        }

        if (array_key_exists('backoff_seconds', $retryPolicy)) {
            $backoff = $retryPolicy['backoff_seconds'];
            if (!is_array($backoff) || !array_is_list($backoff)) {
                throw new InvalidArgumentException(
                    'Local activity retry_policy.backoff_seconds must be an ordered list of non-negative integers.',
                );
            }
            foreach ($backoff as $seconds) {
                if (!is_int($seconds) || $seconds < 0) {
                    throw new InvalidArgumentException(
                        'Local activity retry_policy.backoff_seconds entries must be non-negative integers.',
                    );
                }
            }
            if (count($backoff) > $maxAttempts - 1) {
                throw new InvalidArgumentException(
                    'Local activity retry_policy.backoff_seconds cannot exceed the number of possible retries.',
                );
            }
            $canonical['backoff_seconds'] = $backoff;
        }

        if (array_key_exists('non_retryable_error_types', $retryPolicy)) {
            $types = $retryPolicy['non_retryable_error_types'];
            if (!is_array($types) || !array_is_list($types)) {
                throw new InvalidArgumentException(
                    'Local activity retry_policy.non_retryable_error_types must be an ordered list of strings.',
                );
            }
            $canonicalTypes = [];
            foreach ($types as $type) {
                if (!is_string($type) || trim($type) === '') {
                    throw new InvalidArgumentException(
                        'Local activity retry_policy.non_retryable_error_types entries must be non-empty strings.',
                    );
                }
                $canonicalTypes[] = trim($type);
            }
            $canonical['non_retryable_error_types'] = array_values(array_unique($canonicalTypes));
        }

        return $canonical;
    }

    /** @internal Execute a local activity only after replay found no recorded terminal event. */
    public function resolveLocalActivity(PayloadCodec $codec): self
    {
        if ($this->type !== 'record_local_activity' || $this->localActivity === null) {
            throw new LogicException('Only a local-activity command has an in-process executor.');
        }

        $activityType = (string) ($this->attributes['activity_type'] ?? '');
        $arguments = is_array($this->attributes['arguments_value'] ?? null)
            ? array_values($this->attributes['arguments_value'])
            : [];
        $options = $this->attributes;
        unset($options['activity_type'], $options['arguments_value'], $options['execution_mode']);

        // Materialize the durable input before invoking application code. If
        // the value is unsupported, no local side effect has run and task
        // redelivery remains safe.
        $argumentsEnvelope = $codec->envelope($arguments);
        $preExecutionWire = $this->attributes;
        unset($preExecutionWire['arguments_value']);
        $preExecutionWire = array_merge(
            ['type' => $this->type],
            $preExecutionWire,
            ['arguments' => $argumentsEnvelope],
        );
        json_encode($preExecutionWire, JSON_THROW_ON_ERROR);

        try {
            $outcome = ($this->localActivity)($activityType, $arguments, $options);
        } catch (Throwable $exception) {
            $message = trim($exception->getMessage());
            if ($message === '') {
                $message = sprintf('Local activity failed with %s.', $exception::class);
            }
            $outcome = [
                'outcome' => 'failed',
                'message' => $message,
                'exception_type' => $exception::class,
                'non_retryable' => true,
                'attempts' => [[
                    'attempt_number' => 1,
                    'outcome' => 'failed',
                    'duration_ms' => 0,
                    'message' => $message,
                    'exception_type' => $exception::class,
                    'non_retryable' => true,
                    'heartbeats' => [],
                ]],
            ];
        }
        $status = is_string($outcome['outcome'] ?? null) ? $outcome['outcome'] : 'failed';
        $resultEnvelope = null;
        if ($status === 'completed') {
            try {
                $resultEnvelope = $codec->envelope($outcome['result'] ?? null);
            } catch (Throwable $exception) {
                $outcome = self::unencodableLocalActivityResult($outcome, $exception, $codec->name());
                $status = 'failed';
            }
        }
        if ($status !== 'completed' && self::hasOverlongLocalActivityExceptionType($outcome)) {
            $outcome = self::invalidLocalActivityOutcome(
                'Local activity failure metadata exceeded the published exception type limit.',
            );
            $status = 'failed';
            $resultEnvelope = null;
        }

        $postExecutionWire = array_merge($outcome, $preExecutionWire);
        unset($postExecutionWire['result']);
        if ($status === 'completed' && $resultEnvelope !== null) {
            $postExecutionWire['result'] = $resultEnvelope;
        }
        try {
            json_encode($postExecutionWire, JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            $outcome = self::jsonUnsafeLocalActivityOutcome();
            $status = 'failed';
            $resultEnvelope = null;
        }

        $result = $status === 'completed'
            ? ($outcome['result'] ?? null)
            : new \DurableWorkflow\Exception\ActivityFailed(
                is_string($outcome['message'] ?? null) ? $outcome['message'] : 'Local activity failed.',
                $activityType,
                is_string($outcome['exception_type'] ?? null) ? $outcome['exception_type'] : null,
                (bool) ($outcome['non_retryable'] ?? false),
                $outcome,
            );

        $attributes = array_merge($outcome, $this->attributes);
        unset($attributes['result']);
        if ($status === 'completed') {
            $attributes['result_value'] = $outcome['result'] ?? null;
        } else {
            unset($attributes['result_value']);
        }

        return new self(
            $this->type,
            $this->historyShape,
            $attributes,
            $result,
            localActivityArgumentsEnvelope: $argumentsEnvelope,
            localActivityResultEnvelope: $resultEnvelope,
        );
    }

    /**
     * Replace a completed attempt whose result cannot cross the durable codec
     * boundary with an authoritative terminal failure. The activity must not
     * be retried because its side effect has already happened.
     *
     * @param array<string, mixed> $outcome
     * @return array<string, mixed>
     */
    private static function unencodableLocalActivityResult(
        array $outcome,
        Throwable $exception,
        string $codecName,
    ): array {
        $message = sprintf('Local activity result could not be encoded with the %s payload codec.', $codecName);
        $attempts = is_array($outcome['attempts'] ?? null) && array_is_list($outcome['attempts'])
            ? $outcome['attempts']
            : [];
        $lastPosition = max(0, count($attempts) - 1);
        $attempt = isset($attempts[$lastPosition]) && is_array($attempts[$lastPosition])
            ? $attempts[$lastPosition]
            : [];
        unset($attempt['retry_reason'], $attempt['backoff_seconds'], $attempt['timeout_kind']);
        $attempt['attempt_number'] = is_int($attempt['attempt_number'] ?? null)
            ? $attempt['attempt_number']
            : $lastPosition + 1;
        $attempt['outcome'] = 'failed';
        $attempt['message'] = $message;
        $attempt['exception_type'] = $exception::class;
        $attempt['non_retryable'] = true;
        $attempt['heartbeats'] = is_array($attempt['heartbeats'] ?? null)
            && array_is_list($attempt['heartbeats'])
            ? $attempt['heartbeats']
            : [];
        $attempts[$lastPosition] = $attempt;

        unset($outcome['result'], $outcome['timeout_kind']);

        return array_merge($outcome, [
            'outcome' => 'failed',
            'message' => $message,
            'exception_type' => $exception::class,
            'non_retryable' => true,
            'attempts' => array_values($attempts),
        ]);
    }

    /**
     * Record a terminal, non-retryable failure after application code produced
     * outcome metadata that cannot cross the HTTP JSON document boundary.
     *
     * @return array<string, mixed>
     */
    private static function jsonUnsafeLocalActivityOutcome(): array
    {
        return self::invalidLocalActivityOutcome(
            'Local activity failure metadata could not be encoded for the HTTP JSON wire boundary.',
        );
    }

    /** @param array<string, mixed> $outcome */
    private static function hasOverlongLocalActivityExceptionType(array $outcome): bool
    {
        if (is_string($outcome['exception_type'] ?? null)
            && strlen($outcome['exception_type']) > self::MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES
        ) {
            return true;
        }

        $attempts = $outcome['attempts'] ?? null;
        if (!is_array($attempts)) {
            return false;
        }

        foreach ($attempts as $attempt) {
            if (is_array($attempt)
                && is_string($attempt['exception_type'] ?? null)
                && strlen($attempt['exception_type']) > self::MAX_LOCAL_ACTIVITY_EXCEPTION_TYPE_BYTES
            ) {
                return true;
            }
        }

        return false;
    }

    /** @return array<string, mixed> */
    private static function invalidLocalActivityOutcome(string $message): array
    {

        return [
            'outcome' => 'failed',
            'message' => $message,
            'exception_type' => InvalidLocalActivityReport::class,
            'non_retryable' => true,
            'attempts' => [[
                'attempt_number' => 1,
                'outcome' => 'failed',
                'duration_ms' => 0,
                'message' => $message,
                'exception_type' => InvalidLocalActivityReport::class,
                'non_retryable' => true,
                'heartbeats' => [],
            ]],
        ];
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
            $this->localActivity,
            $this->versionResultKind,
            $this->localActivityArgumentsEnvelope,
            $this->localActivityResultEnvelope,
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
                $wire['arguments'] = $this->type === 'record_local_activity'
                    && $this->localActivityArgumentsEnvelope !== null
                    ? $this->localActivityArgumentsEnvelope
                    : $codec->envelope($value);
                continue;
            }
            if ($key === 'result_value') {
                $wire['result'] = match (true) {
                    $this->type === 'record_side_effect' => $codec->encode($value),
                    $this->type === 'record_local_activity' && $this->localActivityResultEnvelope !== null
                        => $this->localActivityResultEnvelope,
                    default => $codec->envelope($value),
                };
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
