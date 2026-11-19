<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * The Server refused a workflow-task completion because recording it would exceed
 * its history-transaction budget (409 `history_transaction_limit_exceeded`, or
 * `completion_budget_exceeded` from the early local-activity count).
 *
 * A completion is recorded in one history transaction, bounded by the Server's
 * `history_transaction_size` (events), `history_transaction_bytes` and
 * `history_transaction_rows` structural limits. The refusal rolls the whole
 * transaction back, so nothing was recorded and the task lease is unchanged.
 * Native Server 3.13.0 also names `limit_kind`, the measured `current_value` and
 * the `configured_limit` (durable-workflow/.github#228).
 *
 * Replaying the task produces the same commands, so sending them again gets the
 * same refusal. Like {@see WorkflowTaskCompletionTooLarge}, the worker fails the
 * run instead with a `Structural limit exceeded:` failure.
 */
final class WorkflowTaskHistoryLimitExceeded extends ServerException
{
    public const REASONS = ['history_transaction_limit_exceeded', 'completion_budget_exceeded'];
    public const GUIDANCE = WorkflowTaskCompletionTooLarge::GUIDANCE;
    private const UNITS = [
        'history_transaction_size' => 'history events',
        'history_transaction_bytes' => 'history bytes',
        'history_transaction_rows' => 'rows',
    ];

    /**
     * @param string $limitKind the Server's `limit_kind`, or the refusal reason when it named none
     * @param int|null $currentValue the Server's measured value, in the limit's unit, when it sent one
     * @param int|null $configuredLimit the Server's configured limit, in the same unit, when it sent one
     * @param array<string, mixed> $details the Server's response body
     */
    public function __construct(
        public readonly string $limitKind,
        public readonly ?int $currentValue,
        public readonly ?int $configuredLimit,
        public readonly string $serverMessage,
        string $reason,
        array $details,
        ?\Throwable $previous = null,
    ) {
        parent::__construct(
            ucfirst($this->describe())." ({$this->limitKind}). The same completion will not be sent again. ".self::GUIDANCE
                .($serverMessage === '' ? '' : " Server: {$serverMessage}"),
            409,
            $reason,
            $details,
            $previous,
        );
    }

    /**
     * The typed refusal: a 409 with one of the budget reasons that recorded nothing.
     */
    public static function matches(int $status, mixed $details): bool
    {
        return $status === 409 && is_array($details) && !array_is_list($details)
            && in_array($details['reason'] ?? null, self::REASONS, true)
            && ($details['recorded'] ?? null) !== true;
    }

    /** @param array<string, mixed> $details */
    public static function fromResponse(array $details, ?\Throwable $previous = null): self
    {
        $reason = (string) $details['reason'];
        $kind = $details['limit_kind'] ?? null;
        $current = $details['current_value'] ?? null;
        $limit = $details['configured_limit'] ?? null;
        $message = $details['message'] ?? null;

        return new self(
            is_string($kind) && preg_match('/\A[a-z][a-z0-9_]{0,63}\z/', $kind) === 1 ? $kind : $reason,
            is_int($current) && $current >= 0 ? $current : null,
            is_int($limit) && $limit > 0 ? $limit : null,
            is_string($message) ? self::capped($message) : '',
            $reason,
            $details,
            $previous,
        );
    }

    /**
     * The `fail_workflow` command that ends the run with a structural-limit failure.
     * The Server records `failure_category: structural_limit` for a message with
     * this prefix.
     *
     * @return array{type: 'fail_workflow', message: string, exception_type: string, non_retryable: true}
     */
    public function failWorkflowCommand(): array
    {
        return [
            'type' => 'fail_workflow',
            'message' => 'Structural limit exceeded: '.$this->limitKind.': '.$this->describe().'. '.self::GUIDANCE,
            'exception_type' => 'StructuralLimitExceeded',
            'non_retryable' => true,
        ];
    }

    private function describe(): string
    {
        $unit = self::UNITS[$this->limitKind] ?? 'units';
        $size = $this->currentValue === null ? 'more than the limit allows' : "{$this->currentValue} {$unit}";
        $limit = $this->configuredLimit === null ? 'the server limit' : "the server limit of {$this->configuredLimit} {$unit}";

        return "the workflow task completion would write {$size} in one history transaction, over {$limit}";
    }

    /** The Server's message cut to the same bound as the oversized-completion refusal. */
    private static function capped(string $message): string
    {
        if (strlen($message) <= WorkflowTaskCompletionTooLarge::SERVER_MESSAGE_MAX_BYTES) {
            return $message;
        }
        $cut = substr($message, 0, WorkflowTaskCompletionTooLarge::SERVER_MESSAGE_MAX_BYTES);
        while ($cut !== '' && preg_match('//u', $cut) !== 1) {
            $cut = substr($cut, 0, -1);
        }

        return $cut.'...';
    }
}
