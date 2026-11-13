<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * The Server refused a workflow-task completion because the whole request is over
 * its completion size limit (413 `payload_too_large`, `limit_kind:
 * workflow_task_completion_bytes`, Server 3.6.0).
 *
 * The refusal happens before the Server reads the completion, so nothing was
 * recorded and the task lease is unchanged. Sending the same completion again
 * gets the same refusal: replaying the task produces the same commands. The
 * worker fails the run instead with a `Structural limit exceeded:` failure.
 */
final class WorkflowTaskCompletionTooLarge extends ServerException
{
    public const REASON = 'payload_too_large';
    public const LIMIT_KIND = 'workflow_task_completion_bytes';
    /** The most bytes of the Server's message kept on this error and in its logs. */
    public const SERVER_MESSAGE_MAX_BYTES = 512;
    public const GUIDANCE = 'Split the fan-out into smaller batches, move work into child workflows, or continue-as-new; '
        .'offload large payloads with external payload storage.';

    /**
     * @param int|null $limitBytes the Server's completion limit (`limit`), when it sent one
     * @param int $sizeBytes the encoded size of the completion this SDK sent
     * @param array<string, mixed> $details the Server's response body
     */
    public function __construct(
        public readonly ?int $limitBytes,
        public readonly int $sizeBytes,
        public readonly string $serverMessage,
        array $details,
        ?\Throwable $previous = null,
    ) {
        $limit = $limitBytes === null ? 'its limit' : "the limit of {$limitBytes} bytes";
        parent::__construct(
            "The workflow task completion is {$sizeBytes} bytes, over the Server's {$limit} "
            .'('.self::LIMIT_KIND.'). The same completion will not be sent again. '.self::GUIDANCE
            .($serverMessage === '' ? '' : " Server: {$serverMessage}"),
            413,
            self::REASON,
            $details,
            $previous,
        );
    }

    /** The typed refusal, recognized only by its status, reason and limit kind. */
    public static function matches(int $status, mixed $details): bool
    {
        return $status === 413 && is_array($details) && !array_is_list($details)
            && ($details['reason'] ?? null) === self::REASON
            && ($details['limit_kind'] ?? null) === self::LIMIT_KIND;
    }

    /** @param array<string, mixed> $details */
    public static function fromResponse(array $details, int $sizeBytes, ?\Throwable $previous = null): self
    {
        $limit = $details['limit'] ?? null;

        return new self(
            is_int($limit) && $limit > 0 ? $limit : null,
            $sizeBytes,
            is_string($details['message'] ?? null) ? self::capped($details['message']) : '',
            $details,
            $previous,
        );
    }

    /** The Server's message cut to SERVER_MESSAGE_MAX_BYTES at a UTF-8 character boundary. */
    private static function capped(string $message): string
    {
        if (strlen($message) <= self::SERVER_MESSAGE_MAX_BYTES) {
            return $message;
        }
        $cut = substr($message, 0, self::SERVER_MESSAGE_MAX_BYTES);
        while ($cut !== '' && preg_match('//u', $cut) !== 1) {
            $cut = substr($cut, 0, -1);
        }

        return $cut.'...';
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
        $limit = $this->limitBytes === null ? 'the server limit' : "the server limit of {$this->limitBytes} bytes";

        return [
            'type' => 'fail_workflow',
            'message' => 'Structural limit exceeded: '.self::LIMIT_KIND.": the workflow task completion is {$this->sizeBytes} bytes, "
                ."over {$limit}. ".self::GUIDANCE,
            'exception_type' => 'StructuralLimitExceeded',
            'non_retryable' => true,
        ];
    }
}
