<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * An activity's result exceeds what the Server accepts, even after the Worker
 * read the Server's current payload policy again. The attempt is reported as a
 * non-retryable failure: running it again would compute the same result.
 */
final class ActivityResultTooLarge extends DurableWorkflowException
{
    public function __construct(ServerException $refusal)
    {
        parent::__construct('The activity result exceeds the Server\'s payload limit: '.$refusal->getMessage(), 0, $refusal);
    }

    /** A size refusal of an activity result: the Server's 413, or an encoding the policy cannot carry. */
    public static function matches(\Throwable $error): bool
    {
        return $error instanceof ServerException && !$error instanceof WorkflowTaskCompletionTooLarge
            && $error->status === 413 && in_array($error->reason, ['payload_too_large', 'external_payload_oversized'], true);
    }
}
