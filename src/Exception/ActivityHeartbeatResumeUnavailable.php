<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/**
 * This activity claim does not carry the supported `activity_heartbeat_resume`
 * v1 contract. It is distinct from a supported claim that reports no
 * checkpoint, which returns null instead.
 */
final class ActivityHeartbeatResumeUnavailable extends DurableWorkflowException
{
    public function __construct(string $message = 'activity_heartbeat_resume v1 is unavailable for this claim')
    {
        parent::__construct($message);
    }
}
