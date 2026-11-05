<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;

/** @internal Require original-byte wire evidence before a refusal can end acceptance. */
final class TaskDeliveryRefusal
{
    public static function matches(ServerException $exception): bool
    {
        $transport = $exception->getPrevious();
        if (!$transport instanceof TransportException || $transport->taskDeliveryRefusal === null) {
            return false;
        }
        $proof = $transport->taskDeliveryRefusal;
        $response = $exception->details;

        return $transport->status === $proof->status && $exception->status === $proof->status
            && $exception->reason === $proof->reason && $response !== null && count($response) === 3
            && array_key_exists('task', $response) && $response['task'] === null
            && ($response['poll_status'] ?? null) === $proof->pollStatus
            && ($response['reason'] ?? null) === $proof->reason;
    }
}
