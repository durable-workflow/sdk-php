<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional status metadata; existing custom transports remain supported. */
interface ResponseStatusTransport
{
    /** Status of the latest document send, or null when no response was received. */
    public function lastResponseStatus(): ?int;
}
