<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional knowledge of a lower encoded request-body limit. */
interface RequestSizeLimitTransport
{
    public function maximumRequestBytes(): ?int;
}
