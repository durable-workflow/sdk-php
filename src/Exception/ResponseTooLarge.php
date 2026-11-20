<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** A compressed response decoded to more bytes than the transport accepts. */
final class ResponseTooLarge extends TransportException
{
    public function __construct(public readonly int $limitBytes)
    {
        parent::__construct("The server response decodes to more than {$limitBytes} bytes.",
            response: ['reason' => 'response_too_large', 'limit_bytes' => $limitBytes]);
    }
}
