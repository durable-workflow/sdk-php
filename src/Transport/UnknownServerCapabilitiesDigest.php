<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;

/** @internal An answer named capabilities this transport does not hold; recovered by asking again in full. */
final class UnknownServerCapabilitiesDigest extends TransportException
{
    public function __construct()
    {
        parent::__construct('The server omitted capabilities this worker does not hold.');
    }
}
