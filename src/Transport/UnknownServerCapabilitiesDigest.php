<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\TransportException;

/** @internal An answer named capabilities this transport does not hold; recovered without repeating the request. */
final class UnknownServerCapabilitiesDigest extends TransportException
{
    /** @param array<mixed>|null $answer The decoded answer as received, when it was a success. */
    public function __construct(public readonly string $digest, public readonly ?array $answer = null)
    {
        parent::__construct('The server omitted capabilities this worker does not hold.');
    }

    /** @param array<mixed> $answer */
    public function withAnswer(array $answer): self
    {
        return new self($this->digest, $answer);
    }
}
