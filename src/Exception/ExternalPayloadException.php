<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** Runtime payload transport/integrity failure, identified by the stable reason field. */
class ExternalPayloadException extends ServerException
{
}
