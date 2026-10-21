<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** The independent activity maintenance process cannot start or communicate. */
final class ActivityLeaseMaintenanceException extends \RuntimeException
{
}
