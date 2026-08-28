<?php

declare(strict_types=1);

namespace DurableWorkflow\Exception;

/** A local activity produced a report that cannot fit the bounded wire contract. */
final class InvalidLocalActivityReport extends DurableWorkflowException
{
}
