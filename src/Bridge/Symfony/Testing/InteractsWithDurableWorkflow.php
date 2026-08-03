<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony\Testing;

use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\WorkflowClientInterface;

/** Replaces the autowired workflow client in a Symfony KernelTestCase container. */
trait InteractsWithDurableWorkflow
{
    protected function fakeDurableWorkflow(?WorkflowClientFake $fake = null): WorkflowClientFake
    {
        $fake ??= new WorkflowClientFake();
        static::getContainer()->set(WorkflowClientInterface::class, $fake);

        return $fake;
    }
}
