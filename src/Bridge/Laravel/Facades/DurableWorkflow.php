<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel\Facades;

use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Support\Facades\Facade;

/** Laravel facade for application workflow interactions and test fakes. */
final class DurableWorkflow extends Facade
{
    public static function fake(?WorkflowClientFake $fake = null): WorkflowClientFake
    {
        $fake ??= new WorkflowClientFake();
        $application = static::getFacadeApplication();
        if ($application === null) {
            throw new \RuntimeException('A Laravel application must be bootstrapped before DurableWorkflow::fake().');
        }
        $application->instance(WorkflowClientInterface::class, $fake);
        static::clearResolvedInstance(WorkflowClientInterface::class);

        return $fake;
    }

    protected static function getFacadeAccessor(): string
    {
        return WorkflowClientInterface::class;
    }
}
