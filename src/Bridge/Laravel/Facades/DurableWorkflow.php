<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel\Facades;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\Testing\LaravelWorkflowClientFake;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\WorkflowClientInterface;
use DurableWorkflow\WorkflowHandleInterface;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Facade;

/**
 * Laravel facade for application workflow interactions and test fakes.
 *
 * @method static WorkflowHandleInterface start(string $workflowService, list<mixed> $input = [], ?string $workflowId = null, ?\DurableWorkflow\Bridge\Laravel\WorkflowStartOptions $options = null)
 * @method static WorkflowHandleInterface handle(string $workflowService, string $workflowId, ?string $selectedRunId = null)
 */
final class DurableWorkflow extends Facade
{
    public static function fake(?LaravelWorkflowClientFake $fake = null): LaravelWorkflowClientFake
    {
        $application = static::durableWorkflowApplication();
        if (!$application instanceof Application) {
            throw new \RuntimeException('A Laravel application must be bootstrapped before DurableWorkflow::fake().');
        }
        $fake ??= new LaravelWorkflowClientFake($application->make(ServiceConfiguration::class));
        $application->instance(WorkflowClientInterface::class, $fake->workflowClient());
        $application->instance(LaravelWorkflowClientInterface::class, $fake);
        static::clearResolvedInstance(WorkflowClientInterface::class);
        static::clearResolvedInstance(LaravelWorkflowClientInterface::class);

        return $fake;
    }

    protected static function getFacadeAccessor(): string
    {
        return LaravelWorkflowClientInterface::class;
    }

    /** Normalize the nullable runtime state hidden by older Laravel PHPDoc. */
    private static function durableWorkflowApplication(): mixed
    {
        return static::getFacadeApplication();
    }
}
