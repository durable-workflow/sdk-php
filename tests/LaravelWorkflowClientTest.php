<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClient;
use DurableWorkflow\Bridge\Laravel\Testing\LaravelWorkflowClientFake;
use DurableWorkflow\Bridge\Laravel\WorkflowStartOptions;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Testing\AssertionFailed;
use DurableWorkflow\Testing\WorkflowClientFake;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class LaravelWorkflowClientTest extends TestCase
{
    public function testClassShapedStartDerivesTheRegisteredTypeAndDefaultQueue(): void
    {
        $transport = new WorkflowClientFake();
        $client = new LaravelWorkflowClient($transport, $this->configuration());

        $client->start(
            LaravelClientFixtureWorkflow::class,
            ['Ada'],
            workflowId: 'greeting-1',
        );

        $transport->assertWorkflowStarted(
            'laravel.greeting',
            ['Ada'],
            workflowId: 'greeting-1',
            taskQueue: 'laravel-workers',
        );
        self::addToAssertionCount(1);
    }

    public function testTypedOptionsExposeProtocolTypeAndQueueOverrides(): void
    {
        $transport = new WorkflowClientFake();
        $client = new LaravelWorkflowClient($transport, $this->configuration());

        $client->start(
            LaravelClientFixtureWorkflow::class,
            ['Ada'],
            workflowId: 'greeting-override',
            options: new WorkflowStartOptions(
                taskQueue: 'priority-laravel-workers',
                workflowType: 'partner.greeting',
            ),
        );

        $transport->assertWorkflowStarted(
            'partner.greeting',
            ['Ada'],
            workflowId: 'greeting-override',
            taskQueue: 'priority-laravel-workers',
        );
        self::addToAssertionCount(1);
    }

    public function testLaravelFakeArrangesAndAssertsTheSameClassShapedCall(): void
    {
        $fake = (new LaravelWorkflowClientFake($this->configuration()))
            ->setWorkflowResult('greeting-fake', 'hello from Laravel, Ada');

        $result = $fake->start(
            LaravelClientFixtureWorkflow::class,
            ['Ada'],
            workflowId: 'greeting-fake',
        )->result();

        self::assertSame('hello from Laravel, Ada', $result);
        $fake->assertWorkflowStarted(
            LaravelClientFixtureWorkflow::class,
            ['Ada'],
            workflowId: 'greeting-fake',
        );
        $fake->assertResultRequested('greeting-fake');
    }

    public function testInvalidWorkflowServicesFailBeforeAStartIsRecorded(): void
    {
        $cases = [
            ['Missing\\LaravelWorkflow', 'does not exist'],
            [LaravelClientUnregisteredWorkflow::class, 'Add it to durable-workflow.handlers'],
            [LaravelClientAmbiguousWorkflow::class, 'exactly one non-empty #[Workflow] type'],
        ];

        foreach ($cases as [$workflowService, $message]) {
            $transport = new WorkflowClientFake();
            $configuration = $this->configuration([
                LaravelClientFixtureWorkflow::class,
                LaravelClientAmbiguousWorkflow::class,
            ]);
            $client = new LaravelWorkflowClient($transport, $configuration);

            try {
                $client->start($workflowService, workflowId: 'must-not-start');
                self::fail("{$workflowService} unexpectedly reached the workflow client.");
            } catch (InvalidArgumentException $exception) {
                self::assertStringContainsString($message, $exception->getMessage());
            }

            try {
                $transport->assertWorkflowStarted('laravel.greeting', workflowId: 'must-not-start');
                self::fail("{$workflowService} recorded a transport start before validation.");
            } catch (AssertionFailed) {
                self::addToAssertionCount(1);
            }
        }
    }

    /** @param list<class-string>|null $handlers */
    private function configuration(?array $handlers = null): ServiceConfiguration
    {
        return ServiceConfiguration::fromArray([
            'endpoint' => 'http://localhost:8080',
            'namespace' => 'default',
            'task_queue' => 'laravel-workers',
            'handlers' => $handlers ?? [LaravelClientFixtureWorkflow::class],
        ]);
    }
}

final class LaravelClientFixtureWorkflow
{
    #[Workflow('laravel.greeting')]
    public function run(): void
    {
    }
}

final class LaravelClientUnregisteredWorkflow
{
    #[Workflow('laravel.unregistered')]
    public function run(): void
    {
    }
}

final class LaravelClientAmbiguousWorkflow
{
    #[Workflow('laravel.first')]
    public function first(): void
    {
    }

    #[Workflow('laravel.second')]
    public function second(): void
    {
    }
}
