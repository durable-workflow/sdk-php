<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel\Testing;

use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClient;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\WorkflowServiceResolver;
use DurableWorkflow\Bridge\Laravel\WorkflowStartOptions;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\WorkflowHandleInterface;

/** Laravel-native fake for class-shaped workflow starts and handles. */
final class LaravelWorkflowClientFake implements LaravelWorkflowClientInterface
{
    private readonly WorkflowClientFake $client;
    private readonly WorkflowServiceResolver $services;
    private readonly LaravelWorkflowClient $workflows;

    public function __construct(
        private readonly ServiceConfiguration $configuration,
        ?WorkflowClientFake $client = null,
    ) {
        $this->client = $client ?? new WorkflowClientFake();
        $this->services = new WorkflowServiceResolver($this->configuration);
        $this->workflows = new LaravelWorkflowClient(
            $this->client,
            $this->configuration,
            $this->services,
        );
    }

    /**
     * @param class-string $workflowService
     * @param list<mixed> $input
     */
    public function start(
        string $workflowService,
        array $input = [],
        ?string $workflowId = null,
        ?WorkflowStartOptions $options = null,
    ): WorkflowHandleInterface {
        return $this->workflows->start($workflowService, $input, $workflowId, $options);
    }

    /** @param class-string $workflowService */
    public function handle(
        string $workflowService,
        string $workflowId,
        ?string $selectedRunId = null,
    ): WorkflowHandleInterface {
        return $this->workflows->handle($workflowService, $workflowId, $selectedRunId);
    }

    public function setWorkflowResult(string $workflowId, mixed $result): self
    {
        $this->client->setWorkflowResult($workflowId, $result);

        return $this;
    }

    public function setQueryResult(string $workflowId, string $name, mixed $result): self
    {
        $this->client->setQueryResult($workflowId, $name, $result);

        return $this;
    }

    public function setUpdateResult(string $workflowId, string $name, mixed $result): self
    {
        $this->client->setUpdateResult($workflowId, $name, $result);

        return $this;
    }

    /**
     * @param class-string $workflowService
     * @param list<mixed>|null $input
     */
    public function assertWorkflowStarted(
        string $workflowService,
        ?array $input = null,
        ?string $workflowId = null,
        ?WorkflowStartOptions $options = null,
    ): void {
        $options ??= new WorkflowStartOptions();
        $this->client->assertWorkflowStarted(
            $this->services->workflowType($workflowService, $options->workflowType),
            $input,
            $workflowId,
            $this->configuration->taskQueue($options->taskQueue),
        );
    }

    /** @param list<mixed>|null $arguments */
    public function assertSignalSent(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->client->assertSignalSent($workflowId, $name, $arguments);
    }

    /** @param list<mixed>|null $arguments */
    public function assertQueryRequested(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->client->assertQueryRequested($workflowId, $name, $arguments);
    }

    /** @param list<mixed>|null $arguments */
    public function assertUpdateRequested(string $workflowId, string $name, ?array $arguments = null): void
    {
        $this->client->assertUpdateRequested($workflowId, $name, $arguments);
    }

    public function assertResultRequested(string $workflowId): void
    {
        $this->client->assertResultRequested($workflowId);
    }

    /** Framework-neutral fake retained for the low-level injectable interface. */
    public function workflowClient(): WorkflowClientFake
    {
        return $this->client;
    }
}
