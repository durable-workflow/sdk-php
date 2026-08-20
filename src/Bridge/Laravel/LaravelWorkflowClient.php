<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\WorkflowClientInterface;
use DurableWorkflow\WorkflowHandleInterface;

/** Resolves configured Laravel workflow services to their public protocol types. */
final class LaravelWorkflowClient implements LaravelWorkflowClientInterface
{
    private readonly WorkflowServiceResolver $services;

    public function __construct(
        private readonly WorkflowClientInterface $client,
        private readonly ServiceConfiguration $configuration,
        ?WorkflowServiceResolver $services = null,
    ) {
        $this->services = $services ?? new WorkflowServiceResolver($this->configuration);
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
        $options ??= new WorkflowStartOptions();
        $workflowType = $this->services->workflowType($workflowService, $options->workflowType);
        $workflowId ??= $this->workflowId($workflowType);

        return $this->client->startWorkflow(
            workflowType: $workflowType,
            workflowId: $workflowId,
            taskQueue: $this->configuration->taskQueue($options->taskQueue),
            input: $input,
            executionTimeoutSeconds: $options->executionTimeoutSeconds,
            runTimeoutSeconds: $options->runTimeoutSeconds,
            duplicatePolicy: $options->duplicatePolicy,
            memo: $options->memo,
            searchAttributes: $options->searchAttributes,
            priority: $options->priority,
            fairnessKey: $options->fairnessKey,
            fairnessWeight: $options->fairnessWeight,
            buildId: $options->buildId,
        );
    }

    /** @param class-string $workflowService */
    public function handle(
        string $workflowService,
        string $workflowId,
        ?string $selectedRunId = null,
    ): WorkflowHandleInterface {
        $this->services->workflowType($workflowService);

        return $this->client->workflowHandle($workflowId, $selectedRunId);
    }

    private function workflowId(string $workflowType): string
    {
        $prefix = trim((string) preg_replace('/[^a-z0-9]+/i', '-', $workflowType), '-');
        if ($prefix === '') {
            $prefix = 'workflow';
        }

        return strtolower($prefix).'-'.bin2hex(random_bytes(12));
    }
}
