<?php

declare(strict_types=1);

// Workflows and activities for the installed-lifecycle qualification. They use
// only the public API of the normally installed SDK under test.

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

final class LifecycleWorkflow
{
    #[Workflow('installed.lifecycle')]
    public function run(WorkflowContext $context, string $name): array
    {
        $greeting = $context->activity('installed.greet', [$name]);
        $context->waitCondition(static fn (): bool => $context->signals('approve') !== [], 'approval');
        $renames = $context->updates('rename');

        return [
            'greeting' => $greeting,
            'approved_by' => $context->signals('approve')[0][0] ?? null,
            'renames' => array_map(static fn (array $arguments): mixed => $arguments[0] ?? null, $renames),
        ];
    }

    #[Query('status')]
    public function status(QueryContext $context): array
    {
        $types = [];
        foreach ($context->history as $event) {
            $types[] = $event['event_type'] ?? $event['type'] ?? null;
        }

        return ['events' => count($types), 'activity_completed' => in_array('ActivityCompleted', $types, true)];
    }

    #[Signal('approve')]
    public function approve(string $approver): void
    {
    }

    #[Update('rename')]
    public function rename(QueryContext $context, string $name): string
    {
        return 'renamed:'.$name;
    }
}

final class CancellableWorkflow
{
    #[Workflow('installed.cancellable')]
    public function run(WorkflowContext $context): string
    {
        $context->waitCondition(static fn (): bool => $context->signals('never') !== [], 'never');

        return 'not cancelled';
    }

    #[Signal('never')]
    public function never(): void
    {
    }
}

final class FailingWorkflow
{
    #[Workflow('installed.failing')]
    public function run(WorkflowContext $context, string $reason): string
    {
        return $context->activity('installed.reject', [$reason], ['retry_policy' => ['max_attempts' => 1]]);
    }
}

final class InstalledActivities
{
    #[Activity('installed.greet')]
    public function greet(ActivityContext $context, string $name): string
    {
        return 'hello, '.$name;
    }

    #[Activity('installed.reject')]
    public function reject(ActivityContext $context, string $reason): string
    {
        throw new \DomainException('rejected: '.$reason);
    }
}
