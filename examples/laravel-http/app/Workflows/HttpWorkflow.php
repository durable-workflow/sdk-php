<?php

declare(strict_types=1);

namespace App\Workflows;

use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;

final class HttpWorkflow
{
    #[Workflow('example.http')]
    public function run(WorkflowContext $context, string $user, string $kind): array
    {
        $echo = $context->activity('example.echo', [$user]);
        if ($kind === 'timer') {
            $context->sleep(1);
        } elseif ($kind === 'recovery-timer') {
            $context->sleep(10);
        } elseif ($kind === 'signal') {
            $context->waitCondition(fn (): bool => $context->signals('release') !== [], key: 'release');
        } elseif ($kind === 'cancel') {
            $context->sleep(300);
        }
        return ['user' => $user, 'echo' => $echo, 'number' => 9007199254740993, 'nullable' => null, 'text' => '雪'];
    }

    #[Query('phase')]
    public function phase(QueryContext $context): string
    {
        return $context->events('ActivityCompleted') === [] ? 'activity' : 'waiting';
    }

    #[Signal('release')]
    public function release(): void
    {
        // The workflow reads committed signal history rather than shared handler memory.
    }
}
