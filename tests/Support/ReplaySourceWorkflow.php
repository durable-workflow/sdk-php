<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Worker\WorkflowContext;

final class ReplaySourceWorkflow
{
    public function original(WorkflowContext $context): string
    {
        $context->sleep(5);

        return $this->greet($context);
    }

    public function reordered(WorkflowContext $context): string
    {
        $greeting = $this->greet($context);
        $context->sleep(5);

        return $greeting;
    }

    private function greet(WorkflowContext $context): string
    {
        return $context->activity('greet', ['Ada']);
    }
}
