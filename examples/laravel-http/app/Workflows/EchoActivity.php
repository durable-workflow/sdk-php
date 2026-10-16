<?php

declare(strict_types=1);

namespace App\Workflows;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Worker\ActivityContext;

final class EchoActivity
{
    #[Activity('example.echo')]
    public function run(ActivityContext $context, string $user): string
    {
        return 'hello '.$user;
    }
}
