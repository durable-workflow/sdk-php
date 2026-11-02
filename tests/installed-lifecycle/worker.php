<?php

declare(strict_types=1);

require '/consumer/vendor/autoload.php';
require __DIR__.'/workflows.php';

use DurableWorkflow\Client;
use DurableWorkflow\Worker;

$client = new Client((string) getenv('DW_URL'), token: (string) getenv('DW_TOKEN'));

Worker::create($client, (string) getenv('DW_TASK_QUEUE'))
    ->register(LifecycleWorkflow::class, CancellableWorkflow::class, FailingWorkflow::class, InstalledActivities::class)
    ->run();
