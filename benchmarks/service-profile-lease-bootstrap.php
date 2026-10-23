<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Transport\Psr18Transport;

// Same disposable credentials, endpoint, namespace and HTTP settings as the
// profile process. The independent maintainer executes no application callback.
return new Client((string) getenv('DW_PROFILE_URL'),
    namespace: (string) getenv('DW_PROFILE_NAMESPACE'), workerToken: 'claimed-task-worker',
    transport: new Psr18Transport(new GuzzleHttp\Client([
        'http_errors' => false, 'timeout' => 65, 'connect_timeout' => 5,
    ])));
