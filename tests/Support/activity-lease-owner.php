<?php

declare(strict_types=1);

require $argv[1];
$client = new DurableWorkflow\Client($argv[2], namespace: 'tenant', workerToken: 'fixture-worker');
$poll = $client->pollActivityTaskResponse('owner', 'queue', 0);
$process = new DurableWorkflow\Worker\ActivityLeaseProcess($client, $poll['task'], 'owner', 'queue',
    $poll['server_time'], hrtime(true) / 1e9, false, 10);
$handle = (new ReflectionProperty($process, 'process'))->getValue($process);
echo json_encode(['ready' => true, 'helper_pid' => proc_get_status($handle)['pid']], JSON_THROW_ON_ERROR)."\n";
fflush(STDOUT);
sleep(20);
