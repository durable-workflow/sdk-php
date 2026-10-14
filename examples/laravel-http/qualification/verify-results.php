<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

$prefix = getenv('EXAMPLE_RUN_PREFIX');
if (!is_string($prefix) || preg_match('/^[a-zA-Z0-9_-]{1,40}$/', $prefix) !== 1) {
    throw new InvalidArgumentException('Select the original admitted run prefix.');
}
$client = new DurableWorkflow\Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);
foreach ([['alpha', 'short-a'], ['beta', 'short-b'], ['alpha', 'timer'], ['beta', 'signal']] as [$user, $suffix]) {
    $result = $client->workflowHandle($user.'-'.$prefix.'-'.$suffix)->result(15);
    $expected = ['user' => $user, 'echo' => 'hello '.$user, 'number' => 9007199254740993, 'nullable' => null, 'text' => '雪'];
    if ($result !== $expected) {
        throw new RuntimeException('The original typed workflow output changed.');
    }
}
echo "Four original typed results pass, including exact int64/null/Unicode.\n";
