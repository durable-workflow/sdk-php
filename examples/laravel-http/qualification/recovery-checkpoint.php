<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

[$script, $phase, $workflowId, $runId, $checkpointPath] = $argv + array_fill(0, 5, null);
if (!in_array($phase, ['before', 'after'], true) || !is_string($workflowId) || !is_string($runId) || !is_string($checkpointPath)) {
    throw new InvalidArgumentException('Usage: recovery-checkpoint.php before|after ORIGINAL_WORKFLOW_ID ORIGINAL_RUN_ID CHECKPOINT_FILE');
}
$client = new DurableWorkflow\Client(
    getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);
$execution = $client->describeWorkflow($workflowId, $runId);
$page = $client->workflowHistory($workflowId, $runId, pageSize: 100);
if (($page['next_page_token'] ?? null) !== null) {
    throw new RuntimeException('The bounded recovery recipe unexpectedly has another history page.');
}
$history = $page['events'] ?? null;
if (!is_array($history) || $history === [] || !array_is_list($history)) {
    throw new RuntimeException('Canonical original history is unavailable.');
}
$counts = array_count_values(array_column($history, 'event_type'));
if (($counts['ActivityCompleted'] ?? 0) !== 1 || ($counts['ActivityScheduled'] ?? 0) !== 1 || ($counts['TimerScheduled'] ?? 0) !== 1) {
    throw new RuntimeException('The original activity or durable timer is missing or duplicated.');
}
$deadlines = [];
foreach (['run_deadline_at', 'execution_deadline_at'] as $name) {
    $value = $execution->raw[$name] ?? null;
    if (!is_string($value) || $value === '') throw new RuntimeException('The original deadline is unavailable.');
    $deadlines[$name] = $value;
}
if ($phase === 'before') {
    if (strtolower((string) $execution->status) !== 'waiting' || isset($counts['TimerFired']) || isset($counts['WorkflowCompleted'])) {
        throw new RuntimeException('The original is not waiting at its unfired durable timer.');
    }
    $checkpoint = ['workflow_id' => $workflowId, 'run_id' => $runId, 'history' => $history, 'deadlines' => $deadlines];
    if (file_put_contents($checkpointPath, json_encode($checkpoint, JSON_THROW_ON_ERROR)) === false) {
        throw new RuntimeException('Cannot retain the temporary original checkpoint.');
    }
} else {
    $before = json_decode(file_get_contents($checkpointPath), true, 512, JSON_THROW_ON_ERROR);
    if ($before['workflow_id'] !== $workflowId || $before['run_id'] !== $runId
        || $before['deadlines'] !== $deadlines || array_slice($history, 0, count($before['history'])) !== $before['history']) {
        throw new RuntimeException('Recovery changed the original identity, history prefix or deadlines.');
    }
    if (strtolower((string) $execution->status) !== 'completed' || ($counts['TimerFired'] ?? 0) !== 1 || ($counts['WorkflowCompleted'] ?? 0) !== 1) {
        throw new RuntimeException('The same original did not complete once after its timer.');
    }
    $result = $client->workflowHandle($workflowId, $runId)->resultOfSelectedRun(15);
    $expected = ['user' => 'alpha', 'echo' => 'hello alpha', 'number' => 9007199254740993, 'nullable' => null, 'text' => '雪'];
    if ($result !== $expected) throw new RuntimeException('The original typed result changed.');
}
echo json_encode(['phase' => $phase, 'workflow_id' => $workflowId, 'run_id' => $runId, 'events' => $counts], JSON_THROW_ON_ERROR), PHP_EOL;
