<?php

declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

$prefix = getenv('EXAMPLE_RUN_PREFIX');
$seconds = filter_var(getenv('EXAMPLE_DURATION_SECONDS'), FILTER_VALIDATE_INT);
$mixed = getenv('EXAMPLE_MIXED') === 'true';
if (!is_string($prefix) || preg_match('/^[a-zA-Z0-9_-]{1,30}$/', $prefix) !== 1
    || !is_int($seconds) || $seconds < 5 || $seconds > 300) {
    throw new InvalidArgumentException('Use the original load prefix, duration and mixed setting.');
}
$client = new DurableWorkflow\Client(getenv('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: getenv('DURABLE_WORKFLOW_NAMESPACE'), controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN'));
$completed = $cancelled = $eventsRead = 0;
$timers = [];
function timestamp(string $value): float
{
    if (preg_match('/(?:Z|[+-][0-9]{2}:[0-9]{2})$/', $value) !== 1) throw new RuntimeException('Persisted timestamp lacks an explicit zone.');

    return (float) (new DateTimeImmutable($value))->format('U.u');
}
for ($index = 0; $index < $seconds; ++$index) {
    $user = $index % 2 === 0 ? 'alpha' : 'beta';
    $kind = $mixed ? ['short', 'short', 'timer', 'signal', 'cancel'][$index % 5] : 'short';
    $id = $user.'-'.$prefix.'-'.$kind.'-'.$index;
    $execution = $client->describeWorkflow($id);
    if (!is_string($execution->runId) || $execution->runId === '') throw new RuntimeException('Missing original '.$id);
    $handle = $client->workflowHandle($id, $execution->runId);
    $history = $client->workflowHistory($id, $execution->runId, pageSize: 100);
    if (($history['next_page_token'] ?? null) !== null || !is_array($history['events'] ?? null)) {
        throw new RuntimeException('Missing complete bounded original history: '.$id);
    }
    $events = $history['events'];
    $counts = array_count_values(array_column($events, 'event_type'));
    $eventsRead += count($events);
    foreach (['ActivityScheduled', 'ActivityStarted', 'ActivityCompleted'] as $name) {
        if (($counts[$name] ?? 0) !== 1) throw new RuntimeException('Missing/duplicate '.$name.': '.$id);
    }
    if (isset($counts['RepairRequested']) || isset($counts['ActivityFailed'])) throw new RuntimeException('Original contains repair/failure: '.$id);
    foreach (['run_deadline_at', 'execution_deadline_at'] as $name) {
        if (!is_string($execution->raw[$name] ?? null) || $execution->raw[$name] === '') throw new RuntimeException('Missing deadline: '.$id);
    }
    if ($kind === 'cancel') {
        if (strtolower((string) $execution->status) !== 'cancelled' || ($counts['WorkflowCancelled'] ?? 0) !== 1
            || isset($counts['WorkflowCompleted']) || isset($counts['TimerFired'])) throw new RuntimeException('Cancellation differs: '.$id);
        ++$cancelled;
        continue;
    }
    if (strtolower((string) $execution->status) !== 'completed' || ($counts['WorkflowCompleted'] ?? 0) !== 1) {
        throw new RuntimeException('Original is not completed exactly once: '.$id);
    }
    if ($handle->resultOfSelectedRun(5) !== ['user' => $user, 'echo' => 'hello '.$user,
        'number' => 9007199254740993, 'nullable' => null, 'text' => '雪']) throw new RuntimeException('Typed result differs: '.$id);
    if ($kind === 'timer') {
        if (($counts['TimerScheduled'] ?? 0) !== 1 || ($counts['TimerFired'] ?? 0) !== 1) throw new RuntimeException('Timer differs: '.$id);
        $fired = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'TimerFired'))[0];
        $fireAt = $fired['payload']['fire_at'] ?? null;
        $firedAt = $fired['payload']['fired_at'] ?? null;
        if (!is_string($fireAt) || !is_string($firedAt)) throw new RuntimeException('Timer clock evidence missing: '.$id);
        $lateMs = (timestamp($firedAt) - timestamp($fireAt)) * 1000;
        if ($lateMs < 0) throw new RuntimeException('Timer fired early: '.$id);
        $timers[] = ['workflow_id' => $id, 'run_id' => $execution->runId, 'due_to_fire_ms' => $lateMs];
    }
    ++$completed;
}
echo json_encode(['prefix' => $prefix, 'originals' => $seconds, 'completed' => $completed,
    'cancelled' => $cancelled, 'events_read' => $eventsRead, 'typed_outputs_exact' => true, 'timers' => $timers], JSON_THROW_ON_ERROR), PHP_EOL;
