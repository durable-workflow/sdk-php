<?php

declare(strict_types=1);

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

require dirname(__DIR__, 2) . '/vendor/autoload.php';

if ($argc !== 2) {
    throw new RuntimeException('Expected one canonical persisted selection fixture path.');
}

$fixture = json_decode((string) file_get_contents($argv[1]), true, flags: JSON_THROW_ON_ERROR);
$history = $fixture['history'] ?? null;
if (! is_array($history)) {
    throw new RuntimeException('Canonical persisted selection fixture has no history array.');
}

$codec = new AvroPayloadCodec();
$workflow = static function (WorkflowContext $context): array {
    $selected = $context->select([
        'slow' => static fn () => $context->activity('slow-activity'),
        'fast' => static fn () => $context->activity('fast-activity'),
    ]);

    return [
        'winner' => $selected->key,
        'winner_value' => $selected->result(),
        'slow' => $selected->handles['slow']->await(),
    ];
};
$result = (new Replayer($codec))->replay($workflow, $history, [], 'default');
if (count($result->commands) !== 1 || ($result->commands[0]['type'] ?? null) !== 'complete_workflow') {
    throw new RuntimeException('Canonical persisted selection history did not complete on cold replay.');
}

echo json_encode([
    'process_id' => getmypid(),
    ...$codec->decodeEnvelope($result->commands[0]['result']),
], JSON_THROW_ON_ERROR);
