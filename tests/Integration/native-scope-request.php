<?php

declare(strict_types=1);

use Illuminate\Contracts\Console\Kernel;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\CancellationScopeRequests;

// Source qualification only. Server has no public per-scope request endpoint.
if (PHP_SAPI !== 'cli' || getenv('DURABLE_WORKFLOW_NATIVE_SCOPE_FIXTURE') !== '1') {
    throw new RuntimeException('Native scope admission is an explicit isolated source fixture.');
}
require '/app/vendor/autoload.php';
$app = require '/app/bootstrap/app.php';
$app->make(Kernel::class)->bootstrap();
if (!$app->environment('testing')) {
    throw new RuntimeException('Native scope admission requires the disposable testing application.');
}
$input = json_decode(stream_get_contents(STDIN), true, flags: JSON_THROW_ON_ERROR);
if (!is_array($input) || count($input) !== 3 || !is_string($input['run_id'] ?? null)
    || !is_string($input['workflow_id'] ?? null) || !is_string($input['scope_id'] ?? null)
    || preg_match('/\Aphp-cooperative-scope-boundary-[a-f0-9]{8}\z/', $input['workflow_id']) !== 1
    || $input['scope_id'] === '' || $input['scope_id'] === 'root') {
    throw new RuntimeException('Native scope fixture requires its original synthetic workflow and scope.');
}
$run = WorkflowRun::query()->where('namespace', 'default')->findOrFail($input['run_id']);
if ($run->workflow_instance_id !== $input['workflow_id']) {
    throw new RuntimeException('Native scope fixture cannot cross its original workflow.');
}
$accepted = CancellationScopeRequests::request($run, $input['scope_id'], '1.20', 30, 'connected scope receipt fixture');
echo json_encode(['history_event_id' => $accepted->id, 'payload' => $accepted->payload], JSON_THROW_ON_ERROR), "\n";
