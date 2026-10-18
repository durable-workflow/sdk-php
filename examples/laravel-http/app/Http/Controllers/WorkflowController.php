<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Workflows\HttpWorkflow;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\WorkflowStartOptions;
use Illuminate\Http\Request;

final class WorkflowController extends Controller
{
    public function __construct(private readonly LaravelWorkflowClientInterface $workflows)
    {
    }

    public function start(Request $request)
    {
        $values = $request->validate(['id' => 'required|alpha_dash|max:60', 'kind' => 'required|in:short,timer,recovery-timer,signal,cancel']);
        $handle = $this->workflows->start(
            HttpWorkflow::class,
            [$request->attributes->get('example_user'), $values['kind']],
            workflowId: $this->workflowId($request, $values['id']),
            options: new WorkflowStartOptions(executionTimeoutSeconds: 900, runTimeoutSeconds: 600),
        );

        return response()->json(['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId], 201);
    }

    public function status(Request $request, string $id)
    {
        $execution = $this->workflows->handle(HttpWorkflow::class, $this->workflowId($request, $id))->describe();

        return response()->json(['status' => $execution->status, 'output' => $execution->output]);
    }

    public function signal(Request $request, string $id)
    {
        $this->workflows->handle(HttpWorkflow::class, $this->workflowId($request, $id))->signal('release');

        return response()->noContent();
    }

    public function query(Request $request, string $id)
    {
        return response()->json(['phase' => $this->workflows->handle(HttpWorkflow::class, $this->workflowId($request, $id))->query('phase')]);
    }

    public function cancel(Request $request, string $id)
    {
        $this->workflows->handle(HttpWorkflow::class, $this->workflowId($request, $id))->cancel('example cancellation');

        return response()->noContent();
    }

    private function workflowId(Request $request, string $id): string
    {
        abort_unless(preg_match('/^[a-zA-Z0-9_-]{1,60}$/', $id) === 1, 422);

        return $request->attributes->get('example_user').'-'.$id;
    }
}
