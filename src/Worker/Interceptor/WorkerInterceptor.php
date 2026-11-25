<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

/**
 * Wraps worker task execution for audit logging, metrics, tracing and policy hooks.
 *
 * Interceptors run in the order passed to the Worker. The first one is the
 * outermost wrapper, and each hook must call `$next` to continue the chain.
 * Extend {@see PassthroughWorkerInterceptor} to implement only some hooks.
 *
 * Workflow-task hooks observe the task; they cannot change it. The Worker
 * always submits the commands the workflow code produced and reports the
 * workflow code's own failure, whatever the hook returns.
 */
interface WorkerInterceptor
{
    /**
     * Wrap the run of workflow code that produces one workflow task's commands.
     *
     * The hook runs once per workflow task attempt. The commands it observes are
     * new for that task, even when {@see WorkflowTaskInterceptorContext::$replaying}
     * shows the workflow code first re-ran a committed prefix; a retried attempt
     * can repeat them. Throwing fails the workflow task, which records nothing
     * and is retried by the Server.
     *
     * @param callable(WorkflowTaskInterceptorContext): WorkflowTaskOutcome $next
     */
    public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome;

    /**
     * Wrap one activity callback, remote or local.
     *
     * The value returned becomes the activity result.
     *
     * @param callable(ActivityInterceptorContext): mixed $next
     */
    public function executeActivity(ActivityInterceptorContext $context, callable $next): mixed;

    /**
     * Wrap one query handler call.
     *
     * The value returned becomes the query result.
     *
     * @param callable(QueryTaskInterceptorContext): mixed $next
     */
    public function executeQueryTask(QueryTaskInterceptorContext $context, callable $next): mixed;

    /**
     * Wrap one update validation task: the run of an update's validator before
     * the Server accepts the update.
     *
     * The hook observes the decision; it cannot change it. The Worker approves
     * or rejects the update from the validator's own decision, and the
     * validator always runs read-only in its own Fiber with the Worker's copy of
     * the task. Throwing, skipping `$next` or calling it more than once answers
     * the task with `update_validation_worker_failed`: the update is not
     * accepted and the caller can retry.
     *
     * @param callable(UpdateValidationInterceptorContext): UpdateValidationOutcome $next
     */
    public function executeUpdateValidation(UpdateValidationInterceptorContext $context, callable $next): UpdateValidationOutcome;
}
