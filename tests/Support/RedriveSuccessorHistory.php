<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;

/**
 * Checks the supported replay-fixture shapes against Workflow 2.19 redrive.
 * This is a test evidence model. Scoped, grouped, selection and service-operation
 * deliveries remain outside its qualified single-call boundary.
 *
 * Core's DefaultWorkflowControlPlane::redrive() records, in one transaction,
 * a WorkflowStarted carrying the successor identity and the redrive envelope,
 * then the reused prefix from FailedRunRedrivePlan::successorEvents(). Every
 * reused event carries reused_from_run_id and reused_recorded_at, except the
 * TimerScheduled that successorEvents() copies, without them and under a new
 * timer_id, directly before a reused TimerFired of the same timer. Rows the
 * successor records itself come after that prefix, at or after its start, and
 * belong to the successor run.
 */
final class RedriveSuccessorHistory
{
    private const IDENTITY = [
        'workflow_type', 'workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'workflow_link_id', 'continued_from_run_id',
    ];

    private const REUSE_KEYS = ['reused_from_run_id', 'reused_recorded_at'];

    /** Reused events that do not occupy a workflow step of their own. */
    private const STEPLESS = ['SignalReceived'];

    /**
     * Lifecycle events of one durable step, and the payload field that names
     * that step. Only events of the same lifecycle may repeat a sequence.
     * Memo, search-attribute, side-effect and version-marker events are one
     * event per step.
     */
    private const LIFECYCLES = [
        'SignalWaitOpened' => ['signal_wait', 'signal_wait_id'],
        'SignalApplied' => ['signal_wait', 'signal_wait_id'],
        'ActivityScheduled' => ['activity', 'activity_execution_id'],
        'ActivityStarted' => ['activity', 'activity_execution_id'],
        'ActivityCompleted' => ['activity', 'activity_execution_id'],
        'TimerScheduled' => ['timer', 'timer_id'],
        'TimerFired' => ['timer', 'timer_id'],
        'ConditionWaitOpened' => ['condition_wait', 'condition_wait_id'],
        'ConditionWaitSatisfied' => ['condition_wait', 'condition_wait_id'],
    ];

    /** Core FailedRunRedrivePlan::isPlainTimer: redrive copies only plain timers. */
    private const NON_PLAIN_TIMER_KEYS = [
        'timer_kind', 'cancellation_cleanup', 'cancellation_scope_id', 'parallel_group_id', 'condition_wait_id', 'signal_wait_id',
    ];

    private const ACTIVITY_TERMINALS = ['ActivityCompleted', 'ActivityFailed', 'ActivityCancelled', 'ActivityTimedOut'];

    private const TIMER_TERMINALS = ['TimerFired', 'TimerCancelled'];

    /**
     * The admitted single-call families and their canonical resource/terminal
     * pairs. Derive terminal and delivery admission from this same closed set.
     * Service operations, groups and selection deliveries remain unqualified.
     */
    private const CALL_RESOLUTIONS = [
        'activity' => ['activity_execution_id', self::ACTIVITY_TERMINALS],
        'local_activity' => ['activity_execution_id', self::ACTIVITY_TERMINALS],
        'timer' => ['timer_id', self::TIMER_TERMINALS],
        'condition' => ['condition_wait_id', ['ConditionWaitSatisfied', 'ConditionWaitTimedOut', ...self::TIMER_TERMINALS]],
        'signal' => ['signal_wait_id', ['SignalApplied', ...self::TIMER_TERMINALS]],
        'child' => ['child_call_id', ['ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated']],
    ];

    private const SERVICE_OPERATION_EVENTS = [
        'ServiceOperationScheduled', 'ServiceCallStarted', 'ServiceCallCompleted', 'ServiceCallFailed', 'ServiceCallCancelled',
    ];

    /**
     * @param array<string, mixed> $fixture a replay-regression fixture with history, workflow and workflow_tasks
     * @param list<string> $reusedEventTypes the reused prefix, in order, exactly as redrive copies it
     * @return string|null the first violated rule, or null for a valid successor
     */
    public static function violation(array $fixture, array $reusedEventTypes): ?string
    {
        $history = is_array($fixture['history'] ?? null) ? $fixture['history'] : [];
        $task = $fixture['workflow_tasks'][0] ?? null;
        $workflowType = $fixture['workflow']['type'] ?? null;
        if ($reusedEventTypes === []) {
            return 'a redrive successor needs at least one expected reused event type';
        }
        $started = $history[0] ?? null;
        if (!is_array($started) || ($started['event_type'] ?? null) !== 'WorkflowStarted' || ($started['sequence'] ?? null) !== 1) {
            return 'history must open with WorkflowStarted at sequence 1';
        }
        $start = is_array($started['payload'] ?? null) ? $started['payload'] : [];
        foreach (self::IDENTITY as $field) {
            if (!self::nonBlank($start[$field] ?? null)) {
                return "WorkflowStarted {$field} must be a non-blank string";
            }
        }
        if ($start['workflow_type'] !== $workflowType) {
            return 'WorkflowStarted workflow_type must be the fixture workflow type';
        }
        $instance = $start['workflow_instance_id'];
        $run = $start['workflow_run_id'];
        $source = $start['continued_from_run_id'];
        if ($source === $run) {
            return 'continued_from_run_id must name a different run than workflow_run_id';
        }
        if (($start['recovery_kind'] ?? null) !== 'redrive') {
            return 'recovery_kind must be redrive';
        }
        $startedAt = self::time($started['recorded_at'] ?? null);
        $replayedStartedAt = self::time($start['replayed_started_at'] ?? null);
        if ($startedAt === null || $replayedStartedAt === null || $replayedStartedAt >= $startedAt) {
            return 'replayed_started_at must be a valid time before the successor start';
        }
        // Core writes both deadlines into the envelope, null when unbounded.
        foreach (['execution_deadline_at' => $replayedStartedAt, 'run_deadline_at' => $startedAt] as $field => $after) {
            if (!array_key_exists($field, $start)) {
                return "WorkflowStarted must carry {$field}";
            }
            if ($start[$field] !== null) {
                $deadline = self::time($start[$field]);
                if ($deadline === null || $deadline <= $after) {
                    return "{$field} must be null or a valid time after the ".($field === 'run_deadline_at' ? 'successor' : 'workflow').' start';
                }
            }
        }
        if (!is_array($task) || ($task['workflow_id'] ?? null) !== $instance || ($task['run_id'] ?? null) !== $run) {
            return 'workflow task must belong to the successor instance and run';
        }

        $lastReusedStep = 0;
        $reusedSteps = [];
        $stepOwner = null;
        $previousAt = $startedAt;
        $prefixLength = count($reusedEventTypes);
        $requests = [];
        $deliveries = [];
        foreach (array_slice($history, 1) as $offset => $event) {
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $type = (string) ($event['event_type'] ?? '');
            if (in_array($type, self::SERVICE_OPERATION_EVENTS, true)) {
                return 'service-operation histories are outside the qualified successor evidence model';
            }
            if (($event['sequence'] ?? null) !== $offset + 2) {
                return "history order: {$type} must be history sequence ".($offset + 2);
            }
            $recordedAt = self::time($event['recorded_at'] ?? $event['timestamp'] ?? null);
            if ($recordedAt === null || $recordedAt < $previousAt) {
                return "history order: {$type} must not predate the event before it";
            }
            $previousAt = $recordedAt;
            if ($offset < $prefixLength) {
                if ($type !== $reusedEventTypes[$offset]) {
                    return "reused prefix position {$offset} must be {$reusedEventTypes[$offset]}, not {$type}";
                }
                if ($type === 'TimerScheduled') {
                    // Core copies a fired timer's schedule under a new timer_id
                    // and without reuse keys; the reused TimerFired that must
                    // follow it carries them.
                    $fired = $history[$offset + 2] ?? null;
                    $firedPayload = is_array($fired['payload'] ?? null) ? $fired['payload'] : [];
                    if (($reusedEventTypes[$offset + 1] ?? null) !== 'TimerFired'
                        || ($fired['event_type'] ?? null) !== 'TimerFired'
                        || !self::nonBlank($payload['timer_id'] ?? null)
                        || ($firedPayload['timer_id'] ?? null) !== $payload['timer_id']
                        || ($firedPayload['sequence'] ?? null) !== ($payload['sequence'] ?? null)) {
                        return 'copied TimerScheduled must be followed by the reused TimerFired of the same timer_id and step';
                    }
                    foreach (self::REUSE_KEYS as $key) {
                        if (array_key_exists($key, $payload)) {
                            return "copied TimerScheduled must not carry {$key}";
                        }
                    }
                    foreach (self::NON_PLAIN_TIMER_KEYS as $key) {
                        if (($payload[$key] ?? null) !== null) {
                            return "copied TimerScheduled must be a plain timer, without {$key}";
                        }
                    }
                } else {
                    foreach (self::REUSE_KEYS as $key) {
                        if (!self::nonBlank($payload[$key] ?? null)) {
                            return "reused {$type} must carry a non-blank {$key}";
                        }
                    }
                    if ($payload['reused_from_run_id'] !== $source) {
                        return "reused {$type} reused_from_run_id must match continued_from_run_id";
                    }
                }
                if ($recordedAt != $startedAt) {
                    return "reused {$type} is recorded with the successor start";
                }
                if ($type !== 'TimerScheduled') {
                    $sourceAt = self::time($payload['reused_recorded_at']);
                    if ($sourceAt === null || $sourceAt < $replayedStartedAt || $sourceAt >= $startedAt) {
                        return "reused {$type} reused_recorded_at must fall in the source run";
                    }
                }
                if (array_key_exists('workflow_run_id', $payload) && $payload['workflow_run_id'] !== $source) {
                    return "reused {$type} keeps the source run id";
                }
                if (array_key_exists('workflow_instance_id', $payload) && $payload['workflow_instance_id'] !== $instance) {
                    return "reused {$type} must belong to the successor instance";
                }
                // SignalReceived has no workflow step of its own; it belongs to its wait.
                if (!in_array($type, self::STEPLESS, true)) {
                    $step = $payload['sequence'] ?? null;
                    if (!is_int($step) || $step < 1) {
                        return "reused {$type} must name a positive workflow step";
                    }
                    if ($step < $lastReusedStep) {
                        return "reused workflow steps must not descend: {$type} step {$step} follows step {$lastReusedStep}";
                    }
                    [$lifecycle, $identityField] = self::LIFECYCLES[$type] ?? [$type, null];
                    $owner = [$lifecycle, $identityField === null ? null : ($payload[$identityField] ?? null)];
                    if ($step === $lastReusedStep
                        && ($identityField === null || $owner[1] === null || $owner !== $stepOwner)) {
                        return "reused workflow step {$step} is repeated by {$type}, which is not part of the same durable step";
                    }
                    $stepOwner = $owner;
                    $lastReusedStep = $step;
                    $reusedSteps[$step] = true;
                }
                continue;
            }
            foreach (self::REUSE_KEYS as $key) {
                if (array_key_exists($key, $payload)) {
                    return "successor {$type} must not carry {$key} outside the reused prefix";
                }
            }
            if (array_key_exists('workflow_run_id', $payload) && $payload['workflow_run_id'] !== $run) {
                return "successor {$type} must bind to the successor run";
            }
            if (array_key_exists('workflow_instance_id', $payload) && $payload['workflow_instance_id'] !== $instance) {
                return "successor {$type} must bind to the successor instance";
            }
            if ($type === 'ActivityCancelled') {
                if (!is_int($payload['sequence'] ?? null) || $payload['sequence'] < 1) {
                    return 'ActivityCancelled must name a positive workflow step';
                }
                foreach (['activity_type', 'activity_execution_id'] as $field) {
                    if (!self::nonBlank($payload[$field] ?? null)) {
                        return "ActivityCancelled {$field} must be a non-blank string";
                    }
                }
            }
            $cancellation = is_array($payload['cancellation'] ?? null) ? $payload['cancellation'] : null;
            if ($cancellation !== null) {
                if (($cancellation['root_workflow_instance_id'] ?? null) !== $instance || ($cancellation['root_workflow_run_id'] ?? null) !== $run) {
                    return "successor {$type} cancellation must root at the successor";
                }
                foreach ($cancellation['lineage'] ?? [] as $link) {
                    if (($link['workflow_instance_id'] ?? null) !== $instance || ($link['workflow_run_id'] ?? null) !== $run) {
                        return "successor {$type} cancellation lineage must name the successor";
                    }
                }
            }
            if (is_int($payload['sequence'] ?? null) && $payload['sequence'] <= $lastReusedStep) {
                return "successor {$type} must not reuse a reused step sequence";
            }
            if ($type === 'CooperativeCancellationRequested') {
                $requests[] = $offset + 1;
            } elseif ($type === 'CooperativeCancellationDelivered') {
                $deliveries[] = $offset + 1;
            }
        }
        if (count($history) - 1 < $prefixLength) {
            return 'history ends before the expected reused prefix';
        }
        if ($lastReusedStep === 0) {
            return 'history has no reused workflow step';
        }
        if (array_keys($reusedSteps) !== range(1, $lastReusedStep)) {
            return 'reused workflow steps must be exactly 1..'.$lastReusedStep.' with no gap';
        }
        if (($start['resume_step_sequence'] ?? null) !== $lastReusedStep + 1) {
            return 'resume_step_sequence must be the last reused workflow step + 1 ('.($lastReusedStep + 1).')';
        }
        if ($requests !== [] || $deliveries !== []) {
            return self::cancellationViolation($history, $requests, $deliveries, $startedAt, $instance, $run);
        }

        return null;
    }

    /**
     * The successor's own cancellation, as Core 28893f97 writes it: one
     * WorkflowStub::recordCancellationRequest() request, then one
     * CooperativeCancellationDelivery::record() delivery
     * carrying the same context, delivered to a call boundary that
     * CooperativeCancellationDelivery::validateCallBoundary() accepts.
     *
     * @param list<array<string, mixed>> $history
     * @param list<int> $requests history indexes
     * @param list<int> $deliveries history indexes
     */
    private static function cancellationViolation(
        array $history,
        array $requests,
        array $deliveries,
        DateTimeImmutable $startedAt,
        string $instance,
        string $run,
    ): ?string {
        if (count($requests) !== 1 || count($deliveries) !== 1) {
            return 'a successor cancellation is exactly one request and one delivery';
        }
        [$requestIndex, $deliveryIndex] = [$requests[0], $deliveries[0]];
        if ($deliveryIndex < $requestIndex) {
            return 'the cancellation delivery must follow its request';
        }
        $request = $history[$requestIndex]['payload'];
        $delivery = $history[$deliveryIndex]['payload'];
        $command = $request['workflow_command_id'] ?? null;
        if (!self::nonBlank($command)) {
            return 'the cancellation request must carry a non-blank workflow_command_id';
        }
        // recordCancellationRequest() always writes both identities.
        if (($request['workflow_instance_id'] ?? null) !== $instance || ($request['workflow_run_id'] ?? null) !== $run) {
            return 'the cancellation request must carry the successor workflow_instance_id and workflow_run_id';
        }
        if (($request['command_type'] ?? null) !== 'request_cancellation') {
            return 'the cancellation request command_type must be request_cancellation';
        }
        if (($delivery['workflow_command_id'] ?? null) !== $command) {
            return 'the cancellation delivery must name its request workflow_command_id';
        }
        $context = $request['cancellation'] ?? null;
        if (!is_array($context) || $context === []) {
            return 'the cancellation request must carry its cancellation context';
        }
        if (($delivery['cancellation'] ?? null) !== $context) {
            return "the cancellation delivery must carry its request's cancellation context";
        }
        $contextViolation = self::contextViolation($context, $command);
        if ($contextViolation !== null) {
            return $contextViolation;
        }
        if ((($context['reason'] ?? null) !== null && !is_string($context['reason']))
            || ($request['reason'] ?? null) !== ($context['reason'] ?? null)) {
            return "the cancellation request reason must be its context's nullable text reason";
        }
        $requestedAt = self::time($context['requested_at']);
        $requestRecordedAt = self::time($history[$requestIndex]['recorded_at'] ?? null);
        if ($requestedAt === null || $requestRecordedAt === null || $requestedAt < $startedAt || $requestedAt > $requestRecordedAt) {
            return 'cancellation requested_at must fall between the successor start and the request record';
        }
        if (($request['cleanup_deadline_at'] ?? null) !== $context['cleanup_deadline_at']) {
            return "the cancellation request cleanup_deadline_at must be its context's";
        }

        return self::deliveryStepViolation($history, $requestIndex, $deliveryIndex, $delivery);
    }

    /**
     * Core CancellationContext::fromArray() identity rules for a v1 context.
     *
     * @param array<mixed> $context
     */
    private static function contextViolation(array $context, string $command): ?string
    {
        if (($context['schema'] ?? null) !== 'durable-workflow.cancellation-context/v1') {
            return 'cancellation schema must be durable-workflow.cancellation-context/v1';
        }
        if (array_key_exists('scope_origin', $context) || array_key_exists('scope_authority_deadline_at', $context)) {
            return 'a successor own cancellation must use a root context without scope fields';
        }
        foreach (['request_id', 'root_request_id', 'root_workflow_instance_id', 'root_workflow_run_id', 'source'] as $field) {
            if (!self::nonBlank($context[$field] ?? null)) {
                return "cancellation {$field} must be a non-blank string";
            }
        }
        if ($context['request_id'] !== $command) {
            return 'cancellation request_id must be the request workflow_command_id';
        }
        $requester = $context['requester'] ?? null;
        if (!is_array($requester) || $requester === [] || array_is_list($requester)) {
            return 'cancellation requester must identify its caller';
        }
        foreach ($requester as $key => $value) {
            if (!in_array($key, ['type', 'id', 'label'], true) || !is_string($value) || $value === '') {
                return 'cancellation requester may carry only non-empty type, id and label';
            }
        }
        $lineage = $context['lineage'] ?? null;
        if (!is_array($lineage) || $lineage === [] || !array_is_list($lineage)) {
            return 'cancellation lineage must contain the root request';
        }
        foreach ($lineage as $entry) {
            if (!is_array($entry) || !self::nonBlank($entry['request_id'] ?? null)
                || !self::nonBlank($entry['workflow_instance_id'] ?? null) || !self::nonBlank($entry['workflow_run_id'] ?? null)) {
                return 'cancellation lineage entries must name a request, instance and run';
            }
        }
        $last = count($lineage) - 1;
        if (count(array_unique(array_column($lineage, 'request_id'))) !== count($lineage)
            || count(array_unique(array_column($lineage, 'workflow_run_id'))) !== count($lineage)) {
            return 'cancellation lineage must not repeat a request or run';
        }
        if ([$lineage[0]['request_id'], $lineage[0]['workflow_instance_id'], $lineage[0]['workflow_run_id']]
            !== [$context['root_request_id'], $context['root_workflow_instance_id'], $context['root_workflow_run_id']]) {
            return 'cancellation lineage must start at the root request';
        }
        if ($lineage[$last]['request_id'] !== $context['request_id']) {
            return 'cancellation lineage must end at this request';
        }
        if (!array_key_exists('parent_request_id', $context)) {
            return 'cancellation parent_request_id must be present and null for a root request';
        }
        $parent = $context['parent_request_id'];
        if ($parent !== ($last === 0 ? null : $lineage[$last - 1]['request_id'])) {
            return 'cancellation parent_request_id must be the previous lineage request, or null for a root request';
        }
        if ($last !== 0 || count($lineage[0]) !== 3 || $context['root_request_id'] !== $command || $parent !== null) {
            return 'a successor cancellation lineage must contain exactly its own root address';
        }
        $requestedAt = self::time($context['requested_at'] ?? null);
        $deadline = self::time($context['cleanup_deadline_at'] ?? null);
        if ($requestedAt === null || $deadline === null || $deadline <= $requestedAt) {
            return 'cancellation cleanup_deadline_at must be a valid time after requested_at';
        }

        return null;
    }

    /**
     * Core CooperativeCancellationDelivery::validateCallBoundary() for a
     * single call: the step is a recorded call of the delivered kind that
     * was not resolved before the request, or the next reached unrecorded
     * step, which nothing may record after the delivery.
     *
     * @param list<array<string, mixed>> $history
     * @param array<mixed> $delivery
     */
    private static function deliveryStepViolation(array $history, int $requestIndex, int $deliveryIndex, array $delivery): ?string
    {
        $step = $delivery['sequence'] ?? null;
        $callKind = $delivery['call_kind'] ?? null;
        if (!is_int($step) || $step < 1) {
            return 'the cancellation delivery must name a positive workflow step';
        }
        if (!is_string($callKind) || !array_key_exists($callKind, self::CALL_RESOLUTIONS)) {
            return 'the cancellation delivery call_kind must be a single-call kind';
        }
        foreach (['sequence_span', 'operation_sequence', 'operation_sequence_span'] as $field) {
            if (array_key_exists($field, $delivery)) {
                return 'a single-call cancellation delivery must not carry span or operation operands';
            }
        }
        $recordedKind = null;
        $nextStep = 1;
        foreach (array_slice($history, 0, $deliveryIndex) as $index => $event) {
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $sequence = $payload['sequence'] ?? null;
            if (!is_int($sequence) || !self::isStepEvent((string) ($event['event_type'] ?? ''), $payload)) {
                continue;
            }
            $nextStep = max($nextStep, $sequence + 1);
            if ($sequence !== $step) {
                continue;
            }
            $recordedKind ??= self::callKind((string) ($event['event_type'] ?? ''), $payload);
            if ($index < $requestIndex && self::isResolution($event['event_type'] ?? null)) {
                return "the cancellation delivery targets step {$step}, which resolved before the request";
            }
        }
        if ($step > $nextStep) {
            return "the cancellation delivery targets step {$step}, after the next step {$nextStep}";
        }
        if ($step < $nextStep && $recordedKind !== $callKind) {
            return "the cancellation delivery call_kind must match the call recorded at step {$step}";
        }
        if ($step === $nextStep) {
            foreach (array_slice($history, $deliveryIndex + 1) as $event) {
                $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
                if (($payload['sequence'] ?? null) === $step && self::isStepEvent((string) ($event['event_type'] ?? ''), $payload)) {
                    return "step {$step} must not be recorded after a cancellation delivered before it was scheduled";
                }
            }

            return self::priorCallViolation(array_slice($history, 0, $deliveryIndex));
        }

        return null;
    }

    /** @param array<mixed> $payload */
    private static function isStepEvent(string $type, array $payload): bool
    {
        return self::callKind($type, $payload) !== null
            || self::isResolution($type)
            || in_array($type, ['MemoUpserted', 'SearchAttributesUpserted', 'SideEffectRecorded', 'VersionMarkerRecorded', 'CooperativeCancellationDelivered'], true);
    }

    /**
     * This single-call evidence model cannot infer parallel progress. A later
     * unrecorded call is reachable only after each modeled earlier call has a
     * matching later resolution. History row order also applies to equal times.
     *
     * @param list<array<string, mixed>> $beforeDelivery
     */
    private static function priorCallViolation(array $beforeDelivery): ?string
    {
        foreach ($beforeDelivery as $index => $event) {
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $kind = self::callKind((string) ($event['event_type'] ?? ''), $payload);
            $shape = self::CALL_RESOLUTIONS[$kind ?? ''] ?? null;
            if ($kind === null || $shape === null) {
                continue;
            }
            if (!self::callIsResolved($kind, $payload, $shape, array_slice($beforeDelivery, $index + 1))) {
                return 'the cancellation delivery targets an unreachable next step: an earlier call is unresolved';
            }
        }

        return null;
    }

    /**
     * @param array<mixed> $payload
     * @param array{string, list<string>} $shape
     * @param list<array<string, mixed>> $afterOpening
     */
    private static function callIsResolved(string $kind, array $payload, array $shape, array $afterOpening): bool
    {
        [$identity, $terminals] = $shape;
        if (!self::nonBlank($payload[$identity] ?? null)
            || !is_int($payload['sequence'] ?? null) || $payload['sequence'] < 1) {
            return false;
        }
        foreach ($afterOpening as $terminal) {
            $terminalPayload = is_array($terminal['payload'] ?? null) ? $terminal['payload'] : [];
            $type = $terminal['event_type'] ?? null;
            if (in_array($type, $terminals, true)
                && self::terminalFamilyMatches($kind, $type, $terminalPayload)
                && ($terminalPayload['sequence'] ?? null) === $payload['sequence']
                && ($terminalPayload[$identity] ?? null) === $payload[$identity]) {
                return true;
            }
        }

        return false;
    }

    /** @param array<mixed> $payload */
    private static function terminalFamilyMatches(string $kind, mixed $type, array $payload): bool
    {
        if (in_array($type, self::TIMER_TERMINALS, true)) {
            $timerKind = $payload['timer_kind'] ?? null;

            return match ($kind) {
                'condition' => $timerKind === 'condition_timeout',
                'signal' => $timerKind === 'signal_timeout',
                'timer' => !in_array($timerKind, ['condition_timeout', 'signal_timeout'], true),
                default => false,
            };
        }

        return !in_array($kind, ['activity', 'local_activity'], true)
            || (($payload['local_activity'] ?? false) === true) === ($kind === 'local_activity');
    }

    private static function isResolution(mixed $type): bool
    {
        foreach (self::CALL_RESOLUTIONS as [, $terminals]) {
            if (in_array($type, $terminals, true)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Core CooperativeCancellationDelivery::callKindAt() for one event.
     *
     * @param array<mixed> $payload
     */
    private static function callKind(string $type, array $payload): ?string
    {
        return match ($type) {
            'ActivityScheduled' => ($payload['local_activity'] ?? false) === true ? 'local_activity' : 'activity',
            'ConditionWaitOpened' => 'condition',
            'SignalWaitOpened' => 'signal',
            'ChildWorkflowScheduled' => 'child',
            'TimerScheduled' => match ($payload['timer_kind'] ?? null) {
                'condition_timeout' => 'condition',
                'signal_timeout' => 'signal',
                default => 'timer',
            },
            default => null,
        };
    }

    private static function nonBlank(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '';
    }

    private static function time(mixed $value): ?DateTimeImmutable
    {
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            return null;
        }

        return new DateTimeImmutable($value);
    }
}
