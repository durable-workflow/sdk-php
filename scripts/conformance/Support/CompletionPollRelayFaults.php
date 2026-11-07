<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

require_once __DIR__.'/CompletionPollFaults.php';
require_once __DIR__.'/CompletionPollTrace.php';

/** Controlled transport faults around real replies. No producer state or clock is implemented. */
final class CompletionPollRelayFaults
{
    /** @param array<string, mixed> $body */
    public static function before(CompletionPollTrace $trace, string $path, array $body, string $request): void
    {
        $state = $trace->state();
        if ($state['scenario'] !== 'completion-transit' || isset($state['fault_request'])
            || !str_contains($path, '/workflow-tasks/') || !str_ends_with($path, '/complete')
            || !isset($body['workflow_poll'])
            || !in_array('complete_workflow', array_column($body['commands'] ?? [], 'type'), true)) {
            return;
        }
        $grant = self::parentGrant($state, $path);
        if ($grant === null) throw new RuntimeException('completion_fault_original_grant_required');
        $delay = CompletionPollFaults::delay($grant, 'transit', self::budget($state));
        if (self::select($trace, $request)) self::hold($trace, $request, 'completion_transit', $delay);
    }

    /** @param array<string, mixed> $answer
     * @return bool True only when this invocation deliberately withholds a successful answer.
     */
    public static function after(CompletionPollTrace $trace, string $path, array $answer, int $status, string $request): bool
    {
        if ($status !== 200) return false;
        $state = $trace->state();
        if (isset($state['fault_request'])) return false;
        $scenario = $state['scenario'];
        $member = str_ends_with($path, '/complete') ? match ($scenario) {
            'carrier-loss-activity' => str_contains($path, '/workflow-tasks/') ? 'activity_poll' : null,
            'carrier-loss-workflow' => str_contains($path, '/activity-tasks/') ? 'workflow_poll' : null,
            'carrier-loss-workflow-completion' => str_contains($path, '/workflow-tasks/') ? 'workflow_poll' : null,
            'carrier-expiry' => 'workflow_poll',
            'carrier-refence' => str_contains($path, '/workflow-tasks/') ? 'activity_poll' : null,
            default => null,
        } : null;
        $grant = $member !== null && ($answer[$member]['leased'] ?? null) === true
            && is_array($answer[$member]['answer']['task'] ?? null) ? $answer[$member]['answer'] : null;
        if ($grant !== null && in_array($scenario, ['carrier-loss-activity', 'carrier-loss-workflow', 'carrier-loss-workflow-completion'], true)) {
            if (!self::select($trace, $request)) return false;
            $trace->record(['kind' => 'fault', 'request' => $request, 'mode' => 'lost_carried_answer', 'member' => $member]);
            return true;
        }
        if ($scenario === 'prefetch-loss' && str_ends_with($path, '/workflow-tasks/poll')
            && ($answer['task']['workflow_id'] ?? null) === $state['prefix'].'-3' && self::select($trace, $request)) {
            $trace->record(['kind' => 'fault', 'request' => $request, 'mode' => 'lost_prefetch_answer']);
            return true;
        }
        if ($grant !== null && $scenario === 'carrier-refence') {
            if (!self::select($trace, $request)) return false;
            $trace->change(static function (array &$data): void { $data['refence_ready'] = true; });
            while (!(($trace->state())['refence_complete'] ?? false)) {
                self::budget($trace->state());
                usleep(5000);
            }
            // The accepted vector fences original activity receipt recovery.
            // It does not assert revocation of an already granted workflow.
            $trace->record(['kind' => 'fault', 'request' => $request, 'mode' => 'refenced_activity_answer', 'member' => 'activity_poll']);
            return true;
        }
        if ($grant !== null && $scenario === 'carrier-expiry') {
            $delay = CompletionPollFaults::delay($grant, 'expiry', self::budget($state));
            if (!self::select($trace, $request)) return false;
            self::hold($trace, $request, 'expiry', $delay);
            return false;
        }
        if ($scenario === 'pipeline-buffer-age' && str_ends_with($path, '/worker/heartbeat')
            && self::prefetchDelivered($state) && self::select($trace, $request)) {
            if (self::budget($state) < CompletionPollFaults::BUFFER_AGE_SECONDS) throw new RuntimeException('completion_fault_budget');
            self::hold($trace, $request, 'buffer_age', CompletionPollFaults::BUFFER_AGE_SECONDS);
        }
        return false;
    }

    /** @param array<string, mixed> $state */
    private static function budget(array $state): float
    {
        $remaining = ($state['deadline_at'] ?? 0) - hrtime(true) / 1e9 - 5;
        if ($remaining <= 0) throw new RuntimeException('completion_fault_budget');
        return min(90, $remaining);
    }

    private static function select(CompletionPollTrace $trace, string $request): bool
    {
        $selected = false;
        $trace->change(static function (array &$state) use ($request, &$selected): void {
            if (!isset($state['fault_request'])) {
                $state['fault_request'] = $request;
                $selected = true;
            }
        });
        return $selected;
    }

    private static function hold(CompletionPollTrace $trace, string $request, string $mode, float $seconds): void
    {
        $trace->record(['kind' => 'fault_begin', 'request' => $request, 'mode' => $mode, 'seconds' => $seconds]);
        $until = hrtime(true) / 1e9 + $seconds;
        while (hrtime(true) / 1e9 < $until) usleep(5000);
        $trace->record(['kind' => 'fault_end', 'request' => $request, 'mode' => $mode]);
    }

    /** @param array<string, mixed> $state */
    private static function prefetchDelivered(array $state): bool
    {
        $request = null;
        $delivered = false;
        foreach ($state['events'] as $event) {
            if ($event['kind'] === 'callback' && ($event['index'] ?? null) === 3) return false;
            if ($event['kind'] === 'upstream' && ($event['answer']['task']['workflow_id'] ?? null) === $state['prefix'].'-3') {
                $request = $event['request'];
            }
            if ($event['kind'] === 'delivered' && $event['request'] === $request && $event['status'] === 200) $delivered = true;
        }
        return $delivered;
    }

    /** @param array<string, mixed> $state
     * @return array<string, mixed>|null
     */
    private static function parentGrant(array $state, string $path): ?array
    {
        $found = null;
        foreach ($state['events'] as $event) {
            if ($event['kind'] !== 'upstream' || ($event['status'] ?? null) !== 200) continue;
            foreach ([$event['answer'], $event['answer']['workflow_poll'] ?? []] as $grant) {
                if (is_string($grant['task']['task_id'] ?? null)
                    && str_ends_with($path, '/workflow-tasks/'.rawurlencode($grant['task']['task_id']).'/complete')) {
                    $found = $grant;
                }
            }
        }
        return $found;
    }
}
