<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

/** Reconstruct the finite child-call/run ledger from complete parent history. */
final class DeferredChildAllLedger
{
    /** @param list<array<string, mixed>> $initial */
    public function __construct(private readonly DeferredChildAllScenario $scenario, private readonly array $initial)
    {
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<list<array{run_id: string, workflow_link_id: string, terminal: ?string}>>
     */
    public function validate(array $history, bool $parentFinished = false): array
    {
        $initial = $this->initial;
        $runs = array_fill(0, $this->scenario->size, []);
        $scheduled = array_fill(0, $this->scenario->size, false);
        $usedRuns = array_column($initial, 'child_workflow_run_id');
        $usedLinks = array_column($initial, 'workflow_link_id');
        $parentClosed = false;
        foreach ($history as $event) {
            $type = $event['event_type'] ?? null;
            DeferredChildAllScenario::check(is_string($type), 'History event type is not a string.');
            if ($type === 'WorkflowCompleted') {
                DeferredChildAllScenario::check(!$parentClosed, 'Duplicate parent completion event.');
                $parentClosed = true;
            }
            if (!str_starts_with($type, 'Child')) {
                continue;
            }
            DeferredChildAllScenario::check(!$parentClosed, 'Child event follows parent completion.');
            [$index, $payload] = $this->childEvent($type, $event, $initial);
            if ($type === 'ChildWorkflowScheduled') {
                DeferredChildAllScenario::check(!$scheduled[$index] && $runs[$index] === [], 'Child schedule is repeated or follows its start.');
                $scheduled[$index] = true;
                continue;
            }
            DeferredChildAllScenario::check($scheduled[$index], 'Child run event precedes its schedule.');
            if ($type === 'ChildRunStarted') {
                $next = $this->startedRun($payload, $initial[$index], $runs[$index], $usedRuns, $usedLinks);
                if ($runs[$index] !== []) {
                    $usedRuns[] = $next['run_id'];
                    $usedLinks[] = $next['workflow_link_id'];
                }
                $runs[$index][] = $next;
                continue;
            }
            $terminal = $this->terminalRun($type, $payload, $runs[$index]);
            $runs[$index][array_key_last($runs[$index])] = $terminal;
        }
        $this->finishedChains($runs, $parentFinished, $parentClosed);

        return $runs;
    }

    /**
     * @param array<string, mixed> $event
     * @param list<array<string, mixed>> $initial
     * @return array{int, array<string, mixed>}
     */
    private function childEvent(string $type, array $event, array $initial): array
    {
        DeferredChildAllScenario::check(in_array($type, ['ChildWorkflowScheduled', 'ChildRunStarted', 'ChildRunCompleted',
            'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true), 'Unqualified child event type.');
        $payload = $event['payload'] ?? null;
        DeferredChildAllScenario::check(is_array($payload), 'Child event has no payload.');
        $sequence = $payload['sequence'] ?? null;
        DeferredChildAllScenario::check(is_int($sequence) && $sequence >= 1 && $sequence <= $this->scenario->size, 'Child sequence is not an exact member integer.');
        $index = $sequence - 1;
        DeferredChildAllScenario::check(($payload['child_workflow_type'] ?? null) === $this->scenario->childType(), 'Child workflow type changed.');
        foreach (['child_call_id', 'child_workflow_instance_id', 'parallel_group_id', 'parallel_group_path'] as $key) {
            DeferredChildAllScenario::check(isset($payload[$key]) && $payload[$key] === $initial[$index][$key], 'Child event changed its original call/link/path.');
        }
        // Schedules were already checked by identities(); the run transitions
        // also need their exact nonempty identities before any state update.
        self::runIdentity($payload);

        return [$index, $payload];
    }

    /**
     * @param array<string, mixed> $payload
     * @return array{string, string}
     */
    private static function runIdentity(array $payload): array
    {
        DeferredChildAllScenario::check(is_string($payload['child_workflow_run_id'] ?? null) && $payload['child_workflow_run_id'] !== '', 'Child event has no run identity.');
        DeferredChildAllScenario::check(is_string($payload['workflow_link_id'] ?? null) && $payload['workflow_link_id'] !== '', 'Child event has no link identity.');

        return [$payload['child_workflow_run_id'], $payload['workflow_link_id']];
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, mixed> $original
     * @param list<array{run_id: string, workflow_link_id: string, terminal: ?string}> $chain
     * @param list<string> $usedRuns
     * @param list<string> $usedLinks
     * @return array{run_id: string, workflow_link_id: string, terminal: ?string}
     */
    private function startedRun(array $payload, array $original, array $chain, array $usedRuns, array $usedLinks): array
    {
        [$run, $link] = self::runIdentity($payload);
        if ($chain === []) {
            DeferredChildAllScenario::check($run === $original['child_workflow_run_id'] && $link === $original['workflow_link_id']
                && ($payload['child_run_number'] ?? null) === 1, 'Initial child run/link/number changed.');
            DeferredChildAllScenario::check(($payload['retry_of_child_workflow_run_id'] ?? null) === null
                && ($payload['retry_attempt'] ?? null) === null, 'Initial child run claims a retry predecessor.');

            return ['run_id' => $run, 'workflow_link_id' => $link, 'terminal' => null];
        }
        $current = $chain[array_key_last($chain)];
        DeferredChildAllScenario::check($this->replacementMember($payload['sequence']) && count($chain) === 1
            && $current['terminal'] === null, 'Child run cannot replace a closed or unqualified member.');
        DeferredChildAllScenario::check(!in_array($run, $usedRuns, true) && !in_array($link, $usedLinks, true)
            && ($payload['child_run_number'] ?? null) === 2, 'Replacement child run/link/number is not distinct.');
        // Retry carries explicit parent provenance. Continuation's predecessor
        // is in child history, not an invented intermediate parent terminal.
        $this->replacementProvenance($payload, $current['run_id']);

        return ['run_id' => $run, 'workflow_link_id' => $link, 'terminal' => null];
    }

    private function replacementMember(int $sequence): bool
    {
        return $sequence === 1 && in_array($this->scenario->case, ['retry', 'continue'], true);
    }

    /** @param array<string, mixed> $payload */
    private function replacementProvenance(array $payload, string $previousRun): void
    {
        if ($this->scenario->case === 'retry') {
            DeferredChildAllScenario::check(($payload['retry_of_child_workflow_run_id'] ?? null) === $previousRun && ($payload['retry_attempt'] ?? null) === 2,
                'Replacement child transition does not match its case.');
            return;
        }
        DeferredChildAllScenario::check(($payload['retry_of_child_workflow_run_id'] ?? null) === null && ($payload['retry_attempt'] ?? null) === null,
            'Replacement child transition does not match its case.');
    }

    /**
     * @param array<string, mixed> $payload
     * @param list<array{run_id: string, workflow_link_id: string, terminal: ?string}> $chain
     * @return array{run_id: string, workflow_link_id: string, terminal: ?string}
     */
    private function terminalRun(string $type, array $payload, array $chain): array
    {
        [$run, $link] = self::runIdentity($payload);
        $current = $chain === [] ? null : $chain[array_key_last($chain)];
        DeferredChildAllScenario::check($current !== null && $current['terminal'] === null && $run === $current['run_id']
            && $link === $current['workflow_link_id'], 'Child terminal is repeated, late or belongs to another run/link.');
        DeferredChildAllScenario::check(($payload['child_run_number'] ?? null) === count($chain), 'Child terminal run number changed.');
        DeferredChildAllScenario::check(!$this->replacementMember($payload['sequence']) || count($chain) === 2, 'Replacement member resolved before its second run.');
        $expected = $payload['sequence'] === 1 && $this->scenario->failsEarly() ? $this->scenario->expected()['failure_type'] : 'ChildRunCompleted';
        DeferredChildAllScenario::check($type === $expected, 'Child terminal does not match its selected case.');
        $current['terminal'] = $type;

        return $current;
    }

    /** @param list<list<array{run_id: string, workflow_link_id: string, terminal: ?string}>> $runs */
    private function finishedChains(array $runs, bool $parentFinished, bool $parentClosed): void
    {
        foreach ($runs as $index => $chain) {
            DeferredChildAllScenario::check($chain !== [], 'Atomic child start has no initial run event.');
            if ($parentFinished && (!$this->scenario->failsEarly() || $index === 0)) {
                DeferredChildAllScenario::check($chain[array_key_last($chain)]['terminal'] !== null, 'Finished parent has an unresolved required member.');
            }
        }
        DeferredChildAllScenario::check(!$parentFinished || $parentClosed, 'Finished parent has no canonical completion.');
    }

}
