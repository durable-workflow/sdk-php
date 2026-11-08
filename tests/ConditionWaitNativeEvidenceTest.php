<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\ConditionWaitNativeEvidence as Evidence;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/Support/ConditionWaitNativeEvidence.php';

/**
 * Native Server 3.2.0 history shapes for #269: each open is a new
 * condition_wait_id; a condition timeout is a TimerScheduled with
 * timer_kind condition_timeout carrying the wait id; a timeout ends with
 * TimerFired (no ConditionWaitTimedOut).
 */
final class ConditionWaitNativeEvidenceTest extends TestCase
{
    private const T0 = 1_794_000_000.0;

    public function testAFalseWakeThatKeepsTheFirstDeadlinePasses(): void
    {
        $summary = Evidence::falseWake(Evidence::events(self::falseWake(15)), 20);
        self::assertSame(['first_timeout' => 20, 'reopen_timeout' => 15, 'remaining_at_wake' => 15.0,
            'fired_after_first_open' => 20.0], $summary);
    }

    /** The original #269 defect: the reopen restarts the full timeout and the deadline moves. */
    public function testAReopenWithTheFullTimeoutIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_reopen_budget');
        Evidence::falseWake(Evidence::events(self::falseWake(20, firedAt: 25.0)), 20);
    }

    public function testATimeoutThatFiresAfterTheFirstDeadlineIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_deadline_moved');
        Evidence::falseWake(Evidence::events(self::falseWake(15, firedAt: 23.5)), 20);
    }

    /** Diagnostic only: the timeline is bounded, payload-free and leaves every predicate unchanged. */
    public function testTheCheckpointTimelineIsBoundedAndPayloadFree(): void
    {
        $history = self::falseWake(15);
        $history[2]['payload']['arguments'] = 'secret-payload';
        $events = Evidence::events($history);
        $timeline = Evidence::timeline($events);
        self::assertSame(['i' => 0, 'type' => 'ConditionWaitOpened', 'offset' => 0.0, 'timeout_seconds' => 20, 'sequence' => 1], $timeline[0]);
        self::assertSame(['i' => 1, 'type' => 'TimerScheduled', 'offset' => 0.0, 'delay_seconds' => 20, 'sequence' => 1], $timeline[1]);
        self::assertSame(['i' => 2, 'type' => 'SignalReceived', 'offset' => 5.0], $timeline[2]);
        self::assertSame(['i' => 7, 'type' => 'TimerFired', 'offset' => 20.0, 'sequence' => 2], $timeline[7]);
        self::assertStringNotContainsString('secret', (string) json_encode($timeline));
        self::assertCount(40, Evidence::timeline(Evidence::events(array_merge(...array_fill(0, 30, self::falseWake(15))))));
        self::assertSame(Evidence::falseWake($events, 20), Evidence::falseWake(Evidence::events(self::falseWake(15)), 20));
    }

    /** Each history assertion runs through cwnEvidence, which checkpoints identity and timeline before asserting. */
    public function testTheQualifierCheckpointsBeforeEveryHistoryAssertion(): void
    {
        $source = (string) file_get_contents(dirname(__DIR__).'/scripts/qualify-condition-wait-native.php');
        preg_match('~function cwnEvidence\(.*?\n}\n~s', $source, $match);
        self::assertNotEmpty($match);
        self::assertLessThan(strpos($match[0], '$check($events)'), strpos($match[0], "fwrite(STDERR, substr('checkpoint '"));
        self::assertStringContainsString(', 0, 4096)', $match[0]);
        foreach (['falseWake', 'settledRun', 'closedWithOpenWait', 'ownBudget', 'waits'] as $check) {
            preg_match_all('~Evidence::'.$check.'\(~', $source, $all);
            preg_match_all('~fn \(array \$events\): array => Evidence::'.$check.'\(\$events~', $source, $wrapped);
            self::assertSame(count($all[0]), count($wrapped[0]), $check.' runs only inside cwnEvidence');
        }
        self::assertSame(7, substr_count($source, 'cwnEvidence(\''), 'C1, C2, C3 loop, C3 successor, C3 continue, C4, C5 loop');
        self::assertStringContainsString('Evidence::falseWake($events, 20)', $source);
    }

    public function testAFalseWakeWithoutASignalIsRefused(): void
    {
        $history = array_values(array_filter(self::falseWake(15), static fn (array $e): bool => $e['event_type'] !== 'SignalReceived'));
        $this->expectExceptionMessage('condition_evidence_false_wake_signal');
        Evidence::falseWake(Evidence::events($history), 20);
    }

    public function testASatisfiedWakeMustCancelItsTimer(): void
    {
        $history = array_values(array_filter(self::falseWake(15), static fn (array $e): bool => $e['event_type'] !== 'TimerCancelled'));
        $this->expectExceptionMessage('condition_evidence_false_wake_resolution');
        Evidence::falseWake(Evidence::events($history), 20);
    }

    public function testASettledRunPasses(): void
    {
        self::assertSame(['waits' => 1, 'satisfied' => 1], Evidence::settledRun(Evidence::events(self::duplicate(false))));
    }

    /** The second #269 defect: a replayed task opens a second physical wait while the first is unresolved. */
    public function testAnOverlappingDuplicateOpenIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_overlapping_open');
        Evidence::settledRun(Evidence::events(self::duplicate(true)));
    }

    public function testAnUnresolvedWaitInACompletedRunIsRefused(): void
    {
        $history = [self::opened(1.0, 'w1', 'go', 60), self::scheduled(1.0, 't1', 'w1', 60), self::event(3.0, 'WorkflowCompleted')];
        $this->expectExceptionMessage('condition_evidence_unresolved_wait');
        Evidence::settledRun(Evidence::events($history));
    }

    public function testTerminateAndCancelCancelTheOpenConditionTimer(): void
    {
        foreach (['terminate' => 'WorkflowTerminated', 'cancel' => 'WorkflowCancelled'] as $close => $type) {
            $history = [self::opened(1.0, 'w1', 'k', 300), self::scheduled(1.0, 't1', 'w1', 300),
                self::event(4.0, $type), self::event(4.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1])];
            self::assertSame(['waits' => 1, 'close' => $type], Evidence::closedWithOpenWait(Evidence::events($history), $close));
        }
    }

    public function testATerminateWithoutTimerCancellationIsRefused(): void
    {
        $history = [self::opened(1.0, 'w1', 'k', 300), self::scheduled(1.0, 't1', 'w1', 300), self::event(4.0, 'WorkflowTerminated')];
        $this->expectExceptionMessage('condition_evidence_close_timer_not_cancelled');
        Evidence::closedWithOpenWait(Evidence::events($history), 'terminate');
    }

    public function testContinueAsNewAfterAFiredTimeoutPasses(): void
    {
        $history = [self::opened(1.0, 'w1', 'k', 2), self::scheduled(1.0, 't1', 'w1', 2),
            self::event(3.0, 'TimerFired', ['timer_id' => 't1', 'sequence' => 1]), self::event(3.2, 'WorkflowContinuedAsNew')];
        self::assertSame(['waits' => 1, 'close' => 'WorkflowContinuedAsNew'], Evidence::closedWithOpenWait(Evidence::events($history), 'continue'));
    }

    public function testANewlyAuthoredWaitKeepsItsOwnBudget(): void
    {
        self::assertSame(['second_timeout' => 20, 'reopen_timeout' => 15, 'remaining_at_wake' => 15.0],
            Evidence::ownBudget(Evidence::events(self::ownBudget(15)), 'first', 'second', 20));
    }

    /** #308: a new authored wait must not inherit the earlier wait's budget. */
    public function testANewWaitThatInheritsTheEarlierBudgetIsRefused(): void
    {
        $this->expectExceptionMessage('condition_evidence_own_budget');
        Evidence::ownBudget(Evidence::events(self::ownBudget(27)), 'first', 'second', 20);
    }

    public function testEveryConsumedAndSentSignalIsDeclaredOnItsWorkflowType(): void
    {
        self::assertSame(['cwn.false_wake' => ['go'], 'cwn.duplicate' => ['noise', 'go'], 'cwn.close' => ['never'],
            'cwn.own_budget' => ['first', 'second'], 'cwn.turns' => ['decision']], Evidence::DECLARED_SIGNALS);
        $source = (string) file_get_contents(__DIR__.'/../scripts/qualify-condition-wait-native.php');
        // Each workflow's consumed signals (context->signals('x')) are declared for that type.
        $blocks = preg_split("/(?=\\\$worker->registerWorkflow\\('cwn\\.)/", $source) ?: [];
        $types = 0;
        foreach ($blocks as $block) {
            if (preg_match("/^\\\$worker->registerWorkflow\\('(cwn\\.[a-z_]+)'/", $block, $type) !== 1) {
                continue;
            }
            ++$types;
            preg_match_all("/->signals\\('([a-z_]+)'\\)/", $block, $consumed);
            self::assertSame([], array_diff($consumed[1], Evidence::DECLARED_SIGNALS[$type[1]]), $type[1]);
        }
        self::assertSame(5, $types);
        // Every signal the qualifier sends is declared; noise is declared on the workflow it targets.
        preg_match_all("/signalWorkflow\\(\\\$handle->workflowId, '([a-z_]+)'/", $source, $sent);
        self::assertSame([], array_diff(array_unique($sent[1]), array_merge(...array_values(Evidence::DECLARED_SIGNALS))));
        self::assertContains('noise', Evidence::DECLARED_SIGNALS['cwn.duplicate']);
    }

    /**
     * The installed Worker's command contract carries the declared names; the
     * predecessor (no declarations) sent "signals: []", which Native refused.
     */
    public function testTheWorkerCommandContractCarriesTheDeclaredSignals(): void
    {
        foreach ([false, true] as $declare) {
            $worker = new \DurableWorkflow\Worker(new \DurableWorkflow\Client('http://127.0.0.1:9', namespace: 'default', workerToken: 'unused'), 'q', workerId: 'w');
            foreach (array_keys(Evidence::DECLARED_SIGNALS) as $workflowType) {
                $worker->registerWorkflow($workflowType, static fn (\DurableWorkflow\Worker\WorkflowContext $context): mixed => null);
            }
            if ($declare) {
                Evidence::declareSignals($worker);
            }
            $contracts = (new \ReflectionMethod($worker, 'workflowCommandContracts'))->invoke($worker);
            foreach (Evidence::DECLARED_SIGNALS as $workflowType => $signals) {
                $expected = $declare ? $signals : [];
                self::assertEqualsCanonicalizing($expected, $contracts[$workflowType]['signals'], ($declare ? 'candidate ' : 'predecessor ').$workflowType);
            }
        }
    }

    /** The exact signal contracts the installed Worker emits for the five qualifier workflow types. */
    public function testTheEmittedSignalContractsDeclareOneTypedParameterPerSentSignal(): void
    {
        $contracts = self::contracts(true);
        $one = static fn (string $name, string $type): array => [['name' => $name, 'position' => 0, 'required' => true,
            'variadic' => false, 'default_available' => false, 'default' => null, 'type' => $type, 'allows_null' => false]];
        $expected = [
            'cwn.false_wake' => ['go' => $one('value', 'string')],
            'cwn.duplicate' => ['noise' => $one('value', 'string'), 'go' => $one('value', 'string')],
            'cwn.close' => ['never' => []],
            'cwn.own_budget' => ['first' => $one('value', 'string'), 'second' => $one('value', 'string')],
            'cwn.turns' => ['decision' => $one('turn', 'int')],
        ];
        foreach ($expected as $workflowType => $signals) {
            $emitted = array_column($contracts[$workflowType]['signal_contracts'], 'parameters', 'name');
            self::assertSame($signals, $emitted, $workflowType);
        }
    }

    /**
     * Every signal the qualifier sends, checked with the frozen Native 3.2.0
     * admission rule against the Worker's emitted contract: the candidate admits
     * all of them; the #359 contract (no signatures) refuses the arguments, and
     * the original (no declarations) refuses the names.
     */
    public function testNativeAdmitsEverySentSignalOnlyUnderTheCandidateContract(): void
    {
        $sent = self::sentSignals();
        self::assertSame([['cwn.false_wake', 'go', ['wrong']], ['cwn.duplicate', 'noise', ['during-activity']], ['cwn.duplicate', 'go', ['right']],
            ['cwn.own_budget', 'first', ['done']], ['cwn.own_budget', 'second', ['wrong']], ['cwn.own_budget', 'second', ['right']],
            ['cwn.turns', 'decision', [999]], ['cwn.turns', 'decision', [1]]], $sent);
        foreach (['candidate' => [true, true], 'names_only_359' => [true, false], 'undeclared_original' => [false, false]] as $variant => [$declare, $signatures]) {
            $contracts = self::contracts($declare, $signatures);
            foreach ($sent as [$workflowType, $name, $arguments]) {
                $reason = self::nativeAdmission($contracts[$workflowType], $name, $arguments);
                $expected = ['candidate' => null, 'names_only_359' => 'invalid_signal_arguments', 'undeclared_original' => 'unknown_signal'][$variant];
                self::assertSame($expected, $reason, "{$variant} {$workflowType}.{$name}");
            }
        }
    }

    /**
     * The qualifier's initial-run assertion accepts exactly the real Worker's
     * emitted contract as frozen Native 3.2.0 retains and observes it (verbatim
     * parameters, contracts sorted by name, JSON object keys in either order).
     */
    public function testTheInitialRunContractCheckAcceptsTheWorkersContractAsNativeObservesIt(): void
    {
        $contracts = self::contracts(true);
        foreach (array_keys(Evidence::DECLARED_SIGNALS) as $workflowType) {
            self::assertSame(Evidence::expectedSignalContracts($workflowType), self::observed($contracts[$workflowType])['signal_contracts'], $workflowType);
            foreach ([false, true] as $sortKeys) {
                $observed = self::observed($contracts[$workflowType], sortKeys: $sortKeys);
                $checked = Evidence::durableContract($observed, $workflowType);
                self::assertSame('durable_history', $checked['source']);
                self::assertSame($observed['signal_contracts'], $checked['signal_contracts']);
            }
        }
        self::assertSame('int', Evidence::expectedSignalContracts('cwn.turns')[0]['parameters'][0]['type']);
    }

    /** Causal refusals: no durable contract, the #359 names-only contract, or any parameter metadata drift. */
    public function testTheInitialRunContractCheckRefusesAMissingOrDifferentContract(): void
    {
        $candidate = self::contracts(true);
        $namesOnly = self::contracts(true, false);
        $turns = self::observed($candidate['cwn.turns']);
        $refusals = [
            'no observation' => [null, 'cwn.turns', 'condition_evidence_contract_not_durable'],
            'run started before the worker contract' => [self::observed(null), 'cwn.false_wake', 'condition_evidence_contract_not_durable'],
            'names-only #359 contract' => [self::observed($namesOnly['cwn.duplicate']), 'cwn.duplicate', 'condition_evidence_contract_parameters'],
            'another type\'s contract' => [self::observed($candidate['cwn.own_budget']), 'cwn.false_wake', 'condition_evidence_contract_signals'],
            'decision typed string' => [self::mutate($turns, 'type', 'string'), 'cwn.turns', 'condition_evidence_contract_parameters'],
            'decision position as a string' => [self::mutate($turns, 'position', '0'), 'cwn.turns', 'condition_evidence_contract_parameters'],
            'decision nullable' => [self::mutate($turns, 'allows_null', true), 'cwn.turns', 'condition_evidence_contract_parameters'],
            'decision with a default' => [self::mutate($turns, 'default_available', true), 'cwn.turns', 'condition_evidence_contract_parameters'],
        ];
        foreach ($refusals as $label => [$observed, $workflowType, $code]) {
            try {
                Evidence::durableContract($observed, $workflowType);
                self::fail($label.' was accepted');
            } catch (\RuntimeException $error) {
                self::assertSame($code, $error->getMessage(), $label);
            }
        }
    }

    /**
     * A run observation's command_contract, transcribed from frozen Native
     * 3.2.0 (worker_contracts::declaration, children.rs start capture and
     * run_observation::command_contract). Null is a run started with no worker
     * contract.
     *
     * @param array<string, mixed>|null $entry
     * @return array<string, mixed>
     */
    private static function observed(?array $entry, bool $sortKeys = false): array
    {
        if ($entry === null) {
            return ['signals' => [], 'signal_contracts' => [], 'signal_targets' => [], 'source' => 'unavailable'];
        }
        $entry = json_decode(json_encode($entry, JSON_THROW_ON_ERROR), true, 512, JSON_THROW_ON_ERROR);
        $names = array_map('strval', $entry['signals'] ?? []);
        $contracts = [];
        foreach ($entry['signal_contracts'] ?? [] as $handler) {
            $names[] = $handler['name'];
            $contracts[$handler['name']] ??= ['name' => $handler['name'], 'parameters' => array_values(array_filter(
                is_array($handler['parameters'] ?? null) ? $handler['parameters'] : [], 'is_array'))];
        }
        $names = array_values(array_unique($names));
        sort($names, SORT_STRING);
        ksort($contracts, SORT_STRING);
        $observed = ['signals' => $names, 'signal_contracts' => array_values($contracts), 'source' => 'durable_history'];
        if ($sortKeys) {
            $sort = static function (mixed $value) use (&$sort): mixed {
                if (!is_array($value)) {
                    return $value;
                }
                $value = array_map($sort, $value);
                array_is_list($value) || ksort($value, SORT_STRING);

                return $value;
            };
            $observed = $sort($observed);
        }

        return $observed;
    }

    /** @param array<string, mixed> $observed */
    private static function mutate(array $observed, string $field, mixed $value): array
    {
        $observed['signal_contracts'][0]['parameters'][0][$field] = $value;

        return $observed;
    }

    /** @return array<string, array<string, mixed>> */
    private static function contracts(bool $declare, bool $signatures = true): array
    {
        $worker = new \DurableWorkflow\Worker(new \DurableWorkflow\Client('http://127.0.0.1:9', namespace: 'default', workerToken: 'unused'), 'q', workerId: 'w');
        foreach (array_keys(Evidence::DECLARED_SIGNALS) as $workflowType) {
            $worker->registerWorkflow($workflowType, static fn (\DurableWorkflow\Worker\WorkflowContext $context): mixed => null);
        }
        if ($declare && $signatures) {
            Evidence::declareSignals($worker);
        } elseif ($declare) {
            foreach (Evidence::DECLARED_SIGNALS as $workflowType => $names) {
                foreach ($names as $name) {
                    $worker->declareSignal($workflowType, $name);
                }
            }
        }

        return (new \ReflectionMethod($worker, 'workflowCommandContracts'))->invoke($worker);
    }

    /**
     * Each cwnSignal(...) call in the qualifier with its workflow type and literal
     * arguments; the C5 turn variable is an int (sampled as 1).
     *
     * @return list<array{string, string, list<mixed>}>
     */
    private static function sentSignals(): array
    {
        $source = (string) file_get_contents(__DIR__.'/../scripts/qualify-condition-wait-native.php');
        $types = ['go' => null, 'noise' => 'cwn.duplicate', 'first' => 'cwn.own_budget', 'second' => 'cwn.own_budget', 'decision' => 'cwn.turns'];
        preg_match_all("/cwnSignal\\(\\\$client, \\\$handle->workflowId, '([a-z]+)', \\[([^\\]]*)\\]\\);/", $source, $calls, PREG_SET_ORDER);
        $sent = [];
        $goCount = 0;
        foreach ($calls as [, $name, $argument]) {
            $value = match (true) {
                preg_match("/^'([^']*)'$/", $argument, $string) === 1 => $string[1],
                preg_match('/^-?\d+$/', $argument) === 1 => (int) $argument,
                $argument === '$turn' => 1,
                default => throw new \RuntimeException('unrecognized signal argument '.$argument),
            };
            $type = $types[$name] ?? (++$goCount === 1 ? 'cwn.false_wake' : 'cwn.duplicate');
            $sent[] = [$type, $name, [$value]];
        }

        return $sent;
    }

    /**
     * Native 3.2.0 signal admission for a run whose WorkflowStarted payload holds
     * this contract (durable source): server 3c6f1eda rust/src/runtime/store.rs
     * signal path (≈1053–1095) and validate_arguments (≈5233–5290), transcribed.
     * Returns null when admitted, otherwise the refusal reason.
     *
     * @param array<string, mixed> $contract
     * @param list<mixed> $values
     */
    private static function nativeAdmission(array $contract, string $name, array $values): ?string
    {
        if (!in_array($name, $contract['signals'], true)) {
            return 'unknown_signal';
        }
        $parameters = array_column($contract['signal_contracts'], 'parameters', 'name')[$name] ?? null;
        if ($parameters === null) {
            return null; // a declared signal without a parameter contract takes its arguments as given
        }
        $variadic = $parameters !== [] && end($parameters)['variadic'] === true;
        $fixed = count($parameters) - (int) $variadic;
        foreach ($parameters as $index => $parameter) {
            if ($parameter['default_available'] === true || ($parameter['variadic'] === true && $index + 1 !== count($parameters))
                || ($variadic && $index < $fixed && $parameter['required'] !== true)) {
                return 'unsupported_signal_contract';
            }
        }
        if (count($values) < $fixed || (!$variadic && count($parameters) !== count($values))) {
            return 'invalid_signal_arguments';
        }
        foreach ($values as $index => $value) {
            $parameter = $parameters[min($index, $fixed)];
            if ($value === null) {
                if ($parameter['allows_null'] ?? ($parameter['type'] === null || $parameter['type'] === '')) {
                    continue;
                }
                return 'invalid_signal_arguments';
            }
            $valid = match ($parameter['type']) {
                null, 'mixed' => true,
                'array' => is_array($value),
                'int' => is_int($value),
                'float' => is_int($value) || is_float($value),
                'bool' => is_bool($value),
                'string' => is_string($value),
                default => null,
            };
            if ($valid === null) {
                return 'unsupported_signal_contract';
            }
            if (!$valid) {
                return 'invalid_signal_arguments';
            }
        }

        return null;
    }

    public function testMalformedHistoryIsRefused(): void
    {
        $this->expectException(RuntimeException::class);
        Evidence::events([['event_type' => 'ConditionWaitOpened']]);
    }

    /** @return list<array<string, mixed>> */
    private static function falseWake(int $reopenTimeout, float $firedAt = 20.0): array
    {
        return [
            self::opened(0.0, 'w1', 'go', 20), self::scheduled(0.0, 't1', 'w1', 20),
            self::event(5.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(5.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(5.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]),
            self::opened(5.0, 'w2', 'go', $reopenTimeout, 2), self::scheduled(5.0, 't2', 'w2', $reopenTimeout, 2),
            self::event($firedAt, 'TimerFired', ['timer_id' => 't2', 'sequence' => 2]),
            self::event($firedAt + 0.2, 'WorkflowCompleted'),
        ];
    }

    /** @return list<array<string, mixed>> */
    private static function duplicate(bool $overlap): array
    {
        $history = [self::event(0.0, 'ActivityScheduled'), self::event(0.5, 'SignalReceived', ['signal_name' => 'noise']),
            self::event(2.0, 'ActivityCompleted'), self::opened(2.1, 'w1', 'go', 60), self::scheduled(2.1, 't1', 'w1', 60)];
        if ($overlap) {
            $history[] = self::opened(2.12, 'w2', 'go', 60, 2);
        }
        return [...$history, self::event(3.0, 'SignalReceived', ['signal_name' => 'go']),
            self::event(3.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'w1', 'sequence' => 1]),
            self::event(3.0, 'TimerCancelled', ['timer_id' => 't1', 'sequence' => 1]), self::event(3.1, 'WorkflowCompleted')];
    }

    /** @return list<array<string, mixed>> */
    private static function ownBudget(int $reopenTimeout): array
    {
        return [
            self::opened(0.0, 'a1', 'first', 30), self::scheduled(0.0, 'ta', 'a1', 30),
            self::event(10.0, 'SignalReceived', ['signal_name' => 'first']),
            self::event(10.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'a1', 'sequence' => 1]),
            self::event(10.0, 'TimerCancelled', ['timer_id' => 'ta', 'sequence' => 1]),
            self::opened(12.0, 'b1', 'second', 20, 2), self::scheduled(12.0, 'tb', 'b1', 20, 2),
            self::event(17.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event(17.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b1', 'sequence' => 2]),
            self::event(17.0, 'TimerCancelled', ['timer_id' => 'tb', 'sequence' => 2]),
            self::opened(17.0, 'b2', 'second', $reopenTimeout, 3), self::scheduled(17.0, 'tc', 'b2', $reopenTimeout, 3),
            self::event(19.0, 'SignalReceived', ['signal_name' => 'second']),
            self::event(19.0, 'ConditionWaitSatisfied', ['condition_wait_id' => 'b2', 'sequence' => 3]),
            self::event(19.0, 'TimerCancelled', ['timer_id' => 'tc', 'sequence' => 3]),
            self::event(19.1, 'WorkflowCompleted'),
        ];
    }

    /** @return array<string, mixed> */
    private static function opened(float $at, string $id, string $key, int $timeout, int $sequence = 1): array
    {
        return self::event($at, 'ConditionWaitOpened', ['condition_wait_id' => $id, 'condition_key' => $key,
            'timeout_seconds' => $timeout, 'sequence' => $sequence]);
    }

    /** @return array<string, mixed> */
    private static function scheduled(float $at, string $timer, string $wait, int $seconds, int $sequence = 1): array
    {
        return self::event($at, 'TimerScheduled', ['timer_id' => $timer, 'sequence' => $sequence, 'delay_seconds' => $seconds,
            'timer_kind' => 'condition_timeout', 'condition_wait_id' => $wait]);
    }

    /** @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function event(float $at, string $type, array $payload = []): array
    {
        $stamp = \DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', self::T0 + $at))->format('Y-m-d\TH:i:s.uP');

        return ['event_type' => $type, 'recorded_at' => $stamp, 'payload' => $payload];
    }
}
