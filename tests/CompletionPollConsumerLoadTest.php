<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollLoad;
use DurableWorkflow\Qualification\CompletionPollLoadEvidence;
use DurableWorkflow\Qualification\CompletionPollLoadProfile;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollLoad.php';

final class CompletionPollConsumerLoadTest extends TestCase
{
    public function testSixReportsMustAccountForEveryLogicalStepExactlyOnce(): void
    {
        $reports = self::reports();
        self::assertSame(['unique_steps' => 1000, 'step_callbacks' => 1000, 'workflow_callbacks' => 200], CompletionPollLoadEvidence::counts($reports));
        $reports[1]['steps']['1:0'] = 1;
        $this->expectExceptionMessage('completion_load_functional_counts');
        CompletionPollLoadEvidence::counts($reports);
    }

    public function testMissingWorkCannotBeHiddenByACompletedWorkflowCount(): void
    {
        $reports = self::reports();
        unset($reports[0]['steps']['1:0']);
        $this->expectExceptionMessage('completion_load_functional_counts');
        CompletionPollLoadEvidence::counts($reports);
    }

    public function testAWorkerFailureCannotQualifyTiming(): void
    {
        $reports = self::reports();
        $reports[5]['outcome'] = 'runner-blocked';
        $this->expectExceptionMessage('completion_load_worker_failed');
        CompletionPollLoadEvidence::counts($reports);
    }

    public function testComparisonPreservesSlowRepetitionsAndReportsRegression(): void
    {
        $comparison = CompletionPollLoadEvidence::compare(self::results());
        self::assertSame([11, 12, 13], $comparison['elapsed_seconds']['off']);
        self::assertSame([21, 22, 23], $comparison['elapsed_seconds']['on']);
        self::assertTrue($comparison['on_slower_than_off']);
        self::assertSame('not_assessed', $comparison['performance_acceptance']);
        self::assertEqualsWithDelta(12 / 22, $comparison['off_over_on_ratio'], 0.00001);
    }

    #[DataProvider('invalidResults')]
    public function testComparisonRefusesMixedIncompleteOrReusedCells(string $mutation, string $reason): void
    {
        $results = self::results();
        match ($mutation) {
            'database' => $results[5]['result']['load']['database_identity'] = $results[0]['result']['load']['database_identity'],
            'subject' => $results[5]['identity']['subject_sha256'] = $results[0]['identity']['subject_sha256'],
            'conditions' => $results[5]['result']['load']['conditions_sha256'] = str_repeat('e', 64),
            'image' => $results[5]['caller_server_image'] = 'ghcr.io/durable-workflow/server@sha256:'.str_repeat('f', 64),
            'cleanup' => $results[5]['result']['cleanup']['complete'] = false,
            'option' => $results[5]['result']['enable_completion_pipelining'] = false,
            'repetition' => $results[5]['result']['load']['repetition'] = 1,
            'incomplete' => array_pop($results),
        };
        $this->expectExceptionMessage($reason);
        CompletionPollLoadEvidence::compare($results);
    }

    public static function invalidResults(): iterable
    {
        yield ['database', 'completion_load_reused_cell'];
        yield ['subject', 'completion_load_reused_cell'];
        yield ['conditions', 'completion_load_mixed_bindings'];
        yield ['image', 'completion_load_mixed_bindings'];
        yield ['cleanup', 'completion_load_result_not_qualified'];
        yield ['option', 'completion_load_result_not_qualified'];
        yield ['repetition', 'completion_load_duplicate_repetition'];
        yield ['incomplete', 'completion_load_six_results_required'];
    }

    public function testResultReadBindsTheParsedBytesToTheCallerDigest(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'load-evidence-');
        $bytes = json_encode(self::results()[0], JSON_THROW_ON_ERROR);
        file_put_contents($path, $bytes);
        try {
            self::assertSame(self::results()[0], CompletionPollLoadEvidence::read($path, hash('sha256', $bytes)));
            file_put_contents($path, str_replace('11', '99', $bytes));
            $this->expectExceptionMessage('completion_load_result_identity');
            CompletionPollLoadEvidence::read($path, hash('sha256', $bytes));
        } finally {
            unlink($path);
        }
    }

    public function testWorkerShutdownUsesOneGlobalGraceAndReapsEveryOwnedChild(): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_setsid')) $this->markTestSkipped('Requires Unix process groups.');
        $directory = sys_get_temp_dir().'/load-stop-'.bin2hex(random_bytes(6));
        mkdir($directory, 0700);
        $pids = [];
        try {
            for ($i = 0; $i < 6; ++$i) {
                $pid = pcntl_fork();
                self::assertNotSame(-1, $pid);
                if ($pid === 0) {
                    if (posix_setsid() === -1) exit(2);
                    pcntl_signal(SIGTERM, SIG_IGN);
                    file_put_contents($directory.'/'.$i, 'ready');
                    while (true) usleep(10_000);
                }
                $pids[] = $pid;
            }
            $until = hrtime(true) / 1e9 + 2;
            while (count(glob($directory.'/*')) !== 6) {
                if (hrtime(true) / 1e9 >= $until) self::fail('Child fixture readiness failed.');
                usleep(1000);
            }
            $started = hrtime(true);
            try {
                CompletionPollLoad::stop($pids, 0.1);
                self::fail('Forced shutdown cannot pass cleanup.');
            } catch (RuntimeException $error) {
                self::assertSame('completion_load_worker_stop_failed', $error->getMessage());
            }
            self::assertLessThan(0.5, (hrtime(true) - $started) / 1e9);
            foreach ($pids as $pid) {
                self::assertSame(-1, pcntl_waitpid($pid, $status, WNOHANG));
                self::assertFalse(posix_kill(-$pid, 0));
            }
        } finally {
            foreach ($pids as $pid) {
                posix_kill(-$pid, SIGKILL);
                posix_kill($pid, SIGKILL);
                pcntl_waitpid($pid, $status, WNOHANG);
            }
            foreach (glob($directory.'/*') as $file) unlink($file);
            rmdir($directory);
        }
    }

    private static function reports(): array
    {
        $reports = array_fill(0, 6, ['outcome' => 'pass', 'workflow_callbacks' => 0, 'steps' => []]);
        for ($index = 1; $index <= 200; ++$index) {
            $worker = ($index - 1) % 6;
            ++$reports[$worker]['workflow_callbacks'];
            for ($step = 0; $step < 5; ++$step) $reports[$worker]['steps'][$index.':'.$step] = 1;
        }
        return $reports;
    }

    private static function results(): array
    {
        $results = [];
        foreach (['off', 'on'] as $mode) {
            for ($repetition = 1; $repetition <= 3; ++$repetition) {
                $scenario = 'load-local-'.$mode;
                $results[] = ['schema' => 'durable-workflow.php-completion-polls-result/v1', 'outcome' => 'pass',
                    'identity' => ['sdk_version' => '9.0.0', 'sdk_reference' => str_repeat('a', 40), 'caller_runner_reference' => str_repeat('b', 40),
                        'lock_sha256' => str_repeat('c', 64), 'subject_sha256' => hash('sha256', $mode.$repetition)],
                    'server_version' => '9.0.0', 'caller_server_image' => 'ghcr.io/durable-workflow/server@sha256:'.str_repeat('d', 64),
                    'result' => ['scenario' => $scenario, 'outcome' => 'pass', 'workload' => CompletionPollLoadProfile::workload($scenario),
                        'load' => ['backend' => 'sqlite', 'database_identity' => 'fresh-'.$mode.'-'.$repetition, 'repetition' => $repetition,
                            'conditions_sha256' => str_repeat('a', 64)], 'enable_completion_pipelining' => $mode === 'on',
                        'relay_used' => false, 'request_trace_used' => false, 'elapsed_seconds' => ($mode === 'on' ? 20 : 10) + $repetition,
                        'workflow_results_verified' => 200, 'counts' => ['unique_steps' => 1000, 'step_callbacks' => 1000], 'cleanup' => ['complete' => true]]];
            }
        }
        return $results;
    }
}
