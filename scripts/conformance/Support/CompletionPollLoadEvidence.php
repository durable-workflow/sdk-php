<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use RuntimeException;

require_once __DIR__.'/CompletionPollFailure.php';
require_once __DIR__.'/CompletionPollLoadProfile.php';

/** Bounded functional counts and descriptive timings, with no performance pass threshold. */
final class CompletionPollLoadEvidence
{
    /** @param list<array<string, mixed>> $reports
     * @return array<string, int>
     */
    public static function counts(array $reports): array
    {
        if (count($reports) !== CompletionPollLoadProfile::WORKERS) throw new RuntimeException('completion_load_reports_missing');
        $counts = [];
        $callbacks = 0;
        foreach ($reports as $report) {
            if (($report['outcome'] ?? null) !== 'pass' || !is_array($report['steps'] ?? null)
                || !is_int($report['workflow_callbacks'] ?? null) || $report['workflow_callbacks'] < 0) {
                throw new RuntimeException('completion_load_worker_failed');
            }
            $callbacks += $report['workflow_callbacks'];
            foreach ($report['steps'] as $key => $count) {
                if (!is_string($key) || preg_match('/\A([1-9][0-9]*):([0-9]+)\z/', $key, $parts) !== 1
                    || (int) $parts[1] > CompletionPollLoadProfile::WORKFLOWS || (int) $parts[2] >= CompletionPollLoadProfile::STEPS
                    || !is_int($count) || $count < 1) throw new RuntimeException('completion_load_report_shape');
                $counts[$key] = ($counts[$key] ?? 0) + $count;
            }
        }
        $expected = CompletionPollLoadProfile::WORKFLOWS * CompletionPollLoadProfile::STEPS;
        if (count($counts) !== $expected || array_sum($counts) !== $expected || $callbacks < CompletionPollLoadProfile::WORKFLOWS) {
            throw CompletionPollFailure::product('completion_load_functional_counts');
        }
        return ['unique_steps' => count($counts), 'step_callbacks' => array_sum($counts), 'workflow_callbacks' => $callbacks];
    }

    /** Authenticate and parse the same bounded result bytes. No package/autoload/network operation.
     * @return array<string, mixed>
     */
    public static function read(string $path, string $sha256): array
    {
        if (preg_match('/\A[a-f0-9]{64}\z/', $sha256) !== 1 || !stream_is_local($path)) throw new RuntimeException('completion_load_result_identity');
        $file = @fopen($path, 'rb');
        if ($file === false) throw new RuntimeException('completion_load_result_identity');
        try {
            $stat = fstat($file);
            $bytes = stream_get_contents($file, 1_048_577);
            if ($stat === false || ($stat['mode'] & 0170000) !== 0100000 || $bytes === false || strlen($bytes) > 1_048_576
                || !hash_equals($sha256, hash('sha256', $bytes))) throw new RuntimeException('completion_load_result_identity');
            $data = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
            if (!is_array($data) || array_is_list($data)) throw new RuntimeException('completion_load_result_shape');
            return $data;
        } finally {
            fclose($file);
        }
    }

    /** Three OFF and three ON results for exactly one tuple/backend/workload/conditions binding.
     * @param list<array<string, mixed>> $results
     * @return array<string, mixed>
     */
    public static function compare(array $results): array
    {
        if (count($results) !== 6) throw new RuntimeException('completion_load_six_results_required');
        $binding = null;
        $databases = $subjects = $values = [];
        foreach ($results as $result) {
            $cell = $result['result'] ?? [];
            $scenario = $cell['scenario'] ?? '';
            CompletionPollLoadProfile::validate(['scenario' => $scenario, 'load' => $cell['load'] ?? null]);
            if (!in_array($scenario, CompletionPollLoadProfile::SCENARIOS, true)
                || ($result['schema'] ?? null) !== 'durable-workflow.php-completion-polls-result/v1'
                || ($result['outcome'] ?? null) !== 'pass' || ($cell['outcome'] ?? null) !== 'pass'
                || ($cell['workload'] ?? null) !== CompletionPollLoadProfile::workload($scenario)
                || ($cell['workflow_results_verified'] ?? null) !== CompletionPollLoadProfile::WORKFLOWS
                || ($cell['enable_completion_pipelining'] ?? null) !== str_ends_with($scenario, '-on')
                || ($cell['relay_used'] ?? null) !== false || ($cell['request_trace_used'] ?? null) !== false
                || ($cell['counts']['unique_steps'] ?? null) !== CompletionPollLoadProfile::WORKFLOWS * CompletionPollLoadProfile::STEPS
                || ($cell['counts']['step_callbacks'] ?? null) !== CompletionPollLoadProfile::WORKFLOWS * CompletionPollLoadProfile::STEPS
                || ($cell['cleanup']['complete'] ?? null) !== true
                || (!is_float($cell['elapsed_seconds'] ?? null) && !is_int($cell['elapsed_seconds'] ?? null))
                || !is_finite((float) $cell['elapsed_seconds']) || $cell['elapsed_seconds'] <= 0) {
                throw new RuntimeException('completion_load_result_not_qualified');
            }
            $identity = $result['identity'] ?? [];
            foreach (['sdk_reference', 'caller_runner_reference'] as $key) {
                if (!is_string($identity[$key] ?? null) || preg_match('/\A[a-f0-9]{40}\z/', $identity[$key]) !== 1) throw new RuntimeException('completion_load_result_identity');
            }
            foreach (['lock_sha256', 'subject_sha256'] as $key) {
                if (!is_string($identity[$key] ?? null) || preg_match('/\A[a-f0-9]{64}\z/', $identity[$key]) !== 1) throw new RuntimeException('completion_load_result_identity');
            }
            if (!is_string($identity['sdk_version'] ?? null) || !is_string($result['server_version'] ?? null)
                || !is_string($result['caller_server_image'] ?? null)
                || preg_match('~\A(?:ghcr\.io/durable-workflow/server|durableworkflow/server)@sha256:[a-f0-9]{64}\z~', $result['caller_server_image']) !== 1) {
                throw new RuntimeException('completion_load_result_identity');
            }
            $selected = ['sdk_version' => $identity['sdk_version'], 'sdk_reference' => $identity['sdk_reference'],
                'runner_reference' => $identity['caller_runner_reference'], 'lock_sha256' => $identity['lock_sha256'],
                'server_version' => $result['server_version'], 'server_image' => $result['caller_server_image'],
                'workload' => $cell['workload'], 'backend' => $cell['load']['backend'], 'conditions_sha256' => $cell['load']['conditions_sha256']];
            if ($binding !== null && $selected !== $binding) throw new RuntimeException('completion_load_mixed_bindings');
            $binding = $selected;
            $database = $cell['load']['database_identity'];
            if (isset($databases[$database]) || isset($subjects[$identity['subject_sha256']])) throw new RuntimeException('completion_load_reused_cell');
            $databases[$database] = $subjects[$identity['subject_sha256']] = true;
            $mode = str_ends_with($scenario, '-on') ? 'on' : 'off';
            $repetition = $cell['load']['repetition'];
            if (isset($values[$mode][$repetition])) throw new RuntimeException('completion_load_duplicate_repetition');
            $values[$mode][$repetition] = $cell['elapsed_seconds'];
        }
        foreach (['off', 'on'] as $mode) {
            if (count($values[$mode] ?? []) !== 3) throw new RuntimeException('completion_load_repetitions_missing');
            ksort($values[$mode]);
        }
        $off = array_sum($values['off']) / 3;
        $on = array_sum($values['on']) / 3;
        return ['scope' => 'descriptive comparison; caller publication, fresh database and host/resource evidence remain separate',
            'binding' => $binding, 'elapsed_seconds' => array_map('array_values', $values),
            'mean_seconds' => ['off' => $off, 'on' => $on], 'off_over_on_ratio' => $off / $on,
            'on_slower_than_off' => $on > $off, 'performance_acceptance' => 'not_assessed'];
    }
}
