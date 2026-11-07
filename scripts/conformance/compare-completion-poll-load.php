<?php

declare(strict_types=1);

use DurableWorkflow\Qualification\CompletionPollFailure;
use DurableWorkflow\Qualification\CompletionPollLoadEvidence;

require_once __DIR__.'/Support/CompletionPollLoadEvidence.php';

try {
    if (count($argv) !== 13) throw new RuntimeException('completion_load_comparison_arguments');
    $results = $digests = [];
    for ($index = 1; $index < 13; $index += 2) {
        $results[] = CompletionPollLoadEvidence::read($argv[$index], $argv[$index + 1]);
        $digests[] = $argv[$index + 1];
    }
    echo json_encode(['schema' => 'durable-workflow.php-completion-polls-load-comparison/v1',
        'result_sha256' => $digests, 'comparison' => CompletionPollLoadEvidence::compare($results)], JSON_THROW_ON_ERROR).PHP_EOL;
} catch (Throwable $error) {
    fwrite(STDERR, json_encode(CompletionPollFailure::diagnostic($error), JSON_THROW_ON_ERROR).PHP_EOL);
    exit(1);
}
