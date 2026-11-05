<?php

declare(strict_types=1);

use DurableWorkflow\Qualification\StaticCredentialCases;
use DurableWorkflow\Qualification\StaticCredentialFixture;
use DurableWorkflow\Qualification\StaticCredentialProbeFailure;
use DurableWorkflow\Qualification\StaticCredentialQualification;
use DurableWorkflow\Qualification\StaticCredentialSubject;

if (PHP_SAPI !== 'cli' || $argc !== 4) {
    fwrite(STDERR, "Usage: php scripts/qualify-static-credentials.php AUTOLOAD SUBJECT_JSON SUBJECT_SHA256\n");
    exit(2);
}

require_once __DIR__.'/Support/StaticCredentialSubject.php';

$phase = 'preflight';
try {
    StaticCredentialSubject::assertFreshProcess();
    $subject = StaticCredentialSubject::read($argv[1], dirname(__DIR__).'/tests/fixtures/static-credential-consumer/dependency-intent.json',
        $argv[2], $argv[3]);
    if (PHP_VERSION !== $subject->intent['requirements']['php']) {
        throw new StaticCredentialProbeFailure('The proposed PHP interpreter differs.');
    }
    $fixture = new StaticCredentialFixture(dirname(__DIR__).'/tests/fixtures/auth-configuration/static-credentials-v1.json');
    require $argv[1];
    $facts = $subject->installed();
    require_once __DIR__.'/Support/StaticCredentialQualification.php';
    $phase = 'fixtures';
    $cases = new StaticCredentialCases($fixture);
    $count = 0;
    $failed = 0;
    foreach ([$cases->core(), $cases->frameworks()] as $group) {
        foreach ($group as $case) {
            $result = StaticCredentialQualification::check($case);
            echo json_encode($result, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)."\n";
            ++$count;
            $failed += $result['verdict'] === 'PASS' ? 0 : 1;
        }
    }
    $notPhp = [$fixture->data['binary_key_boundary']['id'], ...array_column($fixture->data['server_pre_selection_cases'], 'id')];
    echo json_encode(['qualification' => 'static-credential-fixtures-only', 'verdict' => $failed === 0 ? 'PASS' : 'FAIL',
        'cases' => $count, 'failed' => $failed, 'subject' => $facts, 'fixture_sha256' => StaticCredentialFixture::SHA256,
        'environment_input_exclusions' => $cases->exclusions(), 'not_php_scalar_cases' => $notPhp,
        'caller_preconditions_not_proved_here' => ['complete_advisory_enforced_graph', 'full_archive_and_installed_file_seal',
            'execution_admission', 'native_or_runtime_authentication']], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)."\n";
    exit($failed === 0 ? 0 : 1);
} catch (Throwable) {
    fwrite(STDERR, json_encode(['qualification' => 'static-credential-fixtures-only', 'verdict' => 'REFUSED', 'phase' => $phase],
        JSON_THROW_ON_ERROR)."\n");
    exit(2);
}
