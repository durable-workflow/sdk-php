<?php

declare(strict_types=1);

use DurableWorkflow\Qualification\StaticCredentialCases;
use DurableWorkflow\Qualification\StaticCredentialFixture;
use DurableWorkflow\Qualification\StaticCredentialFrameworkProbe;
use DurableWorkflow\Qualification\StaticCredentialQualification;

require_once $argv[1];
require_once dirname(__DIR__, 2).'/scripts/conformance/Support/StaticCredentialQualification.php';

$framework = match ($argv[2] ?? null) {
    'laravel.php' => 'laravel',
    'symfony.php' => 'symfony',
    default => throw new RuntimeException('A supported framework source cell is required.'),
};
$fixture = new StaticCredentialFixture(dirname(__DIR__).'/fixtures/auth-configuration/static-credentials-v1.json');
$cases = new StaticCredentialCases($fixture);
$targets = StaticCredentialFrameworkProbe::targets();
$seen = [];
$count = 0;
$failed = 0;
foreach ($cases->frameworks() as $case) {
    if ($targets[$case['target']]['framework'] !== $framework) {
        continue;
    }
    $seen[$case['target']] = true;
    $result = StaticCredentialQualification::check($case);
    ++$count;
    if ($result['verdict'] !== 'PASS') {
        if ($failed < 5) {
            fwrite(STDERR, json_encode($result, JSON_THROW_ON_ERROR)."\n");
        }
        ++$failed;
    }
}
$expectedTargets = array_filter($targets, static fn (array $target): bool => $target['framework'] === $framework);
if ($count === 0 || count($seen) !== count($expectedTargets)) {
    throw new RuntimeException('The framework source fixture matrix is incomplete.');
}
echo json_encode(['qualification' => 'static-credential-framework-source', 'source_only' => true,
    'framework' => $framework, 'cases' => $count, 'failed' => $failed, 'targets' => count($seen)], JSON_THROW_ON_ERROR)."\n";
exit($failed === 0 ? 0 : 1);
