#!/usr/bin/env php
<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Composer\InstalledVersions;
use DurableWorkflow\Worker\GroupReplayClockEvidence;
use DurableWorkflow\Worker\ReplayClockHistory;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$tablePath = dirname(__DIR__, 2).'/tests/fixtures/replay-clock/canonical-v1.json';
$table = json_decode((string) file_get_contents($tablePath), true, flags: JSON_THROW_ON_ERROR);
$conversion = new ReflectionMethod(GroupReplayClockEvidence::class, 'canonicalMilliseconds');
$normalization = new ReflectionMethod(GroupReplayClockEvidence::class, 'utcMicroseconds');
$carbonSource = (new ReflectionMethod(CarbonImmutable::class, 'getPreciseTimestamp'))->getFileName();
$clockSource = (new ReflectionClass(GroupReplayClockEvidence::class))->getFileName();
$rows = [];
$pass = true;
foreach ($table['vectors'] as $row) {
    $time = ReplayClockHistory::parseTime($row['input']);
    if ($time === null) {
        throw new RuntimeException('Canonical numeric table contains an unsupported timestamp.');
    }
    $actual = $conversion->invoke(null, $time);
    $microseconds = $normalization->invoke(null, $time);
    $reconstructed = GroupReplayClockEvidence::time([['time' => $time, 'offset' => 0, 'order' => 0]], 0);
    $output = $reconstructed?->format('Y-m-d\TH:i:s.uP');
    $matches = $actual !== null && $microseconds !== null && (string) $actual === $row['milliseconds']
        && (string) $microseconds === $row['utc_microseconds'] && $output === $row['failure_time'];
    $pass = $pass && $matches;
    $legacy = CarbonImmutable::instance($time);
    $legacyMilliseconds = $legacy->getTimestampMs();
    $rows[] = [
        'input' => $row['input'],
        'canonical_expected_utc_microseconds' => $row['utc_microseconds'],
        'canonical_actual_utc_microseconds' => $microseconds === null ? null : (string) $microseconds,
        'canonical_expected_milliseconds' => $row['milliseconds'],
        'canonical_actual_milliseconds' => $actual === null ? null : (string) $actual,
        'canonical_reconstruction' => $output,
        'canonical_matches' => $matches,
        'legacy_carbon_milliseconds' => (string) $legacyMilliseconds,
        'legacy_carbon_reconstruction' => CarbonImmutable::createFromTimestampMs($legacyMilliseconds, 'UTC')->format('Y-m-d\TH:i:s.uP'),
    ];
}

$first = CarbonImmutable::parse('2026-11-06T00:00:00.123400Z');
$second = CarbonImmutable::parse('2026-11-06T00:00:00.123499Z');
$legacyPair = [$first->getTimestampMs(), $second->getTimestampMs()];
$canonicalPair = [$conversion->invoke(null, $first), $conversion->invoke(null, $second)];
echo json_encode([
    'schema' => 'durable-workflow.replay-clock-source-numeric-reference/v1',
    'scope' => 'source_numeric_reference_only',
    'canonical_decision' => $table['decision'],
    'php' => ['version' => PHP_VERSION, 'integer_bytes' => PHP_INT_SIZE, 'binary_sha256' => hash_file('sha256', PHP_BINARY)],
    'carbon' => [
        'version' => InstalledVersions::getPrettyVersion('nesbot/carbon'),
        'source_revision' => InstalledVersions::getReference('nesbot/carbon'),
        'timestamp_source_sha256' => is_string($carbonSource) ? hash_file('sha256', $carbonSource) : null,
    ],
    'canonical_table_sha256' => hash_file('sha256', $tablePath),
    'clock_source_sha256' => is_string($clockSource) ? hash_file('sha256', $clockSource) : null,
    'vectors' => $rows,
    'authored_success_pair' => [
        'inputs' => ['2026-11-06T00:00:00.123400Z', '2026-11-06T00:00:00.123499Z'],
        'legacy_carbon_milliseconds' => array_map('strval', $legacyPair),
        'legacy_carbon_selected_member' => $legacyPair[1] > $legacyPair[0] ? 1 : 0,
        'canonical_milliseconds' => array_map('strval', $canonicalPair),
        'canonical_selected_member' => $canonicalPair[1] > $canonicalPair[0] ? 1 : 0,
    ],
    'canonical_status' => $pass ? 'pass' : 'fail',
], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR), "\n";
exit($pass ? 0 : 1);
