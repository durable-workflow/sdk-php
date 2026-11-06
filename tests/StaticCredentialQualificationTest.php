<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\StaticCredentialCases;
use DurableWorkflow\Qualification\StaticCredentialFixture;
use DurableWorkflow\Qualification\StaticCredentialFrameworkProbe;
use DurableWorkflow\Qualification\StaticCredentialProbeFailure;
use DurableWorkflow\Qualification\StaticCredentialQualification;
use DurableWorkflow\Qualification\StaticCredentialTransportSpy;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

require_once dirname(__DIR__).'/scripts/conformance/Support/StaticCredentialQualification.php';

final class StaticCredentialQualificationTest extends TestCase
{
    public function testEveryCoreCaseUsesTheActualSourcePolicy(): void
    {
        $seen = [];
        $count = 0;
        $scalars = [];
        $scalarIds = self::scalarIds();
        foreach (self::cases()->core() as $case) {
            $key = self::key($case);
            self::assertArrayNotHasKey($key, $seen);
            $seen[$key] = true;
            $result = StaticCredentialQualification::check($case);
            self::assertSame('PASS', $result['verdict'], $key.' / '.json_encode($result));
            self::assertSame(0, $result['transport_attempts']);
            if (isset($scalarIds[$case['id']])) {
                $scalars[$case['target']] = ($scalars[$case['target']] ?? 0) + 1;
            }
            if (in_array($case['id'], ['malformed-unused-worker', 'malformed-unused-control', 'malformed-masked-shared'], true)) {
                self::assertContains($case['target'], ['Client', 'TokenAuthentication']);
            }
            ++$count;
        }
        self::assertGreaterThan(5000, $count);
        self::assertSame(['Client' => 888, 'TokenAuthentication' => 888, 'ServiceConfiguration' => 888,
            'ServiceConfigurationConstructor' => 888, 'ServiceConfigurationAlias' => 592, 'ProcessCredentialResolver' => 879], $scalars);
    }

    public function testFrameworkInventoryRetainsTheSelectedPlaneAndOriginalEnvironmentBytes(): void
    {
        $targets = StaticCredentialFrameworkProbe::targets();
        $seen = [];
        $counts = [];
        $scalars = [];
        $scalarIds = self::scalarIds();
        foreach (self::cases()->frameworks() as $case) {
            $key = self::key($case);
            self::assertArrayNotHasKey($key, $seen);
            $seen[$key] = true;
            self::assertSame($targets[$case['target']]['plane'], $case['plane']);
            self::assertContains($case['expected'], ['preserve', 'reject', 'role-refusal', 'mode-refusal']);
            self::assertNotContains($case['id'], ['malformed-unused-worker', 'malformed-unused-control', 'malformed-masked-shared',
                'php-control-scoped-before-shared', 'php-worker-scoped-before-shared']);
            if (isset($scalarIds[$case['id']])) {
                $scalars[$case['target']] = ($scalars[$case['target']] ?? 0) + 1;
            }
            if ($targets[$case['target']]['framework'] === 'laravel') {
                foreach ($case['options'] as $value) {
                    self::assertTrue($value === null || !str_contains($value, "\0"));
                }
            }
            $counts[$case['target']] = ($counts[$case['target']] ?? 0) + 1;
        }
        self::assertEqualsCanonicalizing(array_keys($targets), array_keys($counts));
        foreach ($counts as $count) {
            self::assertGreaterThan(800, $count);
        }
        foreach ($targets as $target => $metadata) {
            self::assertSame($metadata['framework'] === 'laravel' ? 879 : 888, $scalars[$target]);
        }
        foreach (self::cases()->exclusions() as $excluded) {
            self::assertSame('os_environment_cannot_represent_nul', $excluded['reason']);
        }
    }

    public function testNativeDiagnosticValuesAndAdditionalFieldsCannotReachReports(): void
    {
        $case = self::ordinary();
        $sentinel = 'fixture-value-must-stay-private';
        $case['expected'] = 'reject';
        $case['setting'] = 'token';
        $error = new InvalidArgumentException('Invalid static credential setting token. '.$sentinel);
        $result = StaticCredentialQualification::verdict($case, null, $error, 0);
        self::assertSame('FAIL', $result['verdict']);
        self::assertNull($result['setting']);
        self::assertStringNotContainsString($sentinel, (string) json_encode($result));
        $exact = StaticCredentialQualification::verdict($case, null,
            new InvalidArgumentException('Invalid static credential setting token.'), 0);
        self::assertSame('PASS', $exact['verdict']);
        self::assertSame('token', $exact['setting']);
    }

    public function testHeaderSubstitutionAndTransportAttemptsCannotPass(): void
    {
        $case = self::ordinary();
        $extra = $case['output'] + ['X-Fixture-Unexpected' => 'fixture-value-must-stay-private'];
        $result = StaticCredentialQualification::verdict($case, $extra, null, 0);
        self::assertSame('FAIL', $result['verdict']);
        self::assertStringNotContainsString('fixture-value-must-stay-private', (string) json_encode($result));
        $attempted = StaticCredentialQualification::verdict($case, $case['output'], null, 1);
        self::assertSame('FAIL', $attempted['verdict']);
        self::assertSame('transport_attempted', $attempted['failure']);
        $case['expected'] = 'unsupported';
        self::assertSame('FAIL', StaticCredentialQualification::verdict($case, $case['output'], null, 0)['verdict']);
    }

    #[DataProvider('missingFrameworks')]
    public function testUnavailableFrameworksRefuseInsteadOfFallingBack(string $target, string $packageClass, string $plane): void
    {
        if (class_exists($packageClass)) {
            self::markTestSkipped('The missing-framework negative requires an absent optional package.');
        }
        $this->expectException(StaticCredentialProbeFailure::class);
        StaticCredentialFrameworkProbe::headers($target, [], $plane, new StaticCredentialTransportSpy());
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function missingFrameworks(): iterable
    {
        yield 'Laravel' => ['LaravelClient', 'Illuminate\\Foundation\\Application', 'control'];
        yield 'Symfony' => ['SymfonyClient', 'Symfony\\Bundle\\FrameworkBundle\\FrameworkBundle', 'control'];
    }

    /** @param array<string, mixed> $case */
    private static function key(array $case): string
    {
        return $case['id'].'/'.$case['target'].'/'.$case['plane'].'/'.$case['role'];
    }

    private static function cases(): StaticCredentialCases
    {
        return new StaticCredentialCases(new StaticCredentialFixture(__DIR__.'/fixtures/auth-configuration/static-credentials-v1.json'));
    }

    /** @return array<string, true> */
    private static function scalarIds(): array
    {
        $fixture = new StaticCredentialFixture(__DIR__.'/fixtures/auth-configuration/static-credentials-v1.json');
        return array_fill_keys([...array_column($fixture->data['common_scalar_cases'], 'id'),
            ...array_column($fixture->data['php_byte_loader_cases'], 'id')], true);
    }

    /** @return array<string, mixed> */
    private static function ordinary(): array
    {
        foreach (self::cases()->core() as $case) {
            return $case;
        }
        throw new StaticCredentialProbeFailure('The core fixture inventory is empty.');
    }
}
