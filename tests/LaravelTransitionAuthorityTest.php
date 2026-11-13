<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The embedded-v2 to service-mode transition check CI runs against the
 * installed Workflow package. The historical fixture is published Workflow 2.21.0's
 * resources/laravel-embedded-upgrade-contract.json, byte for byte
 * (SHA-256 1e65a6ffa6f2a1feba53ff0b5f7782a9ac8d7a3560f31c44c040767c592308c6).
 */
final class LaravelTransitionAuthorityTest extends TestCase
{
    private const WORKFLOW_2_17_1 = __DIR__.'/fixtures/laravel-transition/workflow-2.17.1';

    private const WORKFLOW_2_21_0 = __DIR__.'/fixtures/laravel-transition/workflow-2.21.0';

    private string $directory;

    protected function setUp(): void
    {
        $this->directory = sys_get_temp_dir().'/dw-laravel-authority-'.bin2hex(random_bytes(8));
        mkdir($this->directory, 0700);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->directory.'/*') ?: [] as $path) {
            unlink($path);
        }
        rmdir($this->directory);
    }

    public function testHistoricalPublishedAuthorityCannotAuthorizeTheNewPatchedServiceFloor(): void
    {
        [$status, $errors] = $this->check(dirname(__DIR__).'/docs/laravel-adoption-contract.json');

        self::assertSame(1, $status);
        self::assertStringContainsString('Embedded and service modes publish different Laravel constraints.', $errors);
    }

    public function testHistoricalMatchingSupportStillPassesTheUnchangedChecker(): void
    {
        // This is an inert historical service-contract copy. It cannot qualify
        // the current SDK floor or replace the installed corrected producer gate.
        $contract = $this->contract();
        $contract['framework']['constraint'] = '^12.69.0|^13.30.0';
        foreach ($contract['framework']['qualification_matrix'] as $index => $cell) {
            $floor = $cell['laravel'] === '12' ? '12.69.0' : '13.30.0';
            $contract['framework']['qualification_matrix'][$index]['package'] = 'laravel/framework:^'.$floor;
            $contract['framework']['qualification_matrix'][$index]['exercised_package'] = 'laravel/framework:'.$floor;
        }
        $path = $this->directory.'/historical-service.json';
        file_put_contents($path, json_encode($contract, JSON_THROW_ON_ERROR));
        [$status, $errors] = $this->check($path);
        self::assertSame('', $errors);
        self::assertSame(0, $status);
    }

    public function testPublishedAuthorityFixtureRetainsItsExactPackageBytesAndStableV1Scope(): void
    {
        $path = self::WORKFLOW_2_21_0.'/resources/laravel-embedded-upgrade-contract.json';
        self::assertSame('1e65a6ffa6f2a1feba53ff0b5f7782a9ac8d7a3560f31c44c040767c592308c6', hash_file('sha256', $path));
        $published = json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
        $previous = json_decode((string) file_get_contents(self::WORKFLOW_2_17_1.'/resources/laravel-embedded-upgrade-contract.json'),
            true, 512, JSON_THROW_ON_ERROR);
        self::assertSame($previous['supported_intersection']['authority']['stable_v1'],
            $published['supported_intersection']['authority']['stable_v1']);
    }

    public function testPreviousEmbeddedSupportBoundaryDoesNotAuthorizeTheNewServiceFloor(): void
    {
        [$status, $errors] = $this->check(dirname(__DIR__).'/docs/laravel-adoption-contract.json', self::WORKFLOW_2_17_1);

        self::assertSame(1, $status);
        self::assertStringContainsString('Embedded and service modes publish different Laravel constraints.', $errors);
        self::assertStringContainsString('Embedded and service modes publish different PHP constraints.', $errors);
    }

    /** @param callable(array<string, mixed>): array<string, mixed> $change */
    #[DataProvider('counterfactuals')]
    public function testDriftedServiceAuthorityFailsTheTransitionCheck(callable $change, string $expected): void
    {
        $contract = $change($this->contract());
        $path = $this->directory.'/contract.json';
        file_put_contents($path, json_encode($contract, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));

        [$status, $errors] = $this->check($path);

        self::assertSame(1, $status);
        self::assertStringContainsString($expected, $errors);
    }

    /** @return iterable<string, array{callable(array<string, mixed>): array<string, mixed>, string}> */
    public static function counterfactuals(): iterable
    {
        // SDK 3.8.1 published these major-only ranges.
        $majorOnly = ['10' => '^10.0', '11' => '^11.0', '12' => '^12.0', '13' => '^13.0'];
        yield 'SDK 3.8.1 major-only constraint and packages' => [static function (array $contract) use ($majorOnly): array {
            $contract['framework']['constraint'] = implode('|', $majorOnly);
            foreach ($contract['framework']['qualification_matrix'] as $index => $cell) {
                $contract['framework']['qualification_matrix'][$index]['package'] = 'laravel/framework:'.$majorOnly[$cell['laravel']];
            }

            return $contract;
        }, 'Embedded and service modes publish different Laravel constraints.'];
        yield 'major-only constraint alone' => [static function (array $contract): array {
            $contract['framework']['constraint'] = '^10.0|^11.0|^12.0|^13.0';

            return $contract;
        }, 'Embedded and service modes publish different Laravel constraints.'];
        yield 'major-only qualification package' => [static function (array $contract): array {
            $contract['framework']['qualification_matrix'][0]['package'] = 'laravel/framework:^12.0';

            return $contract;
        }, 'Laravel 12 on PHP 8.2 does not qualify the published floor.'];
        yield 'cell exercises a later release than its floor' => [static function (array $contract): array {
            $contract['framework']['qualification_matrix'][0]['exercised_package'] = 'laravel/framework:12.73.0';

            return $contract;
        }, 'Laravel 12 on PHP 8.2 does not exercise exactly the published floor.'];
        yield 'cell exercises its caret range' => [static function (array $contract): array {
            $contract['framework']['qualification_matrix'][0]['exercised_package'] = 'laravel/framework:^12.72.0';

            return $contract;
        }, 'Laravel 12 on PHP 8.2 does not exercise exactly the published floor.'];
        yield 'pre-fix floor' => [static function (array $contract): array {
            $contract['framework']['constraint'] = '^12.68.0|^13.41.0';

            return $contract;
        }, 'Embedded and service modes publish different Laravel constraints.'];
        yield 'PHP constraint drift' => [static function (array $contract): array {
            $contract['framework']['php_constraint'] = '^8.1';

            return $contract;
        }, 'Embedded and service modes publish different PHP constraints.'];
        yield 'restored Laravel 9 cell' => [static function (array $contract): array {
            array_unshift($contract['framework']['qualification_matrix'],
                ['laravel' => '9', 'php' => '8.1', 'package' => 'laravel/framework:^9.52.21', 'bootstrap' => 'existing_supported_lock']);

            return $contract;
        }, 'Embedded and service modes publish different Laravel/PHP support cells.'];
        yield 'other embedded authority' => [static function (array $contract): array {
            $contract['embedded_transition_authority']['version'] = 2;

            return $contract;
        }, 'did not consume the installed embedded transition authority'];
    }

    /** @return array<string, mixed> */
    private function contract(): array
    {
        return json_decode((string) file_get_contents(dirname(__DIR__).'/docs/laravel-adoption-contract.json'),
            true, 512, JSON_THROW_ON_ERROR);
    }

    /** @return array{int, string} */
    private function check(string $contract, string $workflow = self::WORKFLOW_2_21_0): array
    {
        $process = proc_open([PHP_BINARY, dirname(__DIR__).'/scripts/ci/check-laravel-transition-authority.php',
            $contract, $workflow], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        stream_get_contents($pipes[1]);
        $errors = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        return [proc_close($process), $errors];
    }
}
