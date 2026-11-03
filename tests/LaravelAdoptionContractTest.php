<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class LaravelAdoptionContractTest extends TestCase
{
    /** @var array<string, mixed> */
    private array $contract;

    /** @var array<string, mixed> */
    private array $composer;

    protected function setUp(): void
    {
        $this->contract = $this->json('docs/laravel-adoption-contract.json');
        $this->composer = $this->json('composer.json');
    }

    public function testBridgeCompatibilityMatchesTheEmbeddedLaravelSurface(): void
    {
        self::assertSame(
            $this->composer['extra']['durable-workflow']['frameworks']['laravel'],
            $this->contract['framework']['constraint'],
        );
        self::assertSame(
            $this->composer['require']['php'],
            $this->contract['framework']['php_constraint'],
        );
        self::assertSame(
            [
                ['laravel' => '10', 'php' => '8.1', 'package' => 'laravel/framework:^10.50.4', 'exercised_package' => 'laravel/framework:10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.2', 'package' => 'laravel/framework:^10.50.4', 'exercised_package' => 'laravel/framework:10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.2', 'package' => 'laravel/framework:^11.56.1', 'exercised_package' => 'laravel/framework:11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.2', 'package' => 'laravel/framework:^12.69.0', 'exercised_package' => 'laravel/framework:12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '10', 'php' => '8.3', 'package' => 'laravel/framework:^10.50.4', 'exercised_package' => 'laravel/framework:10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.3', 'package' => 'laravel/framework:^11.56.1', 'exercised_package' => 'laravel/framework:11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.3', 'package' => 'laravel/framework:^12.69.0', 'exercised_package' => 'laravel/framework:12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.3', 'package' => 'laravel/framework:^13.30.0', 'exercised_package' => 'laravel/framework:13.30.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '10', 'php' => '8.4', 'package' => 'laravel/framework:^10.50.4', 'exercised_package' => 'laravel/framework:10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.4', 'package' => 'laravel/framework:^11.56.1', 'exercised_package' => 'laravel/framework:11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.4', 'package' => 'laravel/framework:^12.69.0', 'exercised_package' => 'laravel/framework:12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.4', 'package' => 'laravel/framework:^13.30.0', 'exercised_package' => 'laravel/framework:13.30.0', 'bootstrap' => 'fresh_supported_install'],
            ],
            $this->contract['framework']['qualification_matrix'],
        );
        self::assertSame([
            'package' => 'durable-workflow/workflow',
            'manifest' => 'resources/laravel-embedded-upgrade-contract.json',
            'schema' => 'durable-workflow.laravel-embedded-upgrade.contract',
            'version' => 1,
        ], $this->contract['embedded_transition_authority']);
    }

    public function testServiceModePublishesThePatchedLaravelFloorsForGhsaF9x37vq9Cph6(): void
    {
        // The minimum Laravel releases with the TokenGuard fix, as Workflow 2.17.1 records them.
        $floors = '^10.50.4|^11.56.1|^12.69.0|^13.30.0';
        self::assertSame($floors, $this->contract['framework']['constraint']);
        self::assertSame($floors, $this->composer['extra']['durable-workflow']['frameworks']['laravel']);
        foreach (['10.50.4', '11.56.1', '12.69.0', '13.30.0'] as $floor) {
            self::assertStringContainsString($floor, $this->composer['suggest']['laravel/framework']);
        }
        self::assertArrayNotHasKey('laravel/framework', $this->composer['conflict'] ?? [],
            'The positive support boundary must not change an application\'s Laravel resolution.');
    }

    public function testEveryQualificationCellNamesAndExercisesItsMajorsPublishedFloor(): void
    {
        $floors = [];
        foreach (explode('|', $this->contract['framework']['constraint']) as $range) {
            self::assertMatchesRegularExpression('/\A\^([1-9][0-9]*)\.[0-9]+\.[0-9]+\z/', $range);
            $floors[explode('.', substr($range, 1))[0]] = $range;
        }
        $majors = [];
        foreach ($this->contract['framework']['qualification_matrix'] as $cell) {
            // The public caret support range, and the exact minimum release CI installs.
            self::assertSame('laravel/framework:'.$floors[$cell['laravel']], $cell['package']);
            self::assertSame('laravel/framework:'.substr($floors[$cell['laravel']], 1), $cell['exercised_package']);
            $majors[$cell['laravel']] = true;
        }
        self::assertSame(array_keys($floors), array_keys($majors));
    }

    public function testCiInstallsExactlyEachPublishedLaravelMinimum(): void
    {
        self::assertSame([], self::ciMinimumQualificationErrors(
            (string) file_get_contents(dirname(__DIR__).'/.github/workflows/ci.yml'),
            $this->contract['framework']['qualification_matrix'],
        ));
    }

    /** @param callable(string): string $change */
    #[DataProvider('ciCounterfactuals')]
    public function testCiThatCanResolveALaterLaravelReleaseIsRefused(callable $change, string $expected): void
    {
        $workflow = (string) file_get_contents(dirname(__DIR__).'/.github/workflows/ci.yml');
        $changed = $change($workflow);
        self::assertNotSame($workflow, $changed, 'The counterfactual must change the workflow.');

        self::assertContains($expected, self::ciMinimumQualificationErrors(
            $changed,
            $this->contract['framework']['qualification_matrix'],
        ));
    }

    /** @return iterable<string, array{callable(string): string, string}> */
    public static function ciCounterfactuals(): iterable
    {
        $first = static fn (string $search, string $replace): \Closure =>
            static function (string $workflow) use ($search, $replace): string {
                $offset = strpos($workflow, $search);

                return $offset === false ? $workflow : substr_replace($workflow, $replace, $offset, strlen($search));
            };
        yield 'caret range installed instead of the exact minimum' => [$first(
            'composer require "$FRAMEWORK_EXERCISED_PACKAGE" \\',
            'composer require "$FRAMEWORK_PACKAGE" \\',
        ), 'The Laravel consumer must install exactly $FRAMEWORK_EXERCISED_PACKAGE.'];
        yield 'exact minimum removed from a cell' => [$first(
            "            exercised_package: 'laravel/framework:11.56.1'\n",
            '',
        ), 'The CI Laravel cells differ from the adoption contract.'];
        yield 'cell exercises a later 11.x release' => [$first(
            "exercised_package: 'laravel/framework:11.56.1'",
            "exercised_package: 'laravel/framework:11.57.1'",
        ), 'The CI Laravel cells differ from the adoption contract.'];
        yield 'cell exercises a caret range' => [$first(
            "exercised_package: 'laravel/framework:12.69.0'",
            "exercised_package: 'laravel/framework:^12.69.0'",
        ), 'The CI Laravel cells differ from the adoption contract.'];
        yield 'no installed-version assertion after the SDK' => [static fn (string $workflow): string => str_replace(
            "              --no-interaction --prefer-dist --no-progress\n"
            .'            php "$GITHUB_WORKSPACE/scripts/ci/assert-installed-version.php" "$consumer" laravel/framework "$exercised_version"'."\n"
            ."          else\n",
            "              --no-interaction --prefer-dist --no-progress\n          else\n",
            $workflow,
        ), 'The Laravel consumer must assert the exact release before and after installing the SDK.'];
        yield 'exact minimum not passed to the job' => [$first(
            "          FRAMEWORK_EXERCISED_PACKAGE: \${{ matrix.exercised_package }}\n          FRAMEWORK_CONSOLE_PACKAGE",
            '          FRAMEWORK_CONSOLE_PACKAGE',
        ), 'The Laravel consumer must install exactly $FRAMEWORK_EXERCISED_PACKAGE.'];
        yield 'fresh application left on the latest release' => [$first(
            'composer --working-dir="$application" require "$FRAMEWORK_EXERCISED_PACKAGE" \\',
            'composer --working-dir="$application" update \\',
        ), 'The fresh Laravel application must run exactly the cell\'s minimum release.'];
    }

    /**
     * @param list<array<string, string>> $matrix
     * @return list<string>
     */
    private static function ciMinimumQualificationErrors(string $workflow, array $matrix): array
    {
        $errors = [];
        preg_match_all(
            "/^ {10}- framework: Laravel\n {12}version: '([0-9]+)'\n {12}php: '([0-9.]+)'\n"
            ."^ {12}package: '([^']+)'\n {12}exercised_package: '([^']+)'\n {12}bootstrap: ([a-z_]+)\n/m",
            $workflow,
            $matches,
            PREG_SET_ORDER,
        );
        $cells = array_map(static fn (array $match): array => ['laravel' => $match[1], 'php' => $match[2],
            'package' => $match[3], 'exercised_package' => $match[4], 'bootstrap' => $match[5]], $matches);
        if ($cells !== $matrix || count($matches) !== substr_count($workflow, "- framework: Laravel\n")) {
            $errors[] = 'The CI Laravel cells differ from the adoption contract.';
        }

        $install = self::between($workflow, '      - name: Install SDK into a clean framework consumer', '          else');
        $assert = 'php "$GITHUB_WORKSPACE/scripts/ci/assert-installed-version.php" "$consumer" laravel/framework "$exercised_version"';
        $require = strpos($install, 'composer require "$FRAMEWORK_EXERCISED_PACKAGE" \\');
        if (!str_contains($install, 'FRAMEWORK_EXERCISED_PACKAGE: ${{ matrix.exercised_package }}')
            || $require === false || str_contains($install, 'composer require "$FRAMEWORK_PACKAGE"')) {
            $errors[] = 'The Laravel consumer must install exactly $FRAMEWORK_EXERCISED_PACKAGE.';
        }
        $before = $require === false ? false : strpos($install, $assert, $require);
        $sdk = $before === false ? false : strpos($install, 'composer require durable-workflow/sdk:@dev', $before);
        if ($sdk === false || strpos($install, $assert, $sdk) === false) {
            $errors[] = 'The Laravel consumer must assert the exact release before and after installing the SDK.';
        }

        $fresh = self::between($workflow, '      - name: Install SDK into a fresh Laravel application', '      - name: ');
        $pin = strpos($fresh, 'composer --working-dir="$application" require "$FRAMEWORK_EXERCISED_PACKAGE" \\');
        $freshAssert = 'php scripts/ci/assert-installed-version.php "$application" laravel/framework "$exercised_version"';
        $freshSdk = $pin === false ? false : strpos($fresh, 'require durable-workflow/sdk:@dev', $pin);
        if ($pin === false || $freshSdk === false || strpos($fresh, $freshAssert, $freshSdk) === false) {
            $errors[] = 'The fresh Laravel application must run exactly the cell\'s minimum release.';
        }

        return $errors;
    }

    private static function between(string $text, string $start, string $end): string
    {
        $from = strpos($text, $start);
        if ($from === false) {
            return '';
        }
        $to = strpos($text, $end, $from + strlen($start));

        return $to === false ? substr($text, $from) : substr($text, $from, $to - $from);
    }

    public function testEverySupportedOwnershipTransitionHasOneExplicitPolicy(): void
    {
        $transitions = [];
        foreach ($this->contract['transitions'] as $transition) {
            self::assertIsArray($transition);
            $transitions[$transition['id']] = $transition;
        }

        self::assertSame([
            'v1_to_v2_embedded' => [
                'id' => 'v1_to_v2_embedded',
                'from' => 'embedded_v1',
                'to' => 'embedded_v2',
                'authority' => 'embedded_transition_authority',
            ],
            'v1_to_v2_service' => [
                'id' => 'v1_to_v2_service',
                'from' => 'embedded_v1',
                'to' => 'service_v2',
                'ownership_change' => true,
                'open_run_policies' => ['drain', 'coexist_by_runtime'],
                'history_disposition' => 'explicit_state_migration_required',
                'rollback_boundary' => 'per_runtime',
            ],
            'v2_embedded_to_v2_service' => [
                'id' => 'v2_embedded_to_v2_service',
                'from' => 'embedded_v2',
                'to' => 'service_v2',
                'ownership_change' => true,
                'open_run_policies' => ['drain', 'coexist_by_runtime'],
                'history_disposition' => 'explicit_state_migration_required',
                'rollback_boundary' => 'per_runtime',
            ],
        ], $transitions);
    }

    public function testContinuityAndQualificationBoundariesAreMachineOwned(): void
    {
        self::assertSame(
            [
                'workflow_type',
                'workflow_id',
                'task_queue',
                'payload',
                'memo_search_metadata',
                'signals_updates',
                'rollback',
            ],
            array_column($this->contract['continuity'], 'surface'),
        );
        self::assertSame(
            ['local_laravel_runtime'],
            $this->contract['qualification']['embedded']['targets'],
        );
        self::assertSame(
            ['standalone_server', 'managed_cloud'],
            $this->contract['qualification']['service']['targets'],
        );
        self::assertTrue($this->contract['qualification']['service']['published_artifacts_required']);
        self::assertSame(
            'DurableWorkflow\\Bridge\\Laravel\\LaravelWorkflowClientInterface',
            $this->contract['representative_journey']['application_client'],
        );
        self::assertSame(
            'DurableWorkflow\\Bridge\\Laravel\\Testing\\LaravelWorkflowClientFake',
            $this->contract['representative_journey']['application_fake'],
        );
        self::assertSame(
            'attributed_service_class',
            $this->contract['representative_journey']['start_shape'],
        );
        self::assertSame(
            'attributed_service_class',
            $this->contract['representative_journey']['assertion_shape'],
        );
        self::assertSame([
            'workflow_type' => 'laravel.compensated-trip',
            'forward_activity_types' => ['trip.reserve-flight', 'trip.reserve-hotel', 'trip.charge'],
            'compensation_activity_types' => ['trip.cancel-hotel', 'trip.cancel-flight'],
            'authoring_surface' => 'DurableWorkflow\\Worker\\Saga',
            'context_factory' => 'DurableWorkflow\\Worker\\WorkflowContext::saga',
            'typed_failure' => 'DurableWorkflow\\Exception\\SagaCompensationFailed',
            'application_fake' => 'DurableWorkflow\\Bridge\\Laravel\\Testing\\LaravelWorkflowClientFake',
            'compensation_order' => 'reverse_registration',
            'durable_state' => 'ordinary_activity_history',
            'cross_language_activity_binding' => true,
        ], $this->contract['saga_journey']);
        self::assertTrue($this->contract['qualification']['service']['saga_cross_language_activities_required']);
        self::assertTrue($this->contract['qualification']['service']['saga_replay_required']);
        self::assertSame([
            'sources' => ['embedded_v1', 'embedded_v2'],
            'clean_application_required' => true,
            'same_injected_dependency_required' => true,
            'source_history_must_remain_owned' => true,
            'new_service_starts_required' => true,
        ], $this->contract['qualification']['established_application_transitions']);
        self::assertDoesNotMatchRegularExpression(
            '/\b2\.0\.0-(?:alpha|beta|rc)\.\d+\b/',
            json_encode($this->contract, JSON_THROW_ON_ERROR),
        );
    }

    /** @return array<string, mixed> */
    private function json(string $path): array
    {
        $decoded = json_decode(
            (string) file_get_contents(dirname(__DIR__).'/'.$path),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        self::assertIsArray($decoded);

        return $decoded;
    }
}
