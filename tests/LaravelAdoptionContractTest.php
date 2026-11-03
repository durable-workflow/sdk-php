<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

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
                ['laravel' => '10', 'php' => '8.1', 'package' => 'laravel/framework:^10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.2', 'package' => 'laravel/framework:^10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.2', 'package' => 'laravel/framework:^11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.2', 'package' => 'laravel/framework:^12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '10', 'php' => '8.3', 'package' => 'laravel/framework:^10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.3', 'package' => 'laravel/framework:^11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.3', 'package' => 'laravel/framework:^12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.3', 'package' => 'laravel/framework:^13.30.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '10', 'php' => '8.4', 'package' => 'laravel/framework:^10.50.4', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.4', 'package' => 'laravel/framework:^11.56.1', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.4', 'package' => 'laravel/framework:^12.69.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.4', 'package' => 'laravel/framework:^13.30.0', 'bootstrap' => 'fresh_supported_install'],
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

    public function testEveryQualificationCellNamesItsMajorsPublishedFloor(): void
    {
        $floors = [];
        foreach (explode('|', $this->contract['framework']['constraint']) as $range) {
            self::assertMatchesRegularExpression('/\A\^([1-9][0-9]*)\.[0-9]+\.[0-9]+\z/', $range);
            $floors[explode('.', substr($range, 1))[0]] = $range;
        }
        $majors = [];
        foreach ($this->contract['framework']['qualification_matrix'] as $cell) {
            self::assertSame('laravel/framework:'.$floors[$cell['laravel']], $cell['package']);
            $majors[$cell['laravel']] = true;
        }
        self::assertSame(array_keys($floors), array_keys($majors));
    }

    public function testCiQualifiesExactlyThePublishedLaravelCells(): void
    {
        $workflow = (string) file_get_contents(dirname(__DIR__).'/.github/workflows/ci.yml');
        preg_match_all(
            "/^ {10}- framework: Laravel\n {12}version: '([0-9]+)'\n {12}php: '([0-9.]+)'\n"
            ."^ {12}package: '([^']+)'\n {12}bootstrap: ([a-z_]+)\n/m",
            $workflow,
            $matches,
            PREG_SET_ORDER,
        );
        self::assertSame(
            $this->contract['framework']['qualification_matrix'],
            array_map(static fn (array $match): array => [
                'laravel' => $match[1], 'php' => $match[2], 'package' => $match[3], 'bootstrap' => $match[4],
            ], $matches),
        );
        self::assertSame(count($matches), substr_count($workflow, "- framework: Laravel\n"));
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
