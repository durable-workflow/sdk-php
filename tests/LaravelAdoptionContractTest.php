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
                ['laravel' => '9', 'php' => '8.1', 'package' => 'laravel/framework:^9.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.1', 'package' => 'laravel/framework:^10.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '9', 'php' => '8.2', 'package' => 'laravel/framework:^9.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.2', 'package' => 'laravel/framework:^10.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.2', 'package' => 'laravel/framework:^11.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.2', 'package' => 'laravel/framework:^12.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '9', 'php' => '8.3', 'package' => 'laravel/framework:^9.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.3', 'package' => 'laravel/framework:^10.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.3', 'package' => 'laravel/framework:^11.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.3', 'package' => 'laravel/framework:^12.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.3', 'package' => 'laravel/framework:^13.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '9', 'php' => '8.4', 'package' => 'laravel/framework:^9.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '10', 'php' => '8.4', 'package' => 'laravel/framework:^10.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '11', 'php' => '8.4', 'package' => 'laravel/framework:^11.0', 'bootstrap' => 'existing_supported_lock'],
                ['laravel' => '12', 'php' => '8.4', 'package' => 'laravel/framework:^12.0', 'bootstrap' => 'fresh_supported_install'],
                ['laravel' => '13', 'php' => '8.4', 'package' => 'laravel/framework:^13.0', 'bootstrap' => 'fresh_supported_install'],
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
