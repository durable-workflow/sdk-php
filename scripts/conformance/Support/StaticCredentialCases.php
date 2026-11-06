<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

require_once __DIR__.'/StaticCredentialFixture.php';
require_once __DIR__.'/StaticCredentialFrameworkProbe.php';

/** Canonical expectations shared by the source controls and future consumer runner. */
final class StaticCredentialCases
{
    private const ROLES = ['shared', 'control', 'worker'];
    private const CORE = ['Client', 'TokenAuthentication', 'ServiceConfiguration',
        'ServiceConfigurationConstructor', 'ServiceConfigurationAlias', 'ProcessCredentialResolver'];

    public function __construct(private readonly StaticCredentialFixture $fixture)
    {
    }

    /** @return iterable<array<string, mixed>> */
    public function core(): iterable
    {
        foreach ($this->scalarIds() as $id) {
            foreach (self::CORE as $target) {
                $roles = $target === 'ServiceConfigurationAlias' ? ['shared', 'control'] : self::ROLES;
                foreach ($roles as $role) {
                    if (!$this->rejectsNul($target, $id)) {
                        yield $this->scalar($id, $target, $role);
                    }
                }
            }
        }
        yield from $this->coreAdapters();
        yield from $this->controls();
    }

    /** @return iterable<array<string, mixed>> */
    public function frameworks(): iterable
    {
        foreach (StaticCredentialFrameworkProbe::targets() as $target => $metadata) {
            foreach ($this->scalarIds() as $id) {
                foreach (self::ROLES as $role) {
                    if (!$this->rejectsNul($target, $id)) {
                        yield $this->scalar($id, $target, $role, $metadata['plane']);
                    }
                }
            }
            yield from $this->frameworkAdapters($target, $metadata);
            yield from $this->frameworkControls($target, $metadata['plane']);
        }
    }

    /** @return list<array<string, string>> */
    public function exclusions(): array
    {
        $targets = ['ProcessCredentialResolver'];
        foreach (StaticCredentialFrameworkProbe::targets() as $target => $metadata) {
            if ($metadata['framework'] === 'laravel') {
                $targets[] = $target;
            }
        }
        $excluded = [];
        foreach ($this->scalarIds() as $id) {
            foreach ($targets as $target) {
                if ($this->rejectsNul($target, $id)) {
                    $excluded[] = ['id' => $id, 'target' => $target, 'reason' => 'os_environment_cannot_represent_nul'];
                }
            }
        }
        return $excluded;
    }

    /** @return iterable<string> */
    private function scalarIds(): iterable
    {
        foreach (['common_scalar_cases', 'php_byte_loader_cases'] as $group) {
            foreach ($this->fixture->data[$group] as $case) {
                yield $case['id'];
            }
        }
    }

    /** @return array<string, mixed> */
    private function scalar(string $id, string $target, string $role, ?string $plane = null): array
    {
        $value = $this->fixture->value($id);
        $ordinary = $this->fixture->value('scalar-ordinary');
        $plane ??= $role === 'worker' ? 'worker' : 'control';
        $options = match ($role) {
            'shared' => ['shared' => $value],
            'control' => ['control' => $value, 'worker' => $ordinary],
            'worker' => ['worker' => $value, 'control' => $ordinary],
            default => throw new StaticCredentialProbeFailure('Unsupported scalar credential role.'),
        };
        if ($value === '') {
            $options = $role === 'shared' ? ['shared' => '', 'control' => $ordinary, 'worker' => $ordinary]
                : ['shared' => $ordinary, $role => ''];
        }
        $selected = ($role === 'shared' || $role === $plane) && $value !== '' ? $value : $ordinary;
        $output = ['Authorization' => 'Bearer '.$selected];
        if ($target === 'ServiceConfigurationConstructor') {
            // Raw constructor settings preserve empty strings; unlike fromArray they do not normalize them.
            $options = [$role => $value];
            $output = ['token' => $options['shared'] ?? null, 'controlToken' => $options['control'] ?? null,
                'workerToken' => $options['worker'] ?? null];
        }
        return ['id' => $id, 'target' => $target, 'role' => $role, 'plane' => $plane, 'options' => $options,
            'process' => [], 'expected' => match ($this->fixture->scalar($id)['expected']) {
                'preserve' => 'preserve', 'reject', 'reject-invalid-utf8' => 'reject',
                default => throw new StaticCredentialProbeFailure('Unsupported scalar expectation.'),
            },
            'output' => $output, 'setting' => $this->setting($target, $role)];
    }

    /** @return iterable<array{array<string, mixed>, string, string}> */
    private function adapters(): iterable
    {
        foreach (['pre_selection_cases', 'php_adapter_cases'] as $group) {
            foreach ($this->fixture->data[$group] as $case) {
                $targets = $group === 'pre_selection_cases' ? ['Client', 'TokenAuthentication']
                    : ($case['targets'] ?? [$case['target']]);
                foreach ($targets as $target) {
                    yield [$case, $target, $group];
                }
            }
        }
    }

    /** @return iterable<array<string, mixed>> */
    private function coreAdapters(): iterable
    {
        $seen = [];
        foreach ($this->adapters() as [$case, $target]) {
            foreach ($this->adapterTargets($target, $case) as $adapted) {
                $key = $case['id'].'/'.$adapted;
                if (!isset($seen[$key])) {
                    $seen[$key] = true;
                    yield $this->adapter($case, $adapted);
                }
            }
        }
    }

    /**
     * @param array<string, mixed> $case
     * @return list<string>
     */
    private function adapterTargets(string $target, array $case): array
    {
        // The frozen shared-plus-scoped pre-selection rows belong only to their original direct targets.
        if ($target !== 'ServiceConfiguration') {
            return [$target];
        }
        return ($case['selected_plane'] ?? null) === 'control'
            ? [$target, 'ServiceConfigurationConstructor', 'ServiceConfigurationAlias']
            : [$target, 'ServiceConfigurationConstructor'];
    }

    /**
     * @param array<string, string|bool> $metadata
     * @return iterable<array<string, mixed>>
     */
    private function frameworkAdapters(string $target, array $metadata): iterable
    {
        $seen = [];
        foreach ($this->adapters() as [$case, $original, $family]) {
            $plane = $case['selected_plane'] ?? ($case['process_role'] === 'worker' ? 'worker' : 'control');
            $applies = $family === 'php_adapter_cases'
                && $original === ($metadata['framework'] === 'laravel' ? 'ProcessCredentialResolver' : 'ServiceConfiguration');
            if ($plane === $metadata['plane'] && $applies && !isset($seen[$case['id']])) {
                $seen[$case['id']] = true;
                yield $this->adapter($case, $target);
            }
        }
    }

    /**
     * @param array<string, mixed> $case
     * @return array<string, mixed>
     */
    private function adapter(array $case, string $target): array
    {
        $options = $this->adapterOptions($case);
        $process = isset($case['process_token_case_ref']) ? ['DURABLE_WORKFLOW_PROCESS_ROLE' => $case['process_role'],
            'DURABLE_WORKFLOW_PROCESS_TOKEN' => $this->fixture->value($case['process_token_case_ref'])] : [];
        $plane = $case['selected_plane'] ?? ($case['process_role'] === 'worker' ? 'worker' : 'control');
        $expected = match ($case['expected']) {
            'reject', 'reject-credential' => 'reject',
            'existing-role-refusal-before-request' => 'role-refusal',
            'preserve-selected-text' => 'preserve',
            default => throw new StaticCredentialProbeFailure('Unsupported adapter expectation.'),
        };
        $setting = $expected === 'reject' ? ($case['offending_setting'] ?? $this->setting($target, $case['offending_option'])) : null;
        return ['id' => $case['id'], 'target' => $target, 'role' => 'adapter', 'plane' => $plane,
            'options' => $options, 'process' => $process, 'expected' => $expected, 'setting' => $setting,
            'output' => $expected === 'preserve' ? ['Authorization' => 'Bearer '.$this->fixture->value($case['expected_case_ref'])] : []];
    }

    /**
     * @param array<string, mixed> $case
     * @return array<string, ?string>
     */
    private function adapterOptions(array $case): array
    {
        $options = [];
        foreach ($case['options_case_refs'] ?? [] as $role => $reference) {
            $options[$role] = $this->fixture->value($reference);
        }
        foreach ($case['options_code_points'] ?? [] as $role => $points) {
            $options[$role] = StaticCredentialFixture::points($points);
        }
        foreach ($case['null_options'] ?? [] as $role) {
            $options[$role] = null;
        }
        foreach ($case['empty_options'] ?? [] as $role) {
            $options[$role] = '';
        }
        return $options;
    }

    private function rejectsNul(string $target, string $id): bool
    {
        $environment = $target === 'ProcessCredentialResolver'
            || (StaticCredentialFrameworkProbe::targets()[$target]['framework'] ?? null) === 'laravel';
        return $environment && str_contains($this->fixture->value($id), "\0");
    }

    private function setting(string $target, string $role): string
    {
        if (in_array($target, ['Client', 'TokenAuthentication'], true)) {
            return ['shared' => 'token', 'control' => 'controlToken', 'worker' => 'workerToken'][$role];
        }
        if ($target === 'ProcessCredentialResolver' || $target === 'ProcessRole'
            || (StaticCredentialFrameworkProbe::targets()[$target]['framework'] ?? null) === 'laravel') {
            return StaticCredentialProbe::ENVIRONMENT[$role];
        }
        return 'credentials.'.['shared' => 'token', 'control' => 'control_token', 'worker' => 'worker_token'][$role];
    }

    /** @return iterable<array<string, mixed>> */
    private function controls(): iterable
    {
        foreach (['Client', 'ServiceConfiguration', 'ServiceConfigurationAlias', 'ProcessCredentialResolver'] as $target) {
            yield ['id' => 'control-anonymous', 'target' => $target, 'role' => 'control', 'plane' => 'control',
                'options' => [], 'process' => [], 'expected' => 'preserve', 'output' => [], 'setting' => null];
        }
        yield ['id' => 'control-required-token', 'target' => 'TokenAuthentication', 'role' => 'control', 'plane' => 'control',
            'options' => [], 'process' => [], 'expected' => 'required-token', 'output' => [], 'setting' => null];
        yield ['id' => 'control-absent-constructor', 'target' => 'ServiceConfigurationConstructor', 'role' => 'control', 'plane' => 'control',
            'options' => [], 'process' => [], 'expected' => 'preserve',
            'output' => ['token' => null, 'controlToken' => null, 'workerToken' => null], 'setting' => null];
        foreach (['', $this->fixture->value('scalar-ordinary')] as $shared) {
            yield ['id' => $shared === '' ? 'control-raw-empty-mode' : 'control-raw-mixed-mode',
                'target' => 'ServiceConfigurationConstructor', 'role' => 'control', 'plane' => 'control',
                'options' => ['shared' => $shared, 'control' => $this->fixture->value('scalar-ordinary')],
                'process' => [], 'expected' => 'mode-refusal', 'output' => [], 'setting' => null];
        }
        foreach (['php-valid-scoped-mode-unused-worker-scalar', 'php-valid-scoped-mode-unused-control-invalid-utf8',
            'php-process-handoff-worker-scalar', 'php-process-handoff-worker-invalid-utf8'] as $id) {
            yield $this->adapter($this->fixture->adapter($id), 'ProcessRole');
        }
        foreach (['shared', 'worker'] as $mode) {
            yield ['id' => 'control-process-mode-'.$mode, 'target' => 'ProcessRole', 'role' => $mode, 'plane' => 'worker',
                'options' => [$mode => $this->fixture->value('scalar-ordinary')], 'process' => [], 'expected' => 'preserve',
                'output' => ['credential_role' => $mode], 'setting' => null];
        }
        foreach ([['ServiceConfiguration', 'control'], ['ServiceConfiguration', 'worker'],
            ['ServiceConfigurationAlias', 'control'], ['ProcessCredentialResolver', 'control'],
            ['ProcessCredentialResolver', 'worker']] as [$target, $plane]) {
            yield from $this->frameworkControls($target, $plane, false);
        }
    }

    /** @return iterable<array<string, mixed>> */
    private function frameworkControls(string $target, string $plane, bool $anonymous = true): iterable
    {
        if ($anonymous) {
            yield ['id' => 'control-anonymous', 'target' => $target, 'role' => 'none', 'plane' => $plane,
                'options' => [], 'process' => [], 'expected' => 'preserve', 'output' => [], 'setting' => null];
        }
        $opposite = $plane === 'worker' ? 'control' : 'worker';
        $ordinary = $this->fixture->value('scalar-ordinary');
        yield ['id' => 'control-opposite-only', 'target' => $target, 'role' => $opposite, 'plane' => $plane,
            'options' => [$opposite => $ordinary], 'process' => [], 'expected' => 'role-refusal', 'output' => [], 'setting' => null];
        yield ['id' => 'control-existing-mode', 'target' => $target, 'role' => $plane, 'plane' => $plane,
            'options' => ['shared' => $ordinary, $plane => $ordinary], 'process' => [],
            'expected' => 'mode-refusal', 'output' => [], 'setting' => null];
    }
}
