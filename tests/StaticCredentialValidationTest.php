<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Auth\TokenAuthentication;
use DurableWorkflow\Bridge\Laravel\ProcessCredentialResolver;
use DurableWorkflow\Client;
use DurableWorkflow\Qualification\StaticCredentialFixture;
use DurableWorkflow\Qualification\StaticCredentialProbe;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

require_once dirname(__DIR__).'/scripts/Support/StaticCredentialFixture.php';
require_once dirname(__DIR__).'/scripts/Support/StaticCredentialProbe.php';

final class StaticCredentialValidationTest extends TestCase
{
    private const TARGETS = ['Client', 'TokenAuthentication', 'ServiceConfiguration', 'ProcessCredentialResolver'];
    private const OPTIONS = ['shared' => 'token', 'control' => 'controlToken', 'worker' => 'workerToken'];
    private const CONFIGURATION = ['shared' => 'token', 'control' => 'control_token', 'worker' => 'worker_token'];
    private const ENVIRONMENT = ['shared' => 'DURABLE_WORKFLOW_TOKEN', 'control' => 'DURABLE_WORKFLOW_CLIENT_TOKEN',
        'worker' => 'DURABLE_WORKFLOW_WORKER_TOKEN'];

    public function testCanonicalFixtureIdentityIsFrozen(): void
    {
        self::assertSame('0d4f01b763d72850943acbd9a74a0cc5da1ca6db5bc1b6fa5ca37fda11810acb',
            hash_file('sha256', self::fixturePath()));
        self::assertSame(60716, filesize(self::fixturePath()));
        $fixture = self::fixture();
        $ids = [$fixture['binary_key_boundary']['id']];
        foreach (['common_scalar_cases', 'php_byte_loader_cases', 'pre_selection_cases',
            'php_adapter_cases', 'server_pre_selection_cases'] as $group) {
            array_push($ids, ...array_column($fixture[$group], 'id'));
        }
        self::assertCount(319, $ids);
        self::assertCount(319, array_unique($ids));
    }

    #[DataProvider('scalarCases')]
    public function testCanonicalOriginalTextIsValidatedBeforeSelection(string $id, string $target, string $role): void
    {
        $case = self::case($id);
        $value = self::value($id);
        $ordinary = self::value('scalar-ordinary');
        $plane = $role === 'worker' ? 'worker' : 'control';
        $options = match ($role) {
            'shared' => ['shared' => $value],
            'control' => ['control' => $value, 'worker' => $ordinary],
            'worker' => ['worker' => $value, 'control' => $ordinary],
            default => throw new LogicException('The canonical credential role must be supported.'),
        };
        if ($value === '') {
            $options = $role === 'shared'
                ? ['shared' => '', 'control' => $ordinary, 'worker' => $ordinary]
                : ['shared' => $ordinary, $role => ''];
        }
        $transport = new FakeTransport([]);
        $error = null;
        $headers = [];
        try {
            $headers = self::exercise($target, $options, $plane, $transport);
        } catch (InvalidArgumentException $exception) {
            $error = $exception;
        }
        self::assertCount(0, $transport->requests);
        if ($case['expected'] !== 'preserve') {
            self::assertInstanceOf(InvalidArgumentException::class, $error);
            self::assertSame(self::diagnostic($target, $role), $error->getMessage());

            return;
        }
        self::assertNull($error);
        $expected = $value === '' ? $ordinary : $value;
        self::assertSame('Bearer '.$expected, $headers['Authorization'] ?? null,
            'Selected synthetic credential bytes must remain exact.');
        self::assertSame($case['expected_utf8_hex'], bin2hex($value));
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function scalarCases(): iterable
    {
        foreach (['common_scalar_cases', 'php_byte_loader_cases'] as $group) {
            foreach (self::fixture()[$group] as $case) {
                foreach (self::TARGETS as $target) {
                    if ($target === 'ProcessCredentialResolver' && str_contains(self::value($case['id']), "\0")) {
                        continue; // OS environment values cannot represent NUL; direct strings cover it.
                    }
                    foreach (array_keys(self::OPTIONS) as $role) {
                        yield $case['id'].' / '.$target.' / '.$role => [$case['id'], $target, $role];
                    }
                }
            }
        }
    }

    #[DataProvider('adapterCases')]
    public function testCanonicalUnusedRolesHandoffsAndFallbacks(string $id, string $target): void
    {
        $case = self::adapterCase($id);
        $options = [];
        foreach ($case['options_case_refs'] ?? [] as $role => $reference) {
            $options[$role] = self::value($reference);
        }
        foreach ($case['options_code_points'] ?? [] as $role => $points) {
            $options[$role] = self::fromPoints($points);
        }
        foreach ($case['null_options'] ?? [] as $role) {
            $options[$role] = null;
        }
        foreach ($case['empty_options'] ?? [] as $role) {
            $options[$role] = '';
        }
        $process = [];
        if (isset($case['process_token_case_ref'])) {
            $process = ['DURABLE_WORKFLOW_PROCESS_ROLE' => $case['process_role'],
                'DURABLE_WORKFLOW_PROCESS_TOKEN' => self::value($case['process_token_case_ref'])];
        }
        $plane = $case['selected_plane'] ?? ($case['process_role'] === 'worker' ? 'worker' : 'control');
        $transport = new FakeTransport([]);
        $error = null;
        $headers = [];
        try {
            $headers = self::exercise($target, $options, $plane, $transport, $process);
        } catch (InvalidArgumentException $exception) {
            $error = $exception;
        }
        self::assertCount(0, $transport->requests);
        if (in_array($case['expected'], ['reject', 'reject-credential'], true)) {
            self::assertInstanceOf(InvalidArgumentException::class, $error);
            $setting = $case['offending_setting'] ?? self::setting($target, $case['offending_option']);
            self::assertSame('Invalid static credential setting '.$setting.'.', $error->getMessage());
        } elseif ($case['expected'] === 'existing-role-refusal-before-request') {
            self::assertInstanceOf(InvalidArgumentException::class, $error);
            self::assertStringContainsString('A '.$plane.' credential is required', $error->getMessage());
        } else {
            self::assertNull($error);
            self::assertSame('Bearer '.self::value($case['expected_case_ref']), $headers['Authorization'] ?? null,
                'Selected synthetic credential bytes must remain exact.');
        }
    }

    /** @return iterable<string, array{string, string}> */
    public static function adapterCases(): iterable
    {
        foreach (self::fixture()['pre_selection_cases'] as $case) {
            foreach (['Client', 'TokenAuthentication'] as $target) {
                yield $case['id'].' / '.$target => [$case['id'], $target];
            }
        }
        foreach (self::fixture()['php_adapter_cases'] as $case) {
            foreach ($case['targets'] ?? [$case['target']] as $target) {
                yield $case['id'].' / '.$target => [$case['id'], $target];
            }
        }
    }

    public function testAnonymousAndRequiredTokenSemanticsRemainDistinct(): void
    {
        $transport = new FakeTransport([]);
        self::assertSame([], self::exercise('Client', [], 'control', $transport));
        self::assertSame([], self::exercise('ServiceConfiguration', [], 'control', $transport));
        foreach ([[], ['token' => '', 'controlToken' => '', 'workerToken' => '']] as $options) {
            try {
                new TokenAuthentication(...$options);
                self::fail('TokenAuthentication still requires a nonempty token.');
            } catch (InvalidArgumentException $exception) {
                self::assertSame('At least one non-empty authentication token is required.', $exception->getMessage());
            }
        }
        self::assertCount(0, $transport->requests);
    }

    public function testDynamicAuthenticationRemainsOutsideTheStaticTextContract(): void
    {
        $dynamic = new class implements Authentication {
            public function headers(bool $workerRequest): array
            {
                return ['Authorization' => "dynamic\0bytes"];
            }
        };
        $transport = new FakeTransport([]);
        $client = new Client('https://server.example', authentication: $dynamic, transport: $transport);

        self::assertSame($dynamic, (new ReflectionProperty(Client::class, 'authentication'))->getValue($client));
        self::assertCount(0, $transport->requests);
    }

    #[DataProvider('processRoleCases')]
    public function testWorkerCredentialRoleAlsoValidatesUnusedAndHandoffText(string $id): void
    {
        $case = self::adapterCase($id);
        $environment = [];
        foreach ($case['options_case_refs'] ?? [] as $role => $reference) {
            $environment[self::ENVIRONMENT[$role]] = self::value($reference);
        }
        if (isset($case['process_token_case_ref'])) {
            $environment = ['DURABLE_WORKFLOW_PROCESS_ROLE' => $case['process_role'],
                'DURABLE_WORKFLOW_PROCESS_TOKEN' => self::value($case['process_token_case_ref'])];
        }
        try {
            self::withEnvironment($environment, static fn (): string => ProcessCredentialResolver::workerCredentialRole());
            self::fail('The role-only loader must validate original static text.');
        } catch (InvalidArgumentException $exception) {
            $setting = $case['offending_setting'] ?? self::ENVIRONMENT[$case['offending_option']];
            self::assertSame('Invalid static credential setting '.$setting.'.', $exception->getMessage());
        }
    }

    /** @return iterable<string, array{string}> */
    public static function processRoleCases(): iterable
    {
        foreach (['php-valid-scoped-mode-unused-worker-scalar', 'php-valid-scoped-mode-unused-control-invalid-utf8',
            'php-process-handoff-worker-scalar', 'php-process-handoff-worker-invalid-utf8'] as $id) {
            yield $id => [$id];
        }
    }

    /**
     * @param array<string, ?string> $options
     * @param array<string, string> $process
     * @return array<string, string>
     */
    private static function exercise(string $target, array $options, string $plane, FakeTransport $transport, array $process = []): array
    {
        return StaticCredentialProbe::headers($target, $options, $plane, $transport, $process);
    }

    /**
     * @template T
     * @param array<string, string> $process
     * @param callable(): T $build
     * @return T
     */
    private static function withEnvironment(array $process, callable $build): mixed
    {
        $names = [...array_values(self::ENVIRONMENT), 'DURABLE_WORKFLOW_PROCESS_ROLE', 'DURABLE_WORKFLOW_PROCESS_TOKEN'];
        $previous = [];
        foreach ($names as $name) {
            $previous[$name] = getenv($name);
        }
        try {
            foreach ($names as $name) {
                self::assertTrue(putenv($name));
            }
            foreach ($process as $name => $value) {
                self::assertTrue(putenv($name.'='.$value));
                self::assertTrue(getenv($name) === $value, 'Synthetic environment materialization must be exact.');
            }
            return $build();
        } finally {
            foreach ($previous as $name => $value) {
                self::assertTrue(putenv($value === false ? $name : $name.'='.$value));
            }
        }
    }

    private static function diagnostic(string $target, string $role): string
    {
        return 'Invalid static credential setting '.self::setting($target, $role).'.';
    }

    private static function setting(string $target, string $role): string
    {
        return match ($target) {
            'Client', 'TokenAuthentication' => self::OPTIONS[$role],
            'ServiceConfiguration' => 'credentials.'.self::CONFIGURATION[$role],
            'ProcessCredentialResolver' => self::ENVIRONMENT[$role],
            default => throw new LogicException('The canonical credential target must be supported.'),
        };
    }

    private static function canonical(): StaticCredentialFixture
    {
        static $fixture = null;
        return $fixture ??= new StaticCredentialFixture(self::fixturePath());
    }

    /** @return array<string, mixed> */
    private static function fixture(): array
    {
        return self::canonical()->data;
    }

    private static function fixturePath(): string
    {
        return __DIR__.'/fixtures/auth-configuration/static-credentials-v1.json';
    }

    /** @return array<string, mixed> */
    private static function case(string $id): array
    {
        return self::canonical()->scalar($id);
    }

    /** @return array<string, mixed> */
    private static function adapterCase(string $id): array
    {
        return self::canonical()->adapter($id);
    }

    private static function value(string $id): string
    {
        return self::canonical()->value($id);
    }

    /** @param list<int> $points */
    private static function fromPoints(array $points): string
    {
        return StaticCredentialFixture::points($points);
    }
}
