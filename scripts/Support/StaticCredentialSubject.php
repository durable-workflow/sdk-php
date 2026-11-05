<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Composer\InstalledVersions;
use DurableWorkflow\SdkIdentity;
use ReflectionClass;

require_once __DIR__.'/StaticCredentialFixture.php';
require_once __DIR__.'/StaticCredentialProbeFailure.php';

/** Binds caller-owned receipt bytes and installed identity; it does not solve, install or seal a graph. */
final class StaticCredentialSubject
{
    /**
     * @param array<string, mixed> $intent
     * @param array<string, mixed> $data
     */
    private function __construct(public readonly array $intent, public readonly array $data, private readonly string $vendor)
    {
    }

    public static function assertFreshProcess(): void
    {
        foreach (['Composer\\InstalledVersions', 'DurableWorkflow\\Client', 'DurableWorkflow\\SdkIdentity',
            'DurableWorkflow\\Auth\\TokenAuthentication', 'DurableWorkflow\\Auth\\StaticCredential',
            'DurableWorkflow\\Bridge\\ServiceConfiguration', 'DurableWorkflow\\Bridge\\Laravel\\ProcessCredentialResolver',
            'DurableWorkflow\\Bridge\\Laravel\\DurableWorkflowServiceProvider',
            'DurableWorkflow\\Bridge\\Symfony\\DurableWorkflowBundle', 'DurableWorkflow\\Transport\\Psr18Transport',
            'Illuminate\\Foundation\\Application', 'Symfony\\Bundle\\FrameworkBundle\\FrameworkBundle'] as $class) {
            if (class_exists($class, false)) {
                throw new StaticCredentialProbeFailure('Consumer package classes were already loaded.');
            }
        }
    }

    /** @return array<string, mixed> */
    public static function intent(string $path): array
    {
        $intent = self::json($path);
        $names = ['php', 'durable-workflow/sdk', 'laravel/framework', 'symfony/framework-bundle'];
        if (($intent['schema'] ?? null) !== 'durable-workflow.php-static-credential-intent/v1'
            || !is_array($intent['requirements'] ?? null) || count($intent['requirements']) !== count($names)
            || array_diff(array_keys($intent['requirements']), $names) !== []
            || ($intent['fixture']['sha256'] ?? null) !== StaticCredentialFixture::SHA256
            || ($intent['dependency_policy'] ?? null) !== ['plugins' => false, 'scripts' => false, 'advisory_policy' => 'normal']) {
            throw new StaticCredentialProbeFailure('The dependency intent differs.');
        }
        foreach ($intent['requirements'] as $version) {
            if (!is_string($version) || preg_match('/\A\d+\.\d+\.\d+\z/', $version) !== 1) {
                throw new StaticCredentialProbeFailure('Dependency intent requires exact stable versions.');
            }
        }
        foreach (['laravel/framework', 'symfony/framework-bundle'] as $name) {
            self::digest($intent['framework_source_references'][$name] ?? null, 40);
        }
        return $intent;
    }

    public static function read(string $autoload, string $intentPath, string $subjectPath, string $subjectSha256): self
    {
        self::digest($subjectSha256, 64);
        $raw = self::bytes($subjectPath, $subjectSha256, 65536);
        $data = self::object($raw);
        if (json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR)."\n" !== $raw) {
            throw new StaticCredentialProbeFailure('The caller-owned subject must use canonical JSON.');
        }
        $fields = ['schema', 'runner_reference', 'sdk_reference', 'lock_sha256', 'archive_inventory_file',
            'archive_inventory_sha256', 'sdk_archive_file', 'sdk_archive_sha256', 'sdk_archive_bytes'];
        if (($data['schema'] ?? null) !== 'durable-workflow.php-static-credential-subject/v1'
            || array_diff(array_keys($data), $fields) !== []) {
            throw new StaticCredentialProbeFailure('The subject envelope differs.');
        }
        foreach (['runner_reference', 'sdk_reference'] as $name) {
            self::digest($data[$name] ?? null, 40);
        }
        foreach (['lock_sha256', 'archive_inventory_sha256', 'sdk_archive_sha256'] as $name) {
            self::digest($data[$name] ?? null, 64);
        }
        if (!is_int($data['sdk_archive_bytes'] ?? null) || $data['sdk_archive_bytes'] < 1 || $data['sdk_archive_bytes'] > 134217728) {
            throw new StaticCredentialProbeFailure('The SDK archive size is unclosed.');
        }
        $intent = self::intent($intentPath);
        $vendor = realpath(dirname($autoload));
        if ($vendor === false || basename($vendor) !== 'vendor' || realpath($autoload) !== $vendor.'/autoload.php') {
            throw new StaticCredentialProbeFailure('The consumer autoloader boundary differs.');
        }
        $lock = self::object(self::bytes(dirname($vendor).'/composer.lock', $data['lock_sha256'], 4194304));
        self::receiptFiles($subjectPath, $data);
        self::project(dirname($vendor), $intent, $data, $lock);
        return new self($intent, $data, $vendor);
    }

    /** @return array<string, mixed> */
    public function installed(): array
    {
        $metadata = (new ReflectionClass(InstalledVersions::class))->getFileName();
        if (!is_string($metadata) || realpath($metadata) !== $this->vendor.'/composer/InstalledVersions.php') {
            throw new StaticCredentialProbeFailure('Installed metadata came from another consumer.');
        }
        $packages = [];
        foreach (['durable-workflow/sdk', 'laravel/framework', 'symfony/framework-bundle'] as $name) {
            $expected = $this->intent['requirements'][$name];
            $actual = InstalledVersions::getPrettyVersion($name);
            $reference = InstalledVersions::getReference($name);
            $selected = $name === 'durable-workflow/sdk' ? $this->data['sdk_reference']
                : $this->intent['framework_source_references'][$name];
            $path = InstalledVersions::getInstallPath($name);
            if (!in_array($actual, [$expected, 'v'.$expected], true) || $reference !== $selected
                || !is_string($path) || realpath($path) !== $this->vendor.'/'.$name) {
                throw new StaticCredentialProbeFailure('The installed package identity differs.');
            }
            $packages[$name] = ['version' => $actual, 'reference' => $reference];
        }
        $paths = $this->implementations();
        $manifest = self::json($this->vendor.'/durable-workflow/sdk/composer.json');
        if (($manifest['extra']['durable-workflow']['product-train'] ?? null) !== $this->intent['requirements']['durable-workflow/sdk']
            || SdkIdentity::version() !== $this->intent['requirements']['durable-workflow/sdk']) {
            throw new StaticCredentialProbeFailure('Installed release metadata differs.');
        }
        return ['packages' => $packages, 'implementation_paths' => $paths, 'caller_runner_reference' => $this->data['runner_reference'],
            'lock_sha256' => $this->data['lock_sha256'], 'caller_archive_inventory_sha256' => $this->data['archive_inventory_sha256']];
    }

    /** @return array<string, string> */
    private function implementations(): array
    {
        $paths = [];
        $classes = ['DurableWorkflow\\Client', 'DurableWorkflow\\SdkIdentity', 'DurableWorkflow\\Auth\\TokenAuthentication',
            'DurableWorkflow\\Auth\\StaticCredential', 'DurableWorkflow\\Bridge\\ServiceConfiguration',
            'DurableWorkflow\\Bridge\\Laravel\\ProcessCredentialResolver',
            'DurableWorkflow\\Bridge\\Laravel\\DurableWorkflowServiceProvider',
            'DurableWorkflow\\Bridge\\Symfony\\DurableWorkflowBundle', 'DurableWorkflow\\Transport\\Psr18Transport'];
        foreach ($classes as $class) {
            $file = (new ReflectionClass($class))->getFileName();
            $actual = is_string($file) ? realpath($file) : false;
            if ($actual === false || !str_starts_with($actual, $this->vendor.'/durable-workflow/sdk/src/')) {
                throw new StaticCredentialProbeFailure('A credential implementation came from outside the installed SDK.');
            }
            $paths[$class] = substr($actual, strlen($this->vendor) + 1);
        }
        $frameworks = ['Illuminate\\Foundation\\Application' => 'laravel/framework',
            'Symfony\\Bundle\\FrameworkBundle\\FrameworkBundle' => 'symfony/framework-bundle'];
        foreach ($frameworks as $class => $package) {
            if (!class_exists($class)) {
                throw new StaticCredentialProbeFailure('A required framework implementation is absent.');
            }
            $file = (new ReflectionClass($class))->getFileName();
            $actual = is_string($file) ? realpath($file) : false;
            if ($actual === false || !str_starts_with($actual, $this->vendor.'/'.$package.'/')) {
                throw new StaticCredentialProbeFailure('A framework implementation came from another package.');
            }
            $paths[$class] = substr($actual, strlen($this->vendor) + 1);
        }
        return $paths;
    }

    /**
     * @param array<string, mixed> $intent
     * @param array<string, mixed> $subject
     * @param array<string, mixed> $lock
     */
    private static function project(string $root, array $intent, array $subject, array $lock): void
    {
        $project = self::json($root.'/composer.json');
        $actual = $project['require'] ?? [];
        $expected = $intent['requirements'];
        if (!is_array($actual)) {
            throw new StaticCredentialProbeFailure('Consumer requirements must be an object.');
        }
        ksort($actual);
        ksort($expected);
        if ($actual !== $expected || ($project['repositories'] ?? []) !== []
            || ($project['scripts'] ?? []) !== [] || ($project['config']['allow-plugins'] ?? null) !== false
            || isset($project['config']['audit']['ignore']) || ($project['config']['audit']['block-insecure'] ?? true) !== true) {
            throw new StaticCredentialProbeFailure('Consumer intent or normal advisory policy differs.');
        }
        foreach ($lock['packages'] ?? [] as $package) {
            if (($package['name'] ?? null) === 'durable-workflow/sdk'
                && ($package['version'] ?? null) === $intent['requirements']['durable-workflow/sdk']
                && ($package['dist']['type'] ?? null) === 'zip'
                && ($package['dist']['reference'] ?? null) === $subject['sdk_reference']
                && ($package['dist']['url'] ?? null) === 'https://api.github.com/repos/durable-workflow/sdk-php/zipball/'.$subject['sdk_reference']) {
                return;
            }
        }
        throw new StaticCredentialProbeFailure('No normal registry SDK distribution is bound in the lock.');
    }

    /** @param array<string, mixed> $data */
    private static function receiptFiles(string $subjectPath, array $data): void
    {
        $root = realpath(dirname($subjectPath));
        foreach (['archive_inventory', 'sdk_archive'] as $name) {
            $leaf = $data[$name.'_file'] ?? null;
            if (!is_string($leaf) || preg_match('/\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z/', $leaf) !== 1) {
                throw new StaticCredentialProbeFailure('Receipt names must be packet-local files.');
            }
            $path = realpath(dirname($subjectPath).'/'.$leaf);
            if ($root === false || $path === false || dirname($path) !== $root) {
                throw new StaticCredentialProbeFailure('Receipt file escaped the packet.');
            }
            self::fileHash($path, $data[$name.'_sha256'], 134217728);
            if ($name === 'sdk_archive' && filesize($path) !== $data['sdk_archive_bytes']) {
                throw new StaticCredentialProbeFailure('The SDK archive size differs.');
            }
        }
    }

    private static function digest(mixed $value, int $length): void
    {
        if (!is_string($value) || preg_match('/\A[a-f0-9]{'.$length.'}\z/', $value) !== 1) {
            throw new StaticCredentialProbeFailure('A future immutable reference is missing.');
        }
    }

    private static function fileHash(string $path, string $sha256, int $maximum): void
    {
        if (!is_file($path) || !is_readable($path) || filesize($path) > $maximum || hash_file('sha256', $path) !== $sha256) {
            throw new StaticCredentialProbeFailure('A caller-owned receipt file differs.');
        }
    }

    private static function bytes(string $path, string $sha256, int $maximum): string
    {
        if (!is_file($path) || !is_readable($path) || filesize($path) > $maximum) {
            throw new StaticCredentialProbeFailure('A caller-owned input is missing or too large.');
        }
        $bytes = (string) file_get_contents($path);
        if (strlen($bytes) > $maximum || hash('sha256', $bytes) !== $sha256) {
            throw new StaticCredentialProbeFailure('The consumed input bytes differ.');
        }
        return $bytes;
    }

    /** @return array<string, mixed> */
    private static function json(string $path): array
    {
        if (!is_file($path) || !is_readable($path)) {
            throw new StaticCredentialProbeFailure('A caller-owned input is missing.');
        }
        return self::object((string) file_get_contents($path));
    }

    /** @return array<string, mixed> */
    private static function object(string $bytes): array
    {
        $value = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($value) || array_is_list($value)) {
            throw new StaticCredentialProbeFailure('A caller-owned object is required.');
        }
        return $value;
    }
}
