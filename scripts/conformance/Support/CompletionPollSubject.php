<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Composer\InstalledVersions;
use ReflectionClass;
use RuntimeException;

require_once __DIR__.'/CompletionPollLoadProfile.php';

/** Caller-selected identity, not a package installer or runtime admission authority. */
final class CompletionPollSubject
{
    public const SCHEMA = 'durable-workflow.php-completion-polls-subject/v1';
    public const SCENARIOS = ['carriers', 'pipeline', 'pipeline-loss', 'fallback', 'capacity',
        'carrier-loss-activity', 'carrier-loss-workflow', 'carrier-loss-workflow-completion', 'completion-transit',
        'carrier-expiry', 'carrier-refence', 'pipeline-buffer-age', 'prefetch-loss', 'default-off', 'explicit-off', 'packet-defaults', 'opposite-slots', 'workflow-refence',
        ...CompletionPollLoadProfile::SCENARIOS];

    /** @param array<string, mixed> $data */
    private function __construct(public readonly array $data, public readonly string $vendor, public readonly string $sha256)
    {
    }

    public static function freshProcess(): void
    {
        foreach (['Composer\\InstalledVersions', 'DurableWorkflow\\Client', 'DurableWorkflow\\Worker',
            'DurableWorkflow\\Transport\\Psr18Transport'] as $class) {
            if (class_exists($class, false)) {
                throw new RuntimeException('completion_subject_already_loaded');
            }
        }
        foreach (['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'] as $key) {
            if (getenv($key) !== false && getenv($key) !== '') {
                throw new RuntimeException('completion_subject_proxy');
            }
        }
    }

    /** Validate before loading any consumer package or making a request. */
    public static function read(string $autoload, string $packet, string $sha256): self
    {
        if (preg_match('/\A[a-f0-9]{64}\z/', $sha256) !== 1) {
            throw new RuntimeException('completion_subject_identity');
        }
        $bytes = self::bytes($packet, 16384, 'completion_subject_identity');
        if (!hash_equals($sha256, hash('sha256', $bytes))) {
            throw new RuntimeException('completion_subject_identity');
        }
        $data = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
        $fields = ['schema', 'runner_reference', 'sdk_version', 'sdk_reference', 'lock_sha256',
            'server_version', 'server_image', 'endpoint', 'namespace', 'scenario', 'deadline_seconds'];
        if (is_array($data) && in_array($data['scenario'] ?? null, CompletionPollLoadProfile::SCENARIOS, true)) $fields[] = 'load';
        if (!is_array($data) || array_is_list($data) || ($data['schema'] ?? null) !== self::SCHEMA
            || array_diff(array_keys($data), $fields) !== [] || array_diff($fields, array_keys($data)) !== []) {
            throw new RuntimeException('completion_subject_shape');
        }
        foreach (['runner_reference', 'sdk_reference'] as $key) {
            if (!is_string($data[$key]) || preg_match('/\A[a-f0-9]{40}\z/', $data[$key]) !== 1) {
                throw new RuntimeException('completion_subject_reference');
            }
        }
        foreach (['sdk_version', 'server_version'] as $key) {
            if (!is_string($data[$key]) || preg_match('/\A[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta|rc)\.[1-9][0-9]*)?\z/', $data[$key]) !== 1) {
                throw new RuntimeException('completion_subject_version');
            }
        }
        if (!is_string($data['server_image'])
            || preg_match('~\A(?:ghcr\.io/durable-workflow/server|durableworkflow/server)@sha256:[a-f0-9]{64}\z~', $data['server_image']) !== 1
            || !is_string($data['namespace']) || preg_match('/\A[A-Za-z0-9][A-Za-z0-9_-]{0,63}\z/', $data['namespace']) !== 1
            || !in_array($data['scenario'], self::SCENARIOS, true)
            || !is_int($data['deadline_seconds']) || $data['deadline_seconds'] < 15 || $data['deadline_seconds'] > 120) {
            throw new RuntimeException('completion_subject_runtime');
        }
        self::loopback($data['endpoint']);
        CompletionPollLoadProfile::validate($data);
        $vendor = realpath(dirname($autoload));
        if ($vendor === false || basename($vendor) !== 'vendor' || realpath($autoload) !== $vendor.'/autoload.php') {
            throw new RuntimeException('completion_subject_autoload');
        }
        $lockBytes = self::bytes(dirname($vendor).'/composer.lock', 4194304, 'completion_subject_lock');
        if (!is_string($data['lock_sha256']) || preg_match('/\A[a-f0-9]{64}\z/', $data['lock_sha256']) !== 1
            || !hash_equals($data['lock_sha256'], hash('sha256', $lockBytes))) {
            throw new RuntimeException('completion_subject_lock');
        }
        $project = json_decode(file_get_contents(dirname($vendor).'/composer.json'), true, flags: JSON_THROW_ON_ERROR);
        if (($project['repositories'] ?? []) !== [] || ($project['scripts'] ?? []) !== []
            || ($project['config']['allow-plugins'] ?? null) !== false
            || isset($project['config']['audit']['ignore']) || ($project['config']['audit']['block-insecure'] ?? true) !== true
            || ($project['require']['durable-workflow/sdk'] ?? null) !== $data['sdk_version']) {
            throw new RuntimeException('completion_subject_project');
        }
        $lock = json_decode($lockBytes, true, flags: JSON_THROW_ON_ERROR);
        $packages = array_values(array_filter($lock['packages'] ?? [],
            static fn (array $package): bool => ($package['name'] ?? null) === 'durable-workflow/sdk'));
        if (count($packages) !== 1 || ($packages[0]['version'] ?? null) !== $data['sdk_version']
            || ($packages[0]['dist']['type'] ?? null) !== 'zip'
            || ($packages[0]['dist']['reference'] ?? null) !== $data['sdk_reference']
            || ($packages[0]['dist']['url'] ?? null) !== 'https://api.github.com/repos/durable-workflow/sdk-php/zipball/'.$data['sdk_reference']) {
            throw new RuntimeException('completion_subject_registry_distribution');
        }
        return new self($data, $vendor, $sha256);
    }

    /** One bounded descriptor supplies both the authenticated identity and parsed content. */
    private static function bytes(string $path, int $limit, string $code): string
    {
        if (!stream_is_local($path)) {
            throw new RuntimeException($code);
        }
        $stream = @fopen($path, 'rb');
        if ($stream === false) {
            throw new RuntimeException($code);
        }
        try {
            $metadata = fstat($stream);
            if ($metadata === false || ($metadata['mode'] & 0170000) !== 0100000) {
                throw new RuntimeException($code);
            }
            $bytes = stream_get_contents($stream, $limit + 1);
            if ($bytes === false || strlen($bytes) > $limit) {
                throw new RuntimeException($code);
            }
            return $bytes;
        } finally {
            fclose($stream);
        }
    }

    public static function loopback(mixed $endpoint): void
    {
        $parts = is_string($endpoint) ? parse_url($endpoint) : false;
        if (!is_array($parts) || array_diff(array_keys($parts), ['scheme', 'host', 'port']) !== []
            || ($parts['scheme'] ?? null) !== 'http' || ($parts['host'] ?? null) !== '127.0.0.1'
            || !is_int($parts['port'] ?? null) || $parts['port'] < 1024) {
            throw new RuntimeException('completion_subject_loopback');
        }
    }

    /** @return array<string, mixed> */
    public function installed(): array
    {
        $metadata = (new ReflectionClass(InstalledVersions::class))->getFileName();
        if (!is_string($metadata) || realpath($metadata) !== $this->vendor.'/composer/InstalledVersions.php'
            || InstalledVersions::getPrettyVersion('durable-workflow/sdk') !== $this->data['sdk_version']
            || InstalledVersions::getReference('durable-workflow/sdk') !== $this->data['sdk_reference']
            || realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk')) !== $this->vendor.'/durable-workflow/sdk') {
            throw new RuntimeException('completion_subject_installed_identity');
        }
        $implementations = [];
        foreach (['DurableWorkflow\\Client', 'DurableWorkflow\\Worker', 'DurableWorkflow\\Transport\\Psr18Transport'] as $class) {
            $file = (new ReflectionClass($class))->getFileName();
            $path = is_string($file) ? realpath($file) : false;
            if ($path === false || !str_starts_with($path, $this->vendor.'/durable-workflow/sdk/src/')) {
                throw new RuntimeException('completion_subject_source_fallback');
            }
            $implementations[$class] = substr($path, strlen($this->vendor) + 1);
        }
        $manifest = json_decode(file_get_contents($this->vendor.'/durable-workflow/sdk/composer.json'), true, flags: JSON_THROW_ON_ERROR);
        if (($manifest['extra']['durable-workflow']['product-train'] ?? null) !== $this->data['sdk_version']) {
            throw new RuntimeException('completion_subject_release_metadata');
        }
        return ['sdk_version' => $this->data['sdk_version'], 'sdk_reference' => $this->data['sdk_reference'],
            'subject_sha256' => $this->sha256,
            'caller_runner_reference' => $this->data['runner_reference'], 'lock_sha256' => $this->data['lock_sha256'],
            'implementation_paths' => $implementations];
    }
}
