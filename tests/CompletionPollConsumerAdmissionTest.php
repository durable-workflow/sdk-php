<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollSubject;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollSubject.php';

/**
 * Held consumer-root admission for the installed completion-poll runner.
 *
 * Each control builds a synthetic consumer root from the real sealed runner files
 * and a small fake vendor tree. No consumer autoload, package, network or runtime runs.
 */
final class CompletionPollConsumerAdmissionTest extends TestCase
{
    private const REFERENCE = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

    private string $base;
    private string $root;
    /** @var array<string, mixed> */
    private array $data;

    protected function setUp(): void
    {
        $this->base = sys_get_temp_dir().'/completion-admission-'.bin2hex(random_bytes(6));
        mkdir($this->base, 0700);
        $this->base = (string) realpath($this->base);
        $this->root = $this->base.'/consumer';
        $this->buildRoot($this->root);
        $this->data = $this->subject($this->root);
    }

    protected function tearDown(): void
    {
        self::remove($this->base);
    }

    public function testAdmitsOneHeldReadOnlyConsumerRoot(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $receipt = $subject->admit($this->root);
        self::assertSame($this->root, $receipt['consumer_root']);
        self::assertSame(self::seal($this->root, CompletionPollSubject::RUNNER_FILES), $receipt['runner_manifest_sha256']);
        self::assertSame(22, $receipt['runner_files']);
        self::assertSame(self::seal($this->root.'/vendor', self::files($this->root.'/vendor')), $receipt['vendor_inventory_sha256']);
        self::assertSame(self::seal($this->root.'/vendor/durable-workflow/sdk', self::files($this->root.'/vendor/durable-workflow/sdk')),
            $receipt['sdk_inventory_sha256']);
        self::assertSame(3, $receipt['sdk_files']);
        $subject->verifyHeld();
        $gate = $subject->routerGate();
        foreach (CompletionPollSubject::ROUTERS as $router) {
            $gate($this->root.'/'.$router, null);
            $gate($this->root.'/'.$router, $this->root.'/'.CompletionPollSubject::ROUTER_SERVER);
        }
    }

    public function testTheCurrentRunnerSealIsTheCanonicalSortedManifest(): void
    {
        $root = (string) realpath(__DIR__.'/..');
        self::assertSame(self::seal($root, CompletionPollSubject::RUNNER_FILES),
            CompletionPollSubject::runnerManifest($root)['sha256']);
    }

    public function testASourceCheckoutVendorOutsideTheRunnerRootCannotBeAdmitted(): void
    {
        $other = $this->base.'/source-checkout';
        $this->buildRoot($other);
        $this->data = $this->subject($other);
        $subject = $this->read($other);
        $this->expectExceptionMessage('completion_admission_root');
        $subject->admit($this->root);
    }

    public function testAnAliasedRootCannotBeAdmitted(): void
    {
        symlink($this->root, $this->base.'/alias');
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_root');
        $subject->admit($this->base.'/alias');
    }

    #[DataProvider('runnerChanges')]
    public function testAWrongOrMissingSealedHelperOrRouterRefuses(string $path, string $change): void
    {
        $file = $this->root.'/'.$path;
        if ($change === 'missing') {
            unlink($file);
        } elseif ($change === 'substituted') {
            file_put_contents($file, "\n", FILE_APPEND);
        } else {
            rename($file, $this->base.'/'.basename($file));
            symlink($this->base.'/'.basename($file), $file);
        }
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_runner');
        $subject->admit($this->root);
    }

    public static function runnerChanges(): iterable
    {
        yield 'missing worker HTTP router' => ['tests/Support/completion-worker-http.php', 'missing'];
        yield 'substituted relay router' => ['scripts/conformance/Support/completion-poll-relay.php', 'substituted'];
        yield 'substituted subject reader' => ['scripts/conformance/Support/CompletionPollSubject.php', 'substituted'];
        yield 'aliased pipeline server helper' => ['tests/Support/completion-pipeline-server.php', 'aliased'];
    }

    public function testASubstitutedInstalledSdkFileRefuses(): void
    {
        file_put_contents($this->root.'/vendor/durable-workflow/sdk/src/Client.php', "<?php // source\n");
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_package');
        $subject->admit($this->root);
    }

    public function testASubstitutedDependencyFileRefuses(): void
    {
        file_put_contents($this->root.'/vendor/psr/log/src/LoggerInterface.php', "<?php // changed\n");
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_vendor');
        $subject->admit($this->root);
    }

    public function testAPathRepositorySymlinkCannotStandInForTheInstalledPackage(): void
    {
        $source = $this->base.'/sdk-source';
        rename($this->root.'/vendor/durable-workflow/sdk', $source);
        symlink($source, $this->root.'/vendor/durable-workflow/sdk');
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_vendor');
        $subject->admit($this->root);
    }

    #[DataProvider('graphChanges')]
    public function testGraphOrLockDriftRefuses(string $document, callable $change): void
    {
        $path = $this->root.'/'.$document;
        $value = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        $change($value);
        file_put_contents($path, json_encode($value, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
        $this->data = $this->subject($this->root);
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_graph');
        $subject->admit($this->root);
    }

    public static function graphChanges(): iterable
    {
        yield 'installed dependency version differs from lock' => ['vendor/composer/installed.json',
            static function (array &$value): void { $value['packages'][1]['version'] = '3.0.1'; }];
        yield 'installed package absent from lock' => ['vendor/composer/installed.json',
            static function (array &$value): void { $value['packages'][] = ['name' => 'fixture/extra', 'version' => '1.0.0']; }];
        yield 'development install' => ['vendor/composer/installed.json',
            static function (array &$value): void { $value['dev'] = true; }];
        yield 'relocated SDK install path' => ['vendor/composer/installed.json',
            static function (array &$value): void { $value['packages'][0]['install-path'] = '../../sdk-source'; }];
        yield 'path distribution in lock and graph' => ['composer.lock',
            static function (array &$value): void { $value['packages'][1]['dist'] = ['type' => 'path', 'url' => '../log']; }];
        yield 'development packages in lock' => ['composer.lock',
            static function (array &$value): void { $value['packages-dev'] = [['name' => 'phpunit/phpunit', 'version' => '11.0.0']]; }];
    }

    #[DataProvider('authorities')]
    public function testExpiredOrOpenEndedAuthorityRefuses(int $offset): void
    {
        $this->data['admission_expires_at'] = time() + $offset;
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_expired');
        $subject->admit($this->root);
    }

    public static function authorities(): iterable
    {
        yield 'expired' => [-1];
        yield 'longer than the bounded window' => [CompletionPollSubject::AUTHORITY_SECONDS + 60];
    }

    public function testAWritableRootIsNotHeld(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_writable');
        $subject->admit($this->root);
    }

    public function testAReplacedRootRefusesAfterAdmission(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        rename($this->root, $this->base.'/moved');
        mkdir($this->root, 0555);
        $this->expectExceptionMessage('completion_admission_replaced');
        $subject->verifyHeld();
    }

    public function testARootMadeWritableAfterAdmissionRefuses(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        chmod($this->root.'/vendor', 0755);
        $this->expectExceptionMessage('completion_admission_replaced');
        $subject->routerGate()($this->root.'/'.CompletionPollSubject::ROUTERS[0], null);
    }

    /**
     * A host writer can change a file's bytes in place while every held directory
     * keeps its identity and read-only mode. The changed bytes must be refused
     * before they can be required, launched or reported.
     */
    #[DataProvider('postAdmissionChanges')]
    public function testPostAdmissionInPlaceChangesRefuseBeforeUse(string $path, string $consumer): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        $file = $this->root.'/'.$path;
        $mode = fileperms($file) & 0777;
        chmod($file, 0644);
        file_put_contents($file, "\n// changed after admission\n", FILE_APPEND);
        chmod($file, $mode);
        $this->expectExceptionMessage('completion_admission_changed');
        if ($consumer === 'launch') {
            $subject->routerGate()($this->root.'/'.CompletionPollSubject::ROUTERS[2],
                $this->root.'/'.CompletionPollSubject::ROUTER_SERVER);
        } else {
            $subject->verifyHeld();
        }
    }

    public static function postAdmissionChanges(): iterable
    {
        yield 'worker HTTP router before launch' => ['tests/Support/completion-worker-http.php', 'launch'];
        yield 'relay router before launch' => ['scripts/conformance/Support/completion-poll-relay.php', 'launch'];
        yield 'pipeline server helper before launch' => ['tests/Support/completion-pipeline-server.php', 'launch'];
        yield 'subject reader before launch' => ['scripts/conformance/Support/CompletionPollSubject.php', 'launch'];
        yield 'autoloader before require' => ['vendor/autoload.php', 'require'];
        yield 'class loader before require' => ['vendor/composer/ClassLoader.php', 'require'];
        yield 'installed metadata before identity' => ['vendor/composer/installed.json', 'require'];
        yield 'installed SDK source before launch' => ['vendor/durable-workflow/sdk/src/Client.php', 'launch'];
        yield 'dependency source before result' => ['vendor/psr/log/src/LoggerInterface.php', 'require'];
        yield 'lock before result' => ['composer.lock', 'require'];
    }

    public function testAFileAddedToTheVendorTreeAfterAdmissionRefuses(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        chmod($this->root.'/vendor/psr/log/src', 0755);
        file_put_contents($this->root.'/vendor/psr/log/src/Injected.php', "<?php // injected\n");
        $this->expectExceptionMessage('completion_admission_changed');
        $subject->verifyHeld();
    }

    #[DataProvider('mountTables')]
    public function testOnlyAContainerPrivateTmpfsRootIsAnExecutionBoundary(string $root, string $mountinfo, bool $accepted): void
    {
        if (!$accepted) {
            $this->expectExceptionMessage('completion_admission_boundary');
        }
        CompletionPollSubject::privateRoot($root, $mountinfo);
        self::assertTrue($accepted);
    }

    public static function mountTables(): iterable
    {
        $base = "1 0 0:31 / / ro,relatime - overlay overlay rw\n2 1 0:33 / /tmp rw - tmpfs tmpfs rw,size=262144k\n";
        $held = "50 1 0:60 / /held rw,nosuid,nodev - tmpfs tmpfs rw,size=524288k,uid=10001,gid=10001,mode=700\n";
        yield 'private tmpfs root' => ['/held', $base.$held, true];
        yield 'host directory bound at the root' => ['/held', $base."50 1 8:1 /srv/sdk-php-cpq/consumer /held ro - ext4 /dev/sda1 rw\n", false];
        yield 'directory inside a tmpfs, not its mount point' => ['/tmp/consumer', $base, false];
        yield 'host vendor bound beneath the root' => ['/held', $base.$held."51 50 8:1 /srv/vendor /held/vendor ro - ext4 /dev/sda1 rw\n", false];
        yield 'tmpfs later overmounted by a host bind' => ['/held', $base.$held."52 1 8:1 /srv/consumer /held ro - ext4 /dev/sda1 rw\n", false];
        yield 'subtree of a shared tmpfs' => ['/held', $base."50 1 0:60 /shared /held rw - tmpfs tmpfs rw\n", false];
        yield 'no mount at the root' => ['/held', $base, false];
        yield 'unparsable table' => ['/held', "garbage\n", false];
        yield 'relative root' => ['held', $base.$held, false];
        yield 'escaped root path' => ['/held\\040x', $base.$held, false];
    }

    #[DataProvider('routers')]
    public function testOnlyTheThreeSealedRoutersCanLaunch(string $router, ?string $server): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        $gate = $subject->routerGate();
        $this->expectExceptionMessage('completion_admission_router');
        $gate(str_replace('{root}', $this->root, $router), $server === null ? null : str_replace('{root}', $this->root, $server));
    }

    public static function routers(): iterable
    {
        yield 'unlisted runner file' => ['{root}/scripts/conformance/qualify-completion-polls.php', null];
        yield 'aliased router path' => ['{root}/tests/Support/../Support/completion-worker-http.php', null];
        yield 'router outside the held root' => [__DIR__.'/Support/completion-worker-http.php', null];
        yield 'substituted server helper' => ['{root}/tests/Support/completion-pipeline-http.php', '{root}/tests/Support/completion-worker-http.php'];
    }

    public function testTheGateAndHeldChecksRequireAdmissionFirst(): void
    {
        $subject = $this->read();
        $this->expectExceptionMessage('completion_admission_required');
        $subject->routerGate();
    }

    public function testAdmissionCannotBeRepeated(): void
    {
        $this->requireNonRoot();
        $subject = $this->read();
        $this->hold();
        $subject->admit($this->root);
        $this->expectExceptionMessage('completion_admission_repeated');
        $subject->admit($this->root);
    }

    public function testTheScenarioGateRefusesBeforeAnyRouterProcessStarts(): void
    {
        $code = 'require '.var_export(__DIR__.'/Support/CompletionPipelineScenario.php', true).';'
            .' DurableWorkflow\Tests\Support\CompletionPipelineScenario::gate(static function (string $router, ?string $server): void {'
            .' throw new RuntimeException("refused:".basename($router).":".($server === null ? "none" : basename($server))); });'
            .' try { DurableWorkflow\Tests\Support\CompletionPipelineScenario::run(static fn () => "launched"); }'
            .' catch (RuntimeException $error) { echo $error->getMessage(); }';
        $process = proc_open([PHP_BINARY, '-r', $code], [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        $output = stream_get_contents($pipes[1]);
        $errors = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        self::assertSame(0, proc_close($process), $errors);
        self::assertSame('refused:completion-pipeline-http.php:none', $output);
    }

    private function buildRoot(string $root): void
    {
        $source = (string) realpath(__DIR__.'/..');
        foreach (CompletionPollSubject::RUNNER_FILES as $path) {
            if (!is_dir(dirname($root.'/'.$path))) mkdir(dirname($root.'/'.$path), 0700, true);
            copy($source.'/'.$path, $root.'/'.$path);
        }
        $files = [
            'vendor/autoload.php' => '<?php throw new RuntimeException("Consumer autoload must not execute in admission.");',
            'vendor/composer/ClassLoader.php' => "<?php // fixture\n",
            'vendor/durable-workflow/sdk/composer.json' => '{"name":"durable-workflow/sdk"}',
            'vendor/durable-workflow/sdk/src/Client.php' => "<?php // installed\n",
            'vendor/durable-workflow/sdk/resources/protocol/sdk-metrics-v1.json' => '{}',
            'vendor/psr/log/src/LoggerInterface.php' => "<?php // installed\n",
        ];
        foreach ($files as $path => $bytes) {
            if (!is_dir(dirname($root.'/'.$path))) mkdir(dirname($root.'/'.$path), 0700, true);
            file_put_contents($root.'/'.$path, $bytes);
        }
        $sdk = ['name' => 'durable-workflow/sdk', 'version' => '9.0.0', 'dist' => ['type' => 'zip', 'reference' => self::REFERENCE,
            'url' => 'https://api.github.com/repos/durable-workflow/sdk-php/zipball/'.self::REFERENCE, 'shasum' => '']];
        $log = ['name' => 'psr/log', 'version' => '3.0.2', 'dist' => ['type' => 'zip', 'reference' => str_repeat('f', 40),
            'url' => 'https://api.github.com/repos/php-fig/log/zipball/'.str_repeat('f', 40), 'shasum' => '']];
        file_put_contents($root.'/composer.lock', json_encode(['packages' => [$sdk, $log], 'packages-dev' => []], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
        file_put_contents($root.'/composer.json', json_encode(['require' => ['durable-workflow/sdk' => '9.0.0'],
            'config' => ['allow-plugins' => false]], JSON_THROW_ON_ERROR));
        file_put_contents($root.'/vendor/composer/installed.json', json_encode(['packages' => [
            $sdk + ['install-path' => '../durable-workflow/sdk'], $log + ['install-path' => '../psr/log']],
            'dev' => false, 'dev-package-names' => []], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
    }

    /** @return array<string, mixed> */
    private function subject(string $root): array
    {
        return ['schema' => CompletionPollSubject::SCHEMA, 'runner_reference' => str_repeat('b', 40),
            'sdk_version' => '9.0.0', 'sdk_reference' => self::REFERENCE,
            'lock_sha256' => hash_file('sha256', $root.'/composer.lock'),
            'runner_manifest_sha256' => self::seal($root, CompletionPollSubject::RUNNER_FILES),
            'vendor_inventory_sha256' => self::seal($root.'/vendor', self::files($root.'/vendor')),
            'sdk_inventory_sha256' => self::seal($root.'/vendor/durable-workflow/sdk', self::files($root.'/vendor/durable-workflow/sdk')),
            'admission_expires_at' => time() + 600,
            'server_version' => '9.0.0', 'server_image' => 'ghcr.io/durable-workflow/server@sha256:'.str_repeat('c', 64),
            'endpoint' => 'http://127.0.0.1:12345', 'namespace' => 'fixture', 'scenario' => 'carriers', 'deadline_seconds' => 30];
    }

    private function read(?string $root = null): CompletionPollSubject
    {
        $root ??= $this->root;
        $path = $this->base.'/subject-'.bin2hex(random_bytes(4)).'.json';
        file_put_contents($path, json_encode($this->data, JSON_THROW_ON_ERROR));
        return CompletionPollSubject::read($root.'/vendor/autoload.php', $path, hash_file('sha256', $path));
    }

    /** Make the held directories read-only, as the caller's read-only mount does. */
    private function hold(): void
    {
        foreach (['/vendor/durable-workflow/sdk', '/vendor/composer', '/vendor', ''] as $path) {
            chmod($this->root.$path, 0555);
        }
    }

    private function requireNonRoot(): void
    {
        if (function_exists('posix_geteuid') && posix_geteuid() === 0) {
            self::markTestSkipped('Directory permissions cannot model a read-only mount for root.');
        }
    }

    /**
     * Independent canonical seal: sorted compact JSON entries plus one newline.
     *
     * @param list<string> $paths
     */
    private static function seal(string $root, array $paths): string
    {
        sort($paths, SORT_STRING);
        $entries = array_map(static fn (string $path): string => sprintf('{"bytes":%d,"path":"%s","sha256":"%s"}',
            filesize($root.'/'.$path), $path, hash_file('sha256', $root.'/'.$path)), $paths);
        return hash('sha256', '['.implode(',', $entries)."]\n");
    }

    /** @return list<string> */
    private static function files(string $root): array
    {
        $result = [];
        $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS));
        foreach ($iterator as $file) {
            $result[] = substr($file->getPathname(), strlen($root) + 1);
        }
        return $result;
    }

    private static function remove(string $path): void
    {
        if (is_link($path) || is_file($path)) {
            @chmod(dirname($path), 0700);
            unlink($path);
            return;
        }
        if (!is_dir($path)) {
            return;
        }
        chmod($path, 0700);
        foreach (scandir($path) ?: [] as $name) {
            if ($name !== '.' && $name !== '..') self::remove($path.'/'.$name);
        }
        rmdir($path);
    }
}
