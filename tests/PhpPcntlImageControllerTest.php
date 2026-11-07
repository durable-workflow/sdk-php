<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Causal controls for the phase-P outer controller, run against a fake Docker
 * CLI that models containers, images and the tag as files. No real container
 * or image is created.
 */
final class PhpPcntlImageControllerTest extends TestCase
{
    private const CONTROLLER = __DIR__.'/../scripts/conformance/build-php-pcntl-image.sh';
    private const SCRIPT = __DIR__.'/../scripts/conformance/php-pcntl-build.sh';
    private const FAKE = __DIR__.'/Support/fake-docker.sh';

    private string $base;
    private string $state;
    private string $work;

    protected function setUp(): void
    {
        foreach (['bash', 'timeout', 'sha256sum', 'od', 'stat'] as $tool) {
            if (trim((string) shell_exec('command -v '.$tool)) === '') {
                self::markTestSkipped($tool.' is required for the controller controls.');
            }
        }
        $this->base = sys_get_temp_dir().'/phase-p-'.bin2hex(random_bytes(6));
        $this->state = $this->base.'/state';
        $this->work = $this->base.'/work';
        mkdir($this->state.'/containers', 0700, true);
        mkdir($this->state.'/images', 0700, true);
        mkdir($this->work, 0700);
    }

    protected function tearDown(): void
    {
        exec('rm -rf '.escapeshellarg($this->base));
    }

    public function testSuccessRetainsExactlyOneLabeledImageAndRemovesTheContainer(): void
    {
        [$status, $stdout] = $this->control('');
        self::assertSame(0, $status);
        $receipt = json_decode($stdout, true, flags: JSON_THROW_ON_ERROR);
        self::assertSame('durable-workflow.sdk-php-phase-p-receipt/v1', $receipt['schema']);
        self::assertMatchesRegularExpression('/\Asha256:[a-f0-9]{64}\z/', $receipt['image']);
        self::assertSame('caeb0d94d6b8b1853aed199acd575dac92d1bb74e6b3de2cfba56befdcdfa8de', $receipt['pcntl_so_sha256']);
        self::assertSame(11, $receipt['layers']);
        self::assertSame([], $this->containers());
        self::assertSame([substr($receipt['image'], 7)], $this->images());
        self::assertSame($receipt['image'], trim((string) file_get_contents($this->state.'/tag')));
        self::assertSame($receipt['run'], trim((string) file_get_contents($this->state.'/images/'.substr($receipt['image'], 7))));
    }

    #[DataProvider('refusals')]
    public function testRefusalsMakeNoDockerWrite(string $scenario, string $setup): void
    {
        $script = self::SCRIPT;
        $environment = [];
        if ($setup === 'foreign-tag') {
            file_put_contents($this->state.'/images/'.str_repeat('f', 64), 'foreign');
            file_put_contents($this->state.'/tag', 'sha256:'.str_repeat('f', 64));
        } elseif ($setup === 'foreign-container') {
            file_put_contents($this->state.'/containers/sdk-php-cpq-p-build', 'foreign');
        } elseif ($setup === 'changed-script') {
            $script = $this->base.'/changed.sh';
            file_put_contents($script, file_get_contents(self::SCRIPT)."\n");
        } elseif ($setup === 'nonempty-work') {
            file_put_contents($this->work.'/left', 'x');
        } elseif ($setup === 'raised-timeout') {
            $environment['PHASE_P_START_TIMEOUT'] = '301';
        }
        $before = [$this->containers(), $this->images(), @file_get_contents($this->state.'/tag')];
        [$status, $stdout] = $this->control($scenario, $script, $environment);
        self::assertSame(2, $status);
        self::assertSame('', $stdout);
        self::assertSame($before, [$this->containers(), $this->images(), @file_get_contents($this->state.'/tag')]);
        foreach ($this->calls() as $call) {
            self::assertMatchesRegularExpression('/\A(image inspect|container inspect) /', $call);
        }
    }

    public static function refusals(): iterable
    {
        yield 'existing tag owned by someone else' => ['', 'foreign-tag'];
        yield 'existing build container name' => ['', 'foreign-container'];
        yield 'different base image identity' => ['base_mismatch', ''];
        yield 'changed build script bytes' => ['', 'changed-script'];
        yield 'non-empty work directory' => ['', 'nonempty-work'];
        yield 'raised start bound' => ['', 'raised-timeout'];
        yield 'daemon error while probing names is not absence' => ['probe_error', ''];
    }

    public function testAForeignContainerHoldingTheNameAfterAFailedCreateIsNotRemoved(): void
    {
        [$status, $stdout] = $this->control('create_foreign');
        self::assertSame(1, $status);
        self::assertSame('', $stdout);
        self::assertSame(['sdk-php-cpq-p-build'], $this->containers());
        self::assertSame('foreign', trim((string) file_get_contents($this->state.'/containers/sdk-php-cpq-p-build')));
        self::assertFalse($this->called('rm '));
    }

    public function testTheConsumedScriptIsTheHashedScript(): void
    {
        [$status] = $this->control('');
        self::assertSame(0, $status);
        self::assertSame(hash_file('sha256', self::SCRIPT), hash_file('sha256', $this->state.'/create_script'));
        self::assertSame('6ec44aaaa1cc287b4345286623cd9ffbca8ba4b02e78eff434f67398b07d057b', hash_file('sha256', $this->state.'/create_script'));
    }

    #[DataProvider('failures')]
    public function testEveryFailureAfterAnEffectLeavesNoOwnedObjectAndNoFalseSuccess(string $scenario, bool $committed): void
    {
        [$status, $stdout] = $this->control($scenario, self::SCRIPT, ['PHASE_P_START_TIMEOUT' => '2']);
        self::assertSame(1, $status);
        self::assertSame('', $stdout);
        self::assertSame([], $this->containers());
        self::assertSame([], $this->images());
        self::assertFileDoesNotExist($this->state.'/tag');
        self::assertSame($committed, $this->called('commit '));
    }

    public static function failures(): iterable
    {
        yield 'create failed after creating the container' => ['create_partial', false];
        yield 'build exited non-zero' => ['start_fail', false];
        yield 'build exceeded its wall bound' => ['start_timeout', false];
        yield 'build output exceeded its bound' => ['start_flood', false];
        yield 'writable layer over its bound' => ['size_over', false];
        yield 'diff has an extra path' => ['diff_extra', false];
        yield 'commit failed' => ['commit_fail', true];
        yield 'commit created an image then failed' => ['commit_partial', true];
        yield 'committed image configuration differs' => ['config_differs', true];
        yield 'committed image labels differ' => ['labels_wrong', true];
        yield 'committed image adds more than one layer' => ['layers_wrong', true];
        yield 'committed image fails its pcntl check' => ['check_wrong', true];
    }

    public function testATimeoutKillsTheBuildContainer(): void
    {
        [$status] = $this->control('start_timeout', self::SCRIPT, ['PHASE_P_START_TIMEOUT' => '2']);
        self::assertSame(1, $status);
        self::assertFileExists($this->state.'/kills');
    }

    #[DataProvider('residues')]
    public function testUnprovenRemovalIsReportedAsResidueNotFailureOrSuccess(string $scenario): void
    {
        [$status, $stdout] = $this->control($scenario);
        self::assertSame(3, $status);
        self::assertSame('', $stdout);
    }

    public static function residues(): iterable
    {
        yield 'container removal failed' => ['start_fail,rm_fail'];
        yield 'container state unknown after removal' => ['start_fail,probe_error_after_rm'];
        yield 'image listing failed' => ['check_wrong,images_error'];
        yield 'image state unknown after removal' => ['check_wrong,image_probe_error'];
        yield 'image removal failed' => ['check_wrong,rmi_fail'];
    }

    /**
     * @param array<string, string> $environment
     * @return array{int, string, string}
     */
    private function control(string $scenario, string $script = self::SCRIPT, array $environment = []): array
    {
        $environment += ['PHASE_P_DOCKER' => self::FAKE, 'FAKE_DOCKER_STATE' => $this->state,
            'FAKE_DOCKER_SCENARIO' => $scenario, 'PATH' => (string) getenv('PATH')];
        $process = proc_open(['bash', self::CONTROLLER, $script, $this->work],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, env_vars: $environment);
        self::assertIsResource($process);
        $stdout = (string) stream_get_contents($pipes[1]);
        $stderr = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        return [proc_close($process), $stdout, $stderr];
    }

    /** @return list<string> */
    private function containers(): array
    {
        return array_values(array_diff(scandir($this->state.'/containers') ?: [], ['.', '..']));
    }

    /** @return list<string> */
    private function images(): array
    {
        return array_values(array_diff(scandir($this->state.'/images') ?: [], ['.', '..']));
    }

    /** @return list<string> */
    private function calls(): array
    {
        return is_file($this->state.'/calls') ? file($this->state.'/calls', FILE_IGNORE_NEW_LINES) : [];
    }

    private function called(string $prefix): bool
    {
        foreach ($this->calls() as $call) {
            if (str_starts_with($call, $prefix)) return true;
        }
        return false;
    }
}
