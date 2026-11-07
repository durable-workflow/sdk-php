<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Causal controls for the phase-A acquisition script, run against a fake Docker
 * CLI that replays the real captured registry manifests. Nothing is pulled.
 */
final class AcquireCellImagesTest extends TestCase
{
    private const SCRIPT = __DIR__.'/../scripts/conformance/acquire-cell-images.sh';
    private const FAKE = __DIR__.'/Support/fake-docker-acquire.sh';
    private const KEYS = [
        'server-3.2.0' => '2a1c9659d6f00a8c891e901835054c12cdc913a91167717d9d508c3ca1a95653',
        'server-3.0.5' => '2659280f3ad58d1812ce00f44a927beed9781237143b8487ef12e1301eed551e',
        'mysql-8.0.46' => '62fb722c78b24245ddff1796a0fcee4a49cc5b87e0aaaf20c92d1da9e0a2497b',
        'postgres-16.15' => '43f9275df91e012c3da3d86bea5d7bb5e67a473296cc906c5f9e80b8176b1a37',
    ];

    private string $base = '';
    private string $state;
    private string $work;

    protected function setUp(): void
    {
        foreach (['bash', 'timeout', 'sha256sum', 'python3', 'df'] as $tool) {
            if (trim((string) shell_exec('command -v '.$tool)) === '') {
                self::markTestSkipped($tool.' is required for the acquisition controls.');
            }
        }
        $this->base = sys_get_temp_dir().'/phase-a-'.bin2hex(random_bytes(6));
        $this->state = $this->base.'/state';
        $this->work = $this->base.'/work';
        mkdir($this->state.'/images', 0700, true);
        mkdir($this->work, 0700);
    }

    protected function tearDown(): void
    {
        if ($this->base !== '') exec('rm -rf '.escapeshellarg($this->base));
    }

    public function testTheCapturedManifestsBindTheirPinnedConfigs(): void
    {
        foreach (self::KEYS as $name => $digest) {
            self::assertSame($digest, hash_file('sha256', __DIR__.'/fixtures/phase-a/'.$name.'.json'));
        }
        [$status, $stdout] = $this->acquire('', ['--metadata-only']);
        self::assertSame(0, $status);
        $plan = json_decode($stdout, true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(493387190, $plan['compressed_bytes']);
        self::assertSame(['absent', 'absent', 'absent', 'absent'], array_column($plan['images'], 'local'));
        self::assertSame([], $this->images());
        self::assertFalse($this->called('pull '));
    }

    public function testSuccessPullsEachAbsentImageOnceByItsManifest(): void
    {
        [$status, $stdout] = $this->acquire('');
        self::assertSame(0, $status);
        $receipt = json_decode($stdout, true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(array_fill(0, 4, 'pulled'), array_column($receipt['images'], 'acquired'));
        self::assertEqualsCanonicalizing(array_values(self::KEYS), $this->images());
        $pulls = array_values(array_filter($this->calls(), static fn (string $call): bool => str_starts_with($call, 'pull ')));
        self::assertCount(4, $pulls);
        foreach ($pulls as $pull) {
            self::assertStringContainsString('--platform linux/amd64 ', $pull);
            self::assertMatchesRegularExpression('/@sha256:[a-f0-9]{64}\z/', $pull);
        }
    }

    public function testAPreExistingImageIsNeitherPulledNorRemoved(): void
    {
        touch($this->state.'/images/'.self::KEYS['mysql-8.0.46']);
        [$status, $stdout] = $this->acquire('pull_fail');
        self::assertSame(1, $status);
        self::assertSame('', $stdout);
        self::assertSame([self::KEYS['mysql-8.0.46']], $this->images());
        foreach ($this->calls() as $call) {
            self::assertStringNotContainsString('pull --quiet --platform linux/amd64 docker.io/library/mysql', $call);
        }
    }

    #[DataProvider('refusals')]
    public function testRefusalsPullNothing(string $scenario, array $environment): void
    {
        [$status, $stdout] = $this->acquire($scenario, [], $environment);
        self::assertSame(2, $status);
        self::assertSame('', $stdout);
        self::assertSame([], $this->images());
        self::assertFalse($this->called('pull '));
    }

    public static function refusals(): iterable
    {
        yield 'manifest bytes differ from the pinned digest' => ['manifest_tampered', []];
        yield 'local state unknown' => ['probe_error', []];
        yield 'raised total bound' => ['', ['PHASE_A_TOTAL_SECONDS' => '301']];
    }

    #[DataProvider('failures')]
    public function testFailuresRemoveExactlyWhatThisRunPulled(string $scenario, array $environment): void
    {
        [$status, $stdout] = $this->acquire($scenario, [], $environment);
        self::assertSame(1, $status);
        self::assertSame('', $stdout);
        self::assertSame([], $this->images());
    }

    public static function failures(): iterable
    {
        yield 'pull failed after creating its reference' => ['pull_partial', []];
        yield 'pull exceeded the total bound' => ['pull_timeout', ['PHASE_A_TOTAL_SECONDS' => '20']];
        yield 'later pull failed' => ['pull_fail', []];
        yield 'local identity differs from the manifest' => ['wrong_identity', []];
    }

    #[DataProvider('residues')]
    public function testUnprovenRemovalIsResidue(string $scenario): void
    {
        [$status, $stdout] = $this->acquire($scenario);
        self::assertSame(3, $status);
        self::assertSame('', $stdout);
    }

    public static function residues(): iterable
    {
        yield 'image removal failed' => ['pull_fail,rm_fail'];
        yield 'image state unknown after removal' => ['pull_fail,probe_error_after_rm'];
    }

    /**
     * @param list<string> $options
     * @param array<string, string> $environment
     * @return array{int, string, string}
     */
    private function acquire(string $scenario, array $options = [], array $environment = []): array
    {
        $environment += ['PHASE_A_DOCKER' => self::FAKE, 'FAKE_DOCKER_STATE' => $this->state,
            'FAKE_DOCKER_SCENARIO' => $scenario, 'PATH' => (string) getenv('PATH')];
        $process = proc_open(['bash', self::SCRIPT, ...$options, $this->work],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, env_vars: $environment);
        self::assertIsResource($process);
        $stdout = (string) stream_get_contents($pipes[1]);
        $stderr = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        return [proc_close($process), $stdout, $stderr];
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
