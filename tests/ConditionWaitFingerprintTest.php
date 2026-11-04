<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\ConditionWaitDefinition;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConditionWaitFingerprintTest extends TestCase
{
    /** @var list<string> */
    private array $files = [];

    protected function tearDown(): void
    {
        foreach ($this->files as $file) {
            if (is_file($file)) {
                unlink($file);
            }
        }
    }

    public function testWhitespaceCommentsAndLocationsDoNotChangeV2(): void
    {
        $first = $this->load('return static function (): bool { return false; };');
        $second = $this->load("\n\nreturn static function ( ) : bool {\n    // Approval is pending.\n    return /* explanation */ false ;\n};");

        self::assertStringStartsWith('dwphp2:sha256:', ConditionWaitDefinition::fingerprint($first));
        self::assertSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($second));
        self::assertNotSame(ConditionWaitDefinition::legacyFingerprint($first), ConditionWaitDefinition::legacyFingerprint($second));
    }

    public function testNeighboringCallArgumentsAreOutsideTheFingerprint(): void
    {
        $first = $this->load("return (static fn (\$predicate, \$key, \$timeout) => \$predicate)(static fn (): bool => false, 'approval', 30);");
        $second = $this->load("return (static fn (\$predicate, \$key, \$timeout) => \$predicate)(static fn (): bool => false, 'other', 90);");

        self::assertSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($second));
    }

    #[DataProvider('changedPredicates')]
    public function testRealTokenChangesKeepDifferentIdentities(string $left, string $right): void
    {
        self::assertNotSame(ConditionWaitDefinition::fingerprint($this->load($left)), ConditionWaitDefinition::fingerprint($this->load($right)));
    }

    public static function changedPredicates(): iterable
    {
        yield 'result' => ['return static fn (): bool => false;', 'return static fn (): bool => true;'];
        yield 'operator boundaries' => ['$value = 1; return static fn () => $value + +$value;', '$value = 1; return static fn () => $value++ +$value;'];
        yield 'literal whitespace' => ["return static fn () => 'a b';", "return static fn () => 'ab';"];
        yield 'nested closure body' => ['return static fn () => (static fn () => false)();', 'return static fn () => (static fn () => true)();'];
    }

    public function testIndistinguishableSameLineClosuresShareTheWholeCandidateIdentity(): void
    {
        [$first, $second] = $this->load('return [static fn (): bool => false, static fn (): bool => true];');
        [$changed] = $this->load('return [static fn (): bool => false, static fn (): bool => !false];');

        self::assertSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($second));
        self::assertNotSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($changed));
    }

    public function testParameterNamesDistinguishSameLineClosures(): void
    {
        [$first, $second] = $this->load('return [static fn ($left) => $left, static fn ($right) => $right];');
        [$unchanged] = $this->load('return [static fn ($left) => $left, static fn ($right) => !$right];');

        self::assertNotSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($second));
        self::assertSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($unchanged));
    }

    public function testNamedAndInvokableCallablesCanBeReadWithoutCallingThem(): void
    {
        $object = $this->load('return new class { public function ready(): bool { throw new \LogicException("Not evaluated"); } public function __invoke(): bool { return false; } };');

        self::assertStringStartsWith('dwphp2:sha256:', ConditionWaitDefinition::fingerprint(Closure::fromCallable([$object, 'ready'])));
        self::assertStringStartsWith('dwphp2:sha256:', ConditionWaitDefinition::fingerprint(Closure::fromCallable($object)));
    }

    public function testBalancedBodiesTernariesAndCrLfRemainPortable(): void
    {
        $source = 'return static fn (): bool => (true ? (static fn (bool $unused = false): bool => false)() : true);';
        $first = $this->load($source);
        $second = $this->load("\r\n".str_replace(' => ', "\r\n    => ", $source));

        self::assertSame(ConditionWaitDefinition::fingerprint($first), ConditionWaitDefinition::fingerprint($second));
    }

    public function testUnavailableSourceDoesNotReuseCachedTokens(): void
    {
        $predicate = $this->load('return static fn (): bool => false;');
        self::assertNotNull(ConditionWaitDefinition::fingerprint($predicate));
        unlink($this->files[array_key_last($this->files)]);

        self::assertNull(ConditionWaitDefinition::fingerprint($predicate));
        self::assertNull(ConditionWaitDefinition::fingerprint(Closure::fromCallable('strlen')));
    }

    public function testGoldenIdentityMatchesInFreshProcesses(): void
    {
        $predicate = $this->load('return static fn (): bool => false;');
        $file = $this->files[array_key_last($this->files)];
        $expected = 'dwphp2:sha256:02a95ef3eef2f4aec3dbd83735628c87719b16370c73743a0bd4fc7b91abc15c';
        self::assertSame($expected, ConditionWaitDefinition::fingerprint($predicate));
        if (!function_exists('proc_open')) {
            self::markTestSkipped('Fresh-process evidence requires proc_open.');
        }
        for ($attempt = 0; $attempt < 2; ++$attempt) {
            $process = proc_open([PHP_BINARY, '-r',
                'require $argv[1]; echo DurableWorkflow\\Worker\\ConditionWaitDefinition::fingerprint(require $argv[2]);',
                dirname(__DIR__).'/vendor/autoload.php', $file,
            ], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
            self::assertIsResource($process);
            $output = stream_get_contents($pipes[1]);
            $errors = stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            self::assertSame(0, proc_close($process), $errors);
            self::assertSame($expected, $output);
        }
    }

    public function testLegacyWaitsKeepTheirFirstFingerprintOnReopen(): void
    {
        $predicate = $this->load('return static fn (): bool => false;');
        $legacy = ConditionWaitDefinition::legacyFingerprint($predicate);
        $commands = $this->replay($predicate, $legacy);

        self::assertSame($legacy, $commands[0]['condition_definition_fingerprint']);
        self::assertSame('php:condition-wait:0', $commands[0]['condition_wait_occurrence_id']);
    }

    public function testAWaitWithoutARecordedFingerprintKeepsItsAbsence(): void
    {
        $commands = $this->replay($this->load('return static fn (): bool => false;'), null);

        self::assertArrayNotHasKey('condition_definition_fingerprint', $commands[0]);
    }

    public function testV2HistoryAcceptsFormattingChangesAndKeepsItsIdentity(): void
    {
        $first = $this->load('return static fn (): bool => false;');
        $formatted = $this->load('return static fn ( ): bool => /* pending */ false ;');
        $fingerprint = ConditionWaitDefinition::fingerprint($first);

        self::assertSame($fingerprint, $this->replay($formatted, $fingerprint)[0]['condition_definition_fingerprint']);
    }

    public function testLegacyHistoryDoesNotGainFormattingToleranceRetroactively(): void
    {
        $first = $this->load('return static fn (): bool => false;');
        $formatted = $this->load('return static fn ( ): bool => /* pending */ false ;');
        $legacy = ConditionWaitDefinition::legacyFingerprint($first);

        $this->expectException(NonDeterministicWorkflow::class);

        $this->replay($formatted, $legacy);
    }

    public function testALaterAuthoredWaitWritesV2AfterAnEarlierLegacyWait(): void
    {
        $predicate = $this->load('return static fn (): bool => false;');
        $history = [
            ['event_type' => 'ConditionWaitOpened', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'first',
                'condition_key' => 'first', 'timeout_seconds' => 60,
                'condition_definition_fingerprint' => ConditionWaitDefinition::legacyFingerprint($predicate)]],
            ['event_type' => 'ConditionWaitTimedOut', 'payload' => ['sequence' => 1, 'condition_wait_id' => 'first']],
        ];
        $workflow = static function (WorkflowContext $context) use ($predicate): bool {
            $context->waitCondition($predicate, 'first', 60);

            return $context->waitCondition($predicate, 'second', 60);
        };
        $commands = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers')->commands;

        self::assertSame('second', $commands[0]['condition_key']);
        self::assertSame('php:condition-wait:1', $commands[0]['condition_wait_occurrence_id']);
        self::assertSame(ConditionWaitDefinition::fingerprint($predicate), $commands[0]['condition_definition_fingerprint']);
    }

    #[DataProvider('mismatchedFingerprints')]
    public function testLegacyV2AndExplicitMismatchesRemainStrict(string $recorded): void
    {
        $this->expectException(NonDeterministicWorkflow::class);

        $this->replay($this->load('return static fn (): bool => false;'), $recorded);
    }

    public static function mismatchedFingerprints(): iterable
    {
        yield 'legacy' => ['sha256:'.str_repeat('a', 64)];
        yield 'v2' => ['dwphp2:sha256:'.str_repeat('a', 64)];
        yield 'explicit' => ['sha256:different'];
        yield 'unknown version' => ['dwphp3:sha256:'.str_repeat('a', 64)];
    }

    /** @return list<array<string, mixed>> */
    private function replay(Closure $predicate, ?string $recorded): array
    {
        $payload = ['sequence' => 1, 'condition_wait_id' => 'wait-1', 'timeout_seconds' => 60,
            'condition_wait_occurrence_id' => 'php:condition-wait:0'];
        if ($recorded !== null) {
            $payload['condition_definition_fingerprint'] = $recorded;
        }

        return (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context): bool => $context->waitCondition($predicate, timeout: 60),
            [['event_type' => 'ConditionWaitOpened', 'payload' => $payload]], [], 'php-workers',
        )->commands;
    }

    private function load(string $source): mixed
    {
        $file = tempnam(sys_get_temp_dir(), 'condition-fingerprint-');
        self::assertIsString($file);
        $this->files[] = $file;
        file_put_contents($file, "<?php\n".$source."\n");

        return require $file;
    }
}
