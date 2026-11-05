<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Qualification\StaticCredentialProbeFailure;
use DurableWorkflow\Qualification\StaticCredentialSubject;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

require_once dirname(__DIR__).'/scripts/Support/StaticCredentialSubject.php';

/** Inert input/refusal controls; no consumer autoload, solve or installed-subject success. */
final class StaticCredentialSubjectTest extends TestCase
{
    public function testDependencyIntentHasExactVersionsAndFutureInputs(): void
    {
        $intent = StaticCredentialSubject::intent(self::intentPath());
        self::assertSame('8.4.27', $intent['requirements']['php']);
        self::assertSame('3.10.0', $intent['requirements']['durable-workflow/sdk']);
        self::assertCount(3, $intent['future_inputs']);
        self::assertSame(['plugins' => false, 'scripts' => false, 'advisory_policy' => 'normal'], $intent['dependency_policy']);
    }

    public function testAnUnclosedTemplateCannotReachConsumerCode(): void
    {
        $path = __DIR__.'/fixtures/static-credential-consumer/subject-template.json';
        $this->expectException(StaticCredentialProbeFailure::class);
        $this->expectExceptionMessage('A future immutable reference is missing.');
        StaticCredentialSubject::read(__DIR__.'/fixtures/static-credential-consumer/autoload.php', self::intentPath(),
            $path, (string) hash_file('sha256', $path));
    }

    public function testAnAlreadyLoadedSourceClientCannotBeAnInstalledCliSubject(): void
    {
        self::assertTrue(class_exists(Client::class));
        $this->expectException(StaticCredentialProbeFailure::class);
        StaticCredentialSubject::assertFreshProcess();
    }

    #[DataProvider('changedInputs')]
    public function testRawAndUnclosedSubjectInputsRefuseBeforeAutoload(string $kind): void
    {
        $bytes = (string) file_get_contents(__DIR__.'/fixtures/static-credential-consumer/subject-template.json');
        $bytes = match ($kind) {
            'duplicate' => str_replace('    "runner_reference": null,', '    "runner_reference": null,' ."\n".'    "runner_reference": null,', $bytes),
            'escaped duplicate' => str_replace('    "runner_reference": null,', '    "runner_reference": null,' ."\n".'    "runner_\u0072eference": null,', $bytes),
            'trailing' => $bytes.'{}',
            'invalid UTF8' => $bytes."\xff",
            'extra field' => str_replace('    "runner_reference": null,', '    "unknown": "fixture-only",' ."\n".'    "runner_reference": null,', $bytes),
            'hash mismatch' => $bytes,
            default => throw new \LogicException('Unknown source control.'),
        };
        $path = tempnam(sys_get_temp_dir(), 'sdk-php-credential-subject-');
        self::assertIsString($path);
        try {
            self::assertSame(strlen($bytes), file_put_contents($path, $bytes));
            $hash = $kind === 'hash mismatch' ? str_repeat('0', 64) : hash('sha256', $bytes);
            $refused = null;
            try {
                StaticCredentialSubject::read(__DIR__.'/fixtures/static-credential-consumer/autoload.php', self::intentPath(), $path, $hash);
            } catch (Throwable $error) {
                $refused = $error;
            }
            self::assertInstanceOf(Throwable::class, $refused);
        } finally {
            unlink($path);
        }
    }

    /** @return iterable<string, array{string}> */
    public static function changedInputs(): iterable
    {
        foreach (['duplicate', 'escaped duplicate', 'trailing', 'invalid UTF8', 'extra field', 'hash mismatch'] as $kind) {
            yield $kind => [$kind];
        }
    }

    public function testRangesAndAdvisoryPolicyChangesAreNotTheSelectedIntent(): void
    {
        $intent = json_decode((string) file_get_contents(self::intentPath()), true, flags: JSON_THROW_ON_ERROR);
        foreach (['range', 'plugins', 'scripts', 'advisories'] as $change) {
            $candidate = $intent;
            if ($change === 'range') {
                $candidate['requirements']['durable-workflow/sdk'] = '^3.10';
            } else {
                $key = $change === 'advisories' ? 'advisory_policy' : $change;
                $candidate['dependency_policy'][$key] = true;
            }
            $path = tempnam(sys_get_temp_dir(), 'sdk-php-credential-intent-');
            self::assertIsString($path);
            try {
                file_put_contents($path, json_encode($candidate, JSON_THROW_ON_ERROR));
                $refused = false;
                try { StaticCredentialSubject::intent($path); }
                catch (StaticCredentialProbeFailure) { $refused = true; }
                self::assertTrue($refused);
            } finally {
                unlink($path);
            }
        }
    }

    private static function intentPath(): string
    {
        return __DIR__.'/fixtures/static-credential-consumer/dependency-intent.json';
    }
}
