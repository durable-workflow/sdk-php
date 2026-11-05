<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Qualification\StaticCredentialProbe;
use DurableWorkflow\Qualification\StaticCredentialProbeFailure;
use DurableWorkflow\Qualification\StaticCredentialTransportSpy;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

require_once dirname(__DIR__).'/scripts/Support/StaticCredentialProbe.php';
require_once dirname(__DIR__).'/scripts/Support/StaticCredentialTransportSpy.php';

final class StaticCredentialProbeTest extends TestCase
{
    #[DataProvider('requestRoles')]
    public function testDefaultFrameworkTransportRefusesWithoutChangingAuthentication(bool $worker): void
    {
        $client = new Client('https://fixture.invalid', token: 'fixture-only');
        $authentication = StaticCredentialProbe::authentication($client);
        $spy = new StaticCredentialTransportSpy();
        $spy->attach($client);
        self::assertSame($authentication, StaticCredentialProbe::authentication($client));
        self::assertSame(0, $spy->attempts);
        $refused = false;
        try {
            if ($worker) {
                $client->registerWorker('fixture', 'fixture', [], []);
            } else {
                $client->clusterInfo();
            }
        } catch (Throwable) {
            $refused = true;
        }
        self::assertTrue($refused);
        self::assertSame(1, $spy->attempts, 'The real default HTTP path must reach the refusing spy.');
    }

    /** @return iterable<string, array{bool}> */
    public static function requestRoles(): iterable
    {
        yield 'control' => [false];
        yield 'worker' => [true];
    }

    public function testUnsupportedTransportCannotPassAsInstrumented(): void
    {
        $client = new Client('https://fixture.invalid', transport: new FakeTransport([]));
        $this->expectException(StaticCredentialProbeFailure::class);
        (new StaticCredentialTransportSpy())->attach($client);
    }

    public function testEnvironmentIsRestoredWhenConstructionRefuses(): void
    {
        $name = 'DURABLE_WORKFLOW_TOKEN';
        $previous = getenv($name);
        self::assertTrue(putenv($name.'=fixture-outside'));
        try {
            $this->expectException(StaticCredentialProbeFailure::class);
            StaticCredentialProbe::environment([$name => "fixture\xff"], static function () use ($name): void {
                self::assertSame("fixture\xff", getenv($name));
                throw new StaticCredentialProbeFailure('Fixture refusal.');
            });
        } finally {
            self::assertSame('fixture-outside', getenv($name));
            self::assertTrue(putenv($previous === false ? $name : $name.'='.$previous));
        }
    }

    public function testUnrelatedEnvironmentNamesAreRefused(): void
    {
        $this->expectException(StaticCredentialProbeFailure::class);
        StaticCredentialProbe::environment(['NOT_A_CREDENTIAL_SETTING' => 'fixture'], static fn (): bool => true);
    }

    #[DataProvider('configurationTargets')]
    public function testConstructionPathsPreserveOriginalBytesAndRefuseUnusedText(string $target): void
    {
        $spy = new StaticCredentialTransportSpy();
        $value = "fixture-e\u{0301}";
        self::assertSame(['Authorization' => 'Bearer '.$value],
            StaticCredentialProbe::headers($target, ['control' => $value], 'control', $spy));
        try {
            StaticCredentialProbe::headers($target, ['control' => $value, 'worker' => "unused\xff"], 'control', $spy);
            self::fail('The unused original text must refuse.');
        } catch (InvalidArgumentException $error) {
            self::assertSame('Invalid static credential setting credentials.worker_token.', $error->getMessage());
        }
        self::assertSame(0, $spy->attempts);
    }

    /** @return iterable<string, array{string}> */
    public static function configurationTargets(): iterable
    {
        foreach (['ServiceConfiguration', 'ServiceConfigurationConstructor', 'ServiceConfigurationAlias'] as $target) {
            yield $target => [$target];
        }
    }

    public function testUnknownPlanesDoNotFallBackToControl(): void
    {
        $this->expectException(StaticCredentialProbeFailure::class);
        StaticCredentialProbe::headers('Client', [], 'unsupported', new StaticCredentialTransportSpy());
    }
}
