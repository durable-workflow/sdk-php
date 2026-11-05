<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\StaticCredentialFixture;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

require_once dirname(__DIR__).'/scripts/Support/StaticCredentialFixture.php';

final class StaticCredentialFixtureTest extends TestCase
{
    public function testEveryScalarLoadsItsOriginalBytes(): void
    {
        $fixture = new StaticCredentialFixture(self::path());
        self::assertCount(319, $fixture->ids());
        self::assertCount(319, array_unique($fixture->ids()));
        $inventory = hash_init('sha256');
        $count = 0;
        foreach (['common_scalar_cases', 'php_byte_loader_cases'] as $group) {
            foreach ($fixture->data[$group] as $case) {
                $value = $fixture->value($case['id']);
                if (isset($case['input_hex']) || isset($case['expected_utf8_hex'])) {
                    self::assertSame($case['input_hex'] ?? $case['expected_utf8_hex'], bin2hex($value), $case['id']);
                }
                hash_update($inventory, pack('N', strlen($case['id'])).$case['id'].pack('N', strlen($value)).$value);
                ++$count;
            }
        }
        // Independently frozen with Python chr()/UTF-8 and the fixture's original input_hex bytes.
        self::assertSame(296, $count);
        self::assertSame('b7b61969bea23a2dfad8a85e637e3c592b3dad6f62047a39a04874227e18d0ed', hash_final($inventory));
        self::assertNotSame($fixture->value('scalar-composed'), $fixture->value('scalar-decomposed'));
        self::assertSame('', $fixture->value('scalar-literal-empty'));
    }

    public function testAChangedFixtureCannotSupplyExpectations(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'sdk-php-credential-fixture-');
        self::assertIsString($path);
        try {
            $bytes = (string) file_get_contents(self::path());
            self::assertSame(strlen($bytes), file_put_contents($path, str_replace('scalar-ordinary', 'scalar-mutation', $bytes)));
            $this->expectException(InvalidArgumentException::class);
            new StaticCredentialFixture($path);
        } finally {
            unlink($path);
        }
    }

    public function testServerOnlyRowsCannotBecomePhpScalarCases(): void
    {
        $fixture = new StaticCredentialFixture(self::path());
        $this->expectException(InvalidArgumentException::class);
        $fixture->scalar('server-unused-role');
    }

    /** @param list<mixed> $points */
    #[DataProvider('nonScalarPoints')]
    public function testNonScalarPointsRefuse(array $points): void
    {
        $this->expectException(InvalidArgumentException::class);
        StaticCredentialFixture::points($points);
    }

    /** @return iterable<string, array{list<mixed>}> */
    public static function nonScalarPoints(): iterable
    {
        yield 'negative' => [[-1]];
        yield 'surrogate' => [[0xd800]];
        yield 'above limit' => [[0x110000]];
        yield 'string' => [['32']];
    }

    private static function path(): string
    {
        return __DIR__.'/fixtures/auth-configuration/static-credentials-v1.json';
    }
}
