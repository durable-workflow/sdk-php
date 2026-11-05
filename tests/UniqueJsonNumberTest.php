<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Transport\UniqueJson;
use JsonException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class UniqueJsonNumberTest extends TestCase
{
    public function testByteModeNumbersRetainNativeTypesAndUnicodeStrings(): void
    {
        $value = UniqueJson::decode('[12,-4,1.25,1e2,0,-0.0,"١１१"]');
        self::assertSame([12, -4, 1.25, 100.0, 0, -0.0, '١１१'], $value);
        self::assertSame(serialize(-0.0), serialize($value[5]));
    }

    #[DataProvider('invalidNumbers')]
    public function testUnicodeDigitsAndIncompleteNumbersRefuse(string $json): void
    {
        $this->expectException(JsonException::class);
        UniqueJson::decode($json);
    }

    /** @return iterable<string, array{string}> */
    public static function invalidNumbers(): iterable
    {
        foreach (['١', '１', '1١', '1.١', '1e١', '-١', '1e+１', '[1,١]', '{"n":1.१}',
            '01', '1.', '1e', '1e+', 'NaN', 'Infinity'] as $json) {
            yield $json => [$json];
        }
    }
}
