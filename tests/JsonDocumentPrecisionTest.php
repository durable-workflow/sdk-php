<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Transport\JsonDocument;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

/** Request documents keep float precision whatever the host's serialize_precision is (#248). */
final class JsonDocumentPrecisionTest extends TestCase
{
    #[RunInSeparateProcess]
    public function testAFloatSearchAttributeRoundTripsUnderAReducedSerializePrecision(): void
    {
        ini_set('serialize_precision', '3');
        $command = ['type' => 'upsert_search_attributes', 'attributes' => ['Ratio' => 1.2345, 'Tiny' => 0.1 + 0.2, 'Whole' => 2.0]];

        $encoded = JsonDocument::encode($command);

        self::assertSame($command, json_decode($encoded, true, flags: JSON_THROW_ON_ERROR));
        self::assertStringContainsString('"Ratio":1.2345', $encoded);
        self::assertStringContainsString('"Whole":2.0', $encoded);
        self::assertSame('3', ini_get('serialize_precision'), 'the host setting is restored');
    }

    /** Hash inputs keep the caller's flags and are byte-identical under the default precision (#282). */
    public function testEncodeWithMatchesJsonEncodeUnderTheDefaultPrecision(): void
    {
        foreach (self::hashInputs() as $value) {
            foreach ([JSON_THROW_ON_ERROR, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE] as $flags) {
                self::assertSame(json_encode($value, $flags), JsonDocument::encodeWith($value, $flags));
            }
        }
    }

    #[RunInSeparateProcess]
    public function testAHashOverFloatsDoesNotDependOnSerializePrecision(): void
    {
        $default = array_map(static fn (mixed $value): string => hash('sha256', JsonDocument::encodeWith($value, JSON_THROW_ON_ERROR)), self::hashInputs());

        ini_set('serialize_precision', '3');
        $reduced = array_map(static fn (mixed $value): string => hash('sha256', JsonDocument::encodeWith($value, JSON_THROW_ON_ERROR)), self::hashInputs());

        self::assertSame($default, $reduced);
        self::assertSame(json_encode(1.2345), json_encode(1.2344), 'plain json_encode() collides at this precision');
        self::assertSame('3', ini_get('serialize_precision'));
    }

    /** @return list<mixed> */
    private static function hashInputs(): array
    {
        return [
            ['task-1', 'owner', 3, 7, [['type' => 'upsert_search_attributes', 'attributes' => ['Ratio' => 1.2345, 'Score' => 0.1 + 0.2]]]],
            [1.0, 2.5, -0.0, 1e-7, 123456789.123456789, 'text/with/slashes', 'ünicode'],
            ['plain' => 'strings', 'count' => 42, 'flag' => true, 'none' => null],
        ];
    }

    #[RunInSeparateProcess]
    public function testTheRequestBodySentToTheServerKeepsFullPrecision(): void
    {
        ini_set('serialize_precision', '3');
        $sent = new class { public string $body = ''; };
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use ($sent) {
            $sent->body = (string) $request->getBody();

            return Create::promiseFor(new Response(200, ['Content-Type' => 'application/json'], '{}'));
        }]);

        (new Psr18Transport($http))->send('POST', 'https://server.example/api/worker/workflow-tasks/task-1/complete', [], [
            'commands' => [['type' => 'upsert_search_attributes', 'attributes' => ['Ratio' => 1.2345]]],
        ]);

        self::assertStringContainsString('"Ratio":1.2345', $sent->body);
    }
}
