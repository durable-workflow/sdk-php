<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ClientBaseUriTest extends TestCase
{
    public function testClientRejectsEveryNonAvroPayloadCodecConfiguration(): void
    {
        $codec = new class() implements PayloadCodec {
            public function name(): string { return 'json'; }
            public function encode(mixed $value): string { return json_encode($value, JSON_THROW_ON_ERROR); }
            public function decode(string $blob): mixed { return json_decode($blob, true, flags: JSON_THROW_ON_ERROR); }
            public function envelope(mixed $value): array { return ['codec' => 'json', 'blob' => $this->encode($value)]; }
            public function decodeEnvelope(array|string|null $envelope): mixed { return null; }
        };

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('unsupported_payload_codec');
        $this->expectExceptionMessage('HTTP document transport');

        new Client('https://server.example', codec: $codec);
    }

    public function testClientRejectsTheSdkOwnedApiSuffix(): void
    {
        foreach ([
            'http://127.0.0.1:8080/api',
            'http://localhost:8080/api/',
            'https://api/api',
            'https://runtime.example.test/namespaces/orders/api',
            'https://runtime.example.test/namespaces/orders/api/',
        ] as $baseUri) {
            try {
                new Client($baseUri);
                self::fail("SDK-owned /api suffix must be rejected for {$baseUri}");
            } catch (InvalidArgumentException $exception) {
                self::assertStringContainsString('omit the SDK-owned /api suffix', $exception->getMessage());
                self::assertStringContainsString('SDK appends /api automatically', $exception->getMessage());
            }
        }
    }

    public function testClientPreservesSelfHostedAndManagedRuntimePrefixes(): void
    {
        foreach ([
            'http://127.0.0.1:8080' => 'http://127.0.0.1:8080/api/health',
            'http://localhost:8080/' => 'http://localhost:8080/api/health',
            'http://localhost:8080/durable-workflow/' => 'http://localhost:8080/durable-workflow/api/health',
            'https://runtime.example.test/namespaces/orders' => 'https://runtime.example.test/namespaces/orders/api/health',
            'https://cloud.example.test/api/runtime/v1/namespaces/orders/' => 'https://cloud.example.test/api/runtime/v1/namespaces/orders/api/health',
            'https://api.example.test/runtime/orders/' => 'https://api.example.test/runtime/orders/api/health',
        ] as $baseUri => $expectedUri) {
            $transport = new FakeTransport([[]]);
            $client = new Client($baseUri, transport: $transport);

            $client->health();

            self::assertSame($expectedUri, $transport->requests[0]['uri']);
        }
    }

    public function testClientPreservesBareApiHostname(): void
    {
        foreach (['https://api', 'https://api/'] as $baseUri) {
            $transport = new FakeTransport([[]]);
            $client = new Client($baseUri, transport: $transport);

            $client->health();

            self::assertSame('https://api/api/health', $transport->requests[0]['uri']);
        }
    }
}
