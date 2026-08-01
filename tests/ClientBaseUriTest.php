<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ClientBaseUriTest extends TestCase
{
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
            'https://runtime.example.test/gateway/api/namespaces/orders/' => 'https://runtime.example.test/gateway/api/namespaces/orders/api/health',
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
