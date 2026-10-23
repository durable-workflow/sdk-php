<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Model\ScheduleAction;
use DurableWorkflow\Model\ScheduleSpec;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class TransportJsonNumericKindTest extends TestCase
{
    #[DataProvider('metadataOperations')]
    public function testRawHttpMetadataPreservesNumericKindsAndAvroInput(string $operation): void
    {
        $http = new class implements ClientInterface {
            /** @var list<RequestInterface> */
            public array $requests = [];

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;

                return new Response(200, ['Content-Type' => 'application/json'], '{}');
            }
        };
        $transport = new Psr18Transport(client: $http);
        $client = new Client('https://recording.invalid', transport: $transport);
        $values = ['whole' => 1.0, 'positive_zero' => 0.0, 'negative_zero' => -0.0,
            'integer' => 1, 'integer_zero' => 0, 'max' => PHP_INT_MAX, 'fraction' => 1.25,
            'nested' => ['whole' => 2.0, 'negative_zero' => -0.0, 'list' => [3.0, -0.0, 3]],
            'text' => '雪/1.0', 'boolean' => true, 'null' => null];
        if ($operation === 'start') {
            $client->startWorkflow('numeric-kind', 'numeric-kind', 'numeric-kind', [$values],
                memo: $values, searchAttributes: $values);
        } elseif ($operation === 'schedule') {
            $client->createSchedule(new ScheduleSpec(cronExpressions: ['0 * * * *']),
                new ScheduleAction('numeric-kind', 'numeric-kind', [$values]), 'numeric-kind',
                memo: $values, searchAttributes: $values);
        } else {
            $transport->send('POST', 'https://recording.invalid/api/'.$operation,
                $operation === 'worker' ? ['X-Durable-Workflow-Protocol-Version' => '1.20'] : [],
                ['memo' => $values, 'search_attributes' => $values,
                    'input' => $client->payloadCodec()->envelope([$values])]);
        }
        $request = $http->requests[array_key_last($http->requests)];
        $path = ['start' => 'workflows', 'schedule' => 'schedules'][$operation] ?? $operation;
        self::assertSame('/api/'.$path, $request->getUri()->getPath());
        $wire = (string) $request->getBody();
        self::assertStringContainsString('"whole":1.0', $wire);
        self::assertStringContainsString('"negative_zero":-0.0', $wire);
        self::assertStringContainsString('"list":[3.0,-0.0,3]', $wire);
        $decoded = json_decode($wire, true, flags: JSON_THROW_ON_ERROR);
        foreach (['memo', 'search_attributes'] as $field) {
            self::assertSame($values, $decoded[$field]);
            self::assertSame('0000000000000000', bin2hex(pack('E', $decoded[$field]['positive_zero'])));
            self::assertSame('8000000000000000', bin2hex(pack('E', $decoded[$field]['negative_zero'])));
            self::assertSame('8000000000000000', bin2hex(pack('E', $decoded[$field]['nested']['list'][1])));
        }
        $envelope = $operation === 'schedule' ? $decoded['action']['input'] : $decoded['input'];
        self::assertSame($client->payloadCodec()->envelope([$values]), $envelope);
        $input = $client->payloadCodec()->decodeEnvelope($envelope);
        self::assertSame([$values], $input);
        self::assertSame('8000000000000000', bin2hex(pack('E', $input[0]['negative_zero'])));
    }

    public static function metadataOperations(): iterable
    {
        yield 'workflow start' => ['start'];
        yield 'schedule create' => ['schedule'];
        yield 'generic control JSON' => ['control'];
        yield 'generic worker JSON' => ['worker'];
    }
}
