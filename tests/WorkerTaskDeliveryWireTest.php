<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\PollTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker\TaskDelivery;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use ReflectionMethod;
use TypeError;

/** Execute the frozen wire cases through the PHP transport, without a Server. */
final class WorkerTaskDeliveryWireTest extends TestCase
{
    private const FIXTURE = __DIR__.'/fixtures/task-delivery-contract/acceptance-refusals.v1.json';

    public function testFrozenCompanionIdentity(): void
    {
        self::assertSame(74169, filesize(self::FIXTURE));
        self::assertSame('64a936382d4ce829d0e1de50e75247223711bf4011c830b257143b0a56c237ae', hash_file('sha256', self::FIXTURE));
        $data = self::fixture();
        self::assertCount(117, $data['wire_cases']);
        self::assertCount(34, $data['raw_wire_cases']);
        self::assertCount(24, $data['authority_cases']);
        $ids = array_column([...$data['wire_cases'], ...$data['raw_wire_cases'], ...$data['authority_cases']], 'id');
        self::assertCount(175, array_unique($ids));
    }

    /** @param array<string, mixed> $input */
    #[DataProvider('wireCases')]
    public function testTheOriginalWireReplyControlsOnlyItsSelectedAcceptance(array $input, string $action, string $endpoint, string $kind, bool $raw, bool $progress = false): void
    {
        $requests = [];
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use (&$requests, $input, $raw, $kind, $progress) {
            $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
            $requests[] = ['uri' => (string) $request->getUri(), 'headers' => $request->getHeaders(), 'body' => (string) $request->getBody()];
            $index = count($requests);
            if ($index === 1) { return Create::promiseFor(self::json(self::registration())); }
            if ($index === 2 || ($index === 4 && !isset($body['task_offer_token']) && str_ends_with((string) $request->getUri(), '/offers'))) {
                return Create::promiseFor(self::json(self::offer($kind)));
            }
            if ($index === 3) { return Create::promiseFor(self::reply($input, $raw, $progress)); }
            $reply = str_ends_with((string) $request->getUri(), '/poll') ? ['task' => null] : self::claimed($kind);
            return Create::promiseFor(self::json($reply));
        }]);
        $transport = new Psr18Transport(client: $http);
        $client = new Client('https://server.example', transport: self::withExceptionContext($transport, $input));
        $observed = 0;
        $client = $client->withWorkerPayloadObserver(static function (string $path, ?array $body, array $response) use (&$observed): ?Closure {
            if ($response['task'] ?? null) { ++$observed; }
            return null;
        });
        $client->registerWorker('worker', 'orders', [], []);
        $result = null;
        $error = null;
        try { $result = self::poll($client, $endpoint, 'original'); }
        catch (ServerException $caught) { $error = $caught; }
        self::assertSame(0, $observed, 'No refusal or uncertain outcome may start task execution.');
        if ($action === 'retain_selected_packet') {
            self::assertInstanceOf(TaskDeliveryUncertain::class, $error);
            self::assertSame(self::claimed($kind), self::poll(clone $client, $endpoint, 'original'));
            self::assertSame(1, $observed);
            self::assertCount(4, $requests);
            self::assertSame($requests[2], $requests[3], 'Retry must preserve the selected packet, kind set and request.');
            return;
        }
        self::assertNull($error, $error?->getMessage() ?? '');
        self::assertSame($action === 'resolve_selected_packet' ? 'empty' : 'stale_worker_registration', $result['poll_status']);
        self::poll($client, $endpoint, 'next');
        self::assertStringEndsWith('/'.$endpoint.'-tasks/'.($action === 'resolve_selected_packet' ? 'offers' : 'poll'), $requests[3]['uri']);
        self::assertArrayNotHasKey('task_offer_token', json_decode($requests[3]['body'], true, flags: JSON_THROW_ON_ERROR));
        self::assertCount($action === 'resolve_selected_packet' ? 5 : 4, $requests);
    }

    /** @return iterable<string, array{array<string, mixed>, string, string, string, bool, bool}> */
    public static function wireCases(): iterable
    {
        foreach (['wire_cases' => false, 'raw_wire_cases' => true] as $group => $raw) {
            foreach (self::fixture()[$group] as $case) {
                foreach ([['workflow', 'workflow'], ['workflow', 'update_validation'], ['activity', 'activity'], ['query', 'query'], ['update-validation', 'update_validation']] as [$endpoint, $kind]) {
                    foreach ($raw ? [false, true] : [false] as $progress) {
                        yield $case['id'].' / '.$endpoint.' / '.$kind.($progress ? ' / progress' : '') => [$case['input'], $case['expected_action'], $endpoint, $kind, $raw, $progress];
                    }
                }
            }
        }
    }

    /** @param array<string, mixed> $input */
    private static function reply(array $input, bool $raw, bool $progress): Response
    {
        if (isset($input['transport_failure'])) { throw new TransportException('Synthetic body read failure.'); }
        if ($raw) {
            $bytes = isset($input['raw_body_hex']) ? hex2bin($input['raw_body_hex']) : $input['raw_body_utf8'];
            if ($progress) {
                return new Response(200, ['Content-Type' => PollTransport::MEDIA_TYPE],
                    '{"schema":"durable-workflow.poll-transport.v1","status":'.$input['http_status'].',"headers":{},"body":'.$bytes.'}');
            }
            return new Response($input['http_status'], ['Content-Type' => 'application/json'], $bytes);
        }
        // The real progress decoder also exercises wrong-type logical statuses
        // that PSR-7's integer status constructor cannot represent directly.
        return new Response(200, ['Content-Type' => PollTransport::MEDIA_TYPE], json_encode([
            'schema' => 'durable-workflow.poll-transport.v1', 'status' => $input['http_status'],
            'headers' => (object) [], 'body' => $input['decoded_body'],
        ], JSON_THROW_ON_ERROR));
    }

    /** @param array<string, mixed> $input */
    private static function withExceptionContext(Psr18Transport $transport, array $input): Transport
    {
        return new class($transport, $input) implements Transport {
            /** @param array<string, mixed> $input */
            public function __construct(private Psr18Transport $transport, private array $input) {}

            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                try { return $this->transport->send($method, $uri, $headers, $body); }
                catch (TransportException $error) {
                    $input = $this->input;
                    if (!array_key_exists('exception_http_status', $input) && !array_key_exists('exception_reason', $input)) { throw $error; }
                    $status = array_key_exists('exception_http_status', $input) ? $input['exception_http_status'] : $error->status;
                    $reason = array_key_exists('exception_reason', $input) ? $input['exception_reason'] : ($error->response['reason'] ?? null);
                    try { $context = new ServerException('Synthetic typed exception context.', $status, $reason, $error->response, $error); }
                    // Invalid types cannot become typed SDK exception fields.
                    // Treat that construction failure as unreadable, without coercion.
                    catch (TypeError $typeError) { throw new TransportException('Unreadable exception context.', previous: $typeError); }
                    throw $context;
                }
            }
        };
    }

    /** @return array<string, mixed> */
    private static function fixture(): array { return json_decode((string) file_get_contents(self::FIXTURE), true, flags: JSON_THROW_ON_ERROR); }

    /** @return array<string, mixed> */
    private static function poll(Client $client, string $endpoint, string $poll): array
    {
        return (new ReflectionMethod(Client::class, 'pollTaskResponse'))->invoke($client, '/worker/'.$endpoint.'-tasks/poll', [
            'worker_id' => 'worker', 'task_queue' => 'orders', 'timeout_seconds' => 4, 'poll_request_id' => $poll,
            'task_kinds' => $endpoint === 'workflow' ? ['workflow', 'update_validation'] : [str_replace('-', '_', $endpoint)],
        ]);
    }

    /** @param array<string, mixed> $body */
    private static function json(array $body): Response { return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR)); }

    /** @return array<string, mixed> */
    private static function registration(): array
    {
        return ['registered' => true, 'registration_token' => str_repeat('a', 32), 'server_capabilities' => ['task_delivery' => [
            'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
            'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER, 'version' => TaskDelivery::VERSION,
            'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2, 'unaccepted_offer_reserves_task' => false,
        ]]];
    }

    /** @return array<string, mixed> */
    private static function offer(string $kind): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => $kind, 'task_id' => 'selected-task', 'token' => 'synthetic-packet', 'expires_at_ms' => 1000,
        ]];
    }

    /** @return array<string, mixed> */
    private static function claimed(string $kind): array
    {
        $field = match ($kind) { 'query' => 'query_task_id', 'update_validation' => 'update_validation_task_id', default => 'task_id' };
        return ['task' => [$field => 'selected-task', 'task_kind' => $kind, 'lease_owner' => 'worker']];
    }
}
