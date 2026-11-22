<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker\TaskDelivery;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/** PHP source controls for the original logical request and selected receipt. */
final class WorkerTaskDeliveryKindsTest extends TestCase
{
    private const FIXTURE = __DIR__.'/fixtures/task-delivery-contract/offer-accept.v1.json';
    private const BOTH = ['update_validation', 'workflow'];

    public function testReviewedContractIdentityAndKindScope(): void
    {
        self::assertSame('cac2bc6852a09db9526c623c3683772ebbba7d3f1d0b401fb92ae2026cd32b11', hash_file('sha256', self::FIXTURE));
        $data = json_decode((string) file_get_contents(self::FIXTURE), true, flags: JSON_THROW_ON_ERROR);
        self::assertCount(49, $data['cases']);
        self::assertCount(49, array_unique(array_column($data['cases'], 'id')));
        self::assertSame(['kind-workflow', 'kind-validator', 'kind-mixed-order', 'kind-unrequested', 'kind-accepted-substitution'],
            array_values(array_filter(array_column($data['cases'], 'id'), static fn (string $id): bool => str_starts_with($id, 'kind-'))));
    }

    /** @param list<string>|null $kinds */
    #[DataProvider('validKinds')]
    public function testOnlyRequestedKindsCanProduceTheirSelectedReceipt(?array $kinds, string $selected, string $endpoint): void
    {
        $transport = new FakeTransport([self::registration(), self::offer($selected), self::claimed($selected)]);
        $client = self::client($transport);
        [$result, $error] = self::capture(static fn (): array => self::poll($client, $kinds, $endpoint));
        self::assertNull($error, $error?->getMessage() ?? '');
        self::assertSame(self::claimed($selected), $result);
        self::assertCount(3, $transport->requests);
        $offer = $transport->requests[1];
        $accept = $transport->requests[2];
        self::assertStringEndsWith('/'.$endpoint.'-tasks/offers', $offer['uri']);
        self::assertSame($offer['uri'], $accept['uri']);
        $originalBody = $offer['body'];
        self::assertIsArray($originalBody);
        self::assertSame(array_replace($originalBody, ['timeout_seconds' => 0, 'task_offer_token' => 'selected-packet']), $accept['body']);
        if ($kinds !== null) {
            self::assertSame(in_array('workflow', $kinds, true) && in_array('update_validation', $kinds, true) ? self::BOTH : $kinds,
                $offer['body']['task_kinds']);
        }
    }

    /** @return iterable<string, array{list<string>|null, string, string}> */
    public static function validKinds(): iterable
    {
        $data = json_decode((string) file_get_contents(self::FIXTURE), true, flags: JSON_THROW_ON_ERROR);
        foreach ($data['cases'] as $case) {
            if (in_array($case['id'], ['kind-workflow', 'kind-validator', 'kind-mixed-order'], true)) {
                foreach ($case['request_kind_orders'] ?? [$case['requested_kinds']] as $index => $kinds) {
                    yield $case['id'].' / '.$index => [$kinds, $case['offered_kind'], $case['endpoint']];
                }
            }
        }
        yield 'mixed workflow / order 1' => [self::BOTH, 'workflow', 'workflow'];
        yield 'mixed workflow / order 2' => [['workflow', 'update_validation'], 'workflow', 'workflow'];
        yield 'normalized duplicate kinds' => [['workflow', 'update_validation', 'workflow'], 'update_validation', 'workflow'];
        yield 'default workflow set' => [null, 'workflow', 'workflow'];
        foreach (['activity', 'query', 'update-validation'] as $endpoint) {
            $kind = str_replace('-', '_', $endpoint);
            yield 'single-kind '.$endpoint => [[$kind], $kind, $endpoint];
        }
    }

    /** @param list<string> $kinds */
    #[DataProvider('unrequestedKinds')]
    public function testAnUnrequestedOfferSendsNoAcceptance(array $kinds, string $offered, string $endpoint): void
    {
        $transport = new FakeTransport([self::registration(), self::offer($offered), self::claimed($offered)]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::poll($client, $kinds, $endpoint));
        self::assertInstanceOf(ServerException::class, $error);
        self::assertSame('invalid_task_delivery_offer', $error->reason);
        self::assertCount(2, $transport->requests);
    }

    /** @return iterable<string, array{list<string>, string, string}> */
    public static function unrequestedKinds(): iterable
    {
        yield 'kind-unrequested' => [['workflow'], 'update_validation', 'workflow'];
        yield 'validator-only cannot select workflow' => [['update_validation'], 'workflow', 'workflow'];
        foreach (['activity', 'query', 'update-validation'] as $endpoint) {
            yield $endpoint => [[str_replace('-', '_', $endpoint)], 'workflow', $endpoint];
        }
    }

    #[DataProvider('receiptMutations')]
    public function testSubstitutedOrMissingReceiptIdentityCannotReleaseThePacket(string $kind, string $mutation): void
    {
        $bad = self::claimed($kind);
        $field = $kind === 'update_validation' ? 'update_validation_task_id' : 'task_id';
        if ($mutation === 'kind-substitution') {
            $bad['task']['task_kind'] = $kind === 'workflow' ? 'update_validation' : 'workflow';
            $bad['task']['task_id'] = $bad['task']['update_validation_task_id'] = 'selected-task';
        } elseif ($mutation === 'missing-kind') {
            unset($bad['task']['task_kind']);
        } elseif ($mutation === 'null-kind') {
            $bad['task']['task_kind'] = null;
        } elseif ($mutation === 'task-substitution') {
            $bad['task'][$field] = 'other-task';
        } else {
            unset($bad['task'][$field]);
            $bad['task'][$field === 'task_id' ? 'update_validation_task_id' : 'task_id'] = 'selected-task';
        }
        $transport = new FakeTransport([self::registration(), self::offer($kind), $bad, self::claimed($kind)]);
        $observed = 0;
        $client = self::client($transport)->withWorkerPayloadObserver(
            static function (string $path, ?array $body, array $response) use (&$observed): ?Closure {
                if ($response['task'] ?? null) { ++$observed; }
                return null;
            });
        [, $error] = self::capture(static fn (): array => self::poll($client, self::BOTH));
        self::assertInstanceOf(TaskDeliveryUncertain::class, $error);
        self::assertSame(0, $observed);
        self::assertSame(self::claimed($kind), self::poll(clone $client, ['workflow', 'update_validation']));
        self::assertSame(1, $observed);
        self::assertCount(4, $transport->requests);
        self::assertSame($transport->requests[2], $transport->requests[3]);
    }

    /** @return iterable<string, array{string, string}> */
    public static function receiptMutations(): iterable
    {
        foreach (['workflow', 'update_validation'] as $kind) {
            foreach (['kind-substitution', 'missing-kind', 'null-kind', 'task-substitution', 'wrong-id-field'] as $mutation) {
                yield $kind.' / '.$mutation => [$kind, $mutation];
            }
        }
    }

    #[DataProvider('selectedKinds')]
    public function testChangedKindsCannotReplaceAnUncertainOriginalRequest(string $kind): void
    {
        $transport = new FakeTransport([self::registration(), self::offer($kind), new TransportException('Timeout.', 504), self::claimed($kind)]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::poll($client, self::BOTH));
        self::assertInstanceOf(TaskDeliveryUncertain::class, $error);
        $clone = clone $client;
        [, $error] = self::capture(static fn (): array => self::poll($clone, ['workflow']));
        self::assertInstanceOf(InvalidArgumentException::class, $error);
        self::assertCount(3, $transport->requests);
        self::assertSame(self::claimed($kind), self::poll($clone, ['workflow', 'update_validation', 'workflow']));
        self::assertSame($transport->requests[2], $transport->requests[3]);
    }

    /** @return array<array{string}> */
    public static function selectedKinds(): array { return [['workflow'], ['update_validation']]; }

    public function testCallerReferencesCannotMutateTheSelectedKindSetOrRetryBytes(): void
    {
        $kind = 'workflow';
        $requested = [&$kind, 'update_validation'];
        $transport = new FakeTransport([self::registration(), self::offer('workflow'),
            new TransportException('Timeout.', 504), self::claimed('workflow')]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::poll($client, $requested));
        self::assertInstanceOf(TaskDeliveryUncertain::class, $error);
        $originalPacket = json_encode($transport->requests[2], JSON_THROW_ON_ERROR);
        $kind = 'activity';
        [$result, $error] = self::capture(static fn (): array => self::poll(clone $client, self::BOTH));
        self::assertNull($error, $error?->getMessage() ?? '');
        self::assertSame(self::claimed('workflow'), $result);
        self::assertSame($originalPacket, json_encode($transport->requests[3], JSON_THROW_ON_ERROR));
    }

    #[DataProvider('concurrentKinds')]
    public function testKindSetBindingSpansOfferAndAcceptanceOwnership(string $kind, string $phase, bool $clone, bool $changed): void
    {
        $offers = $accepts = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $kind, $phase, &$offers, &$accepts,
        ): array {
            if (str_ends_with($uri, '/worker/register')) { return self::registration(); }
            if (isset($body['task_offer_token'])) {
                ++$accepts;
                if ($phase === 'accept') { \Fiber::suspend('accept'); }
                return self::claimed($kind);
            }
            ++$offers;
            if ($phase === 'offer') { \Fiber::suspend('offer'); }
            return self::offer($kind);
        });
        $client = self::client($transport);
        $owner = new \Fiber(static fn (): array => self::poll($client, self::BOTH));
        try { $paused = $owner->start(); }
        catch (ServerException $error) { self::fail('The valid owner was refused before its pause: '.$error->reason); }
        self::assertSame($phase, $paused);
        $other = $clone ? clone $client : $client;
        [$result, $error] = self::capture(static fn (): array => self::poll($other, $changed ? ['workflow'] : ['workflow', 'update_validation']));
        try { $owner->resume(); }
        catch (ServerException $error) { self::fail('The valid owner was refused after its pause: '.$error->reason); }
        self::assertSame(self::claimed($kind), $owner->getReturn());
        if ($changed) {
            self::assertInstanceOf(InvalidArgumentException::class, $error);
        } else {
            self::assertNull($error);
            self::assertSame(['task' => null], $result);
        }
        self::assertSame(1, $offers);
        self::assertSame(1, $accepts);
        self::assertCount(3, $transport->requests);
    }

    /** @return iterable<string, array{string, string, bool, bool}> */
    public static function concurrentKinds(): iterable
    {
        foreach (['workflow', 'update_validation'] as $kind) {
            foreach (['offer', 'accept'] as $phase) {
                foreach ([false, true] as $clone) {
                    foreach ([false, true] as $changed) {
                        yield $kind.' / '.$phase.' / '.(int) $clone.' / '.(int) $changed => [$kind, $phase, $clone, $changed];
                    }
                }
            }
        }
    }

    #[DataProvider('invalidKindSets')]
    public function testMalformedKindSetsRefuseBeforeAnOffer(mixed $kinds, string $endpoint): void
    {
        $transport = new FakeTransport([self::registration(), ['task' => null]]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::rawPoll($client, ['task_kinds' => $kinds], $endpoint));
        self::assertInstanceOf(InvalidArgumentException::class, $error);
        self::assertCount(1, $transport->requests);
    }

    /** @return iterable<string, array{mixed, string}> */
    public static function invalidKindSets(): iterable
    {
        foreach ([null, '', [], ['kind' => 'workflow'], [1], ['activity'], ['update-validation'], ['workflow', 'foreign']] as $index => $kinds) {
            yield (string) $index => [$kinds, 'workflow'];
        }
        foreach (['activity', 'query', 'update-validation', 'unknown'] as $endpoint) {
            yield $endpoint => [['workflow'], $endpoint];
        }
    }

    /** @param TransportException|array<string, mixed>|null $outcome */
    #[DataProvider('uncertainKinds')]
    public function testUncertainMixedRepliesRetainTheExactOriginalPacket(string $kind, TransportException|array|null $outcome): void
    {
        $transport = new FakeTransport([self::registration(), self::offer($kind), $outcome, self::claimed($kind)]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::poll($client, self::BOTH));
        self::assertInstanceOf(TaskDeliveryUncertain::class, $error);
        self::assertSame(self::claimed($kind), self::poll(clone $client, ['workflow', 'update_validation']));
        self::assertCount(4, $transport->requests);
        self::assertSame($transport->requests[2], $transport->requests[3]);
        self::assertStringEndsWith('/workflow-tasks/offers', $transport->requests[3]['uri']);
    }

    /** @return iterable<string, array{string, TransportException|array<string, mixed>|null}> */
    public static function uncertainKinds(): iterable
    {
        foreach (['workflow', 'update_validation'] as $kind) {
            foreach (WorkerTaskDeliveryTest::uncertainOutcomes() as $name => [$outcome]) {
                yield $kind.' / '.$name => [$kind, $outcome];
            }
        }
    }

    public function testAnExecutableTaskWithoutASelectedOfferCannotBypassKindBinding(): void
    {
        $transport = new FakeTransport([self::registration(), self::claimed('workflow')]);
        $client = self::client($transport);
        [, $error] = self::capture(static fn (): array => self::poll($client, ['workflow']));
        self::assertInstanceOf(ServerException::class, $error);
        self::assertSame('invalid_task_delivery_response', $error->reason);
        self::assertCount(2, $transport->requests);
    }

    /**
     * @param list<string>|null $kinds
     * @return array<string, mixed>
     */
    private static function poll(Client $client, ?array $kinds, string $endpoint = 'workflow'): array
    {
        return self::rawPoll($client, $kinds === null ? [] : ['task_kinds' => $kinds], $endpoint);
    }

    /**
     * @param array<string, mixed> $extra
     * @return array<string, mixed>
     */
    private static function rawPoll(Client $client, array $extra, string $endpoint = 'workflow'): array
    {
        // Exercise the same logical-request boundary used by the public poll
        // methods, without adding update-validator authoring to this source slice.
        return (new ReflectionMethod(Client::class, 'pollTaskResponse'))->invoke($client, '/worker/'.$endpoint.'-tasks/poll', [
            'worker_id' => 'worker', 'task_queue' => 'orders', 'poll_request_id' => 'kind-poll', 'timeout_seconds' => 4,
            ...$extra,
        ]);
    }

    /** @return array{array<string, mixed>|null, ServerException|InvalidArgumentException|null} */
    private static function capture(Closure $operation): array
    {
        try { return [$operation(), null]; }
        catch (ServerException|InvalidArgumentException $error) { return [null, $error]; }
    }

    private static function client(FakeTransport $transport): Client
    {
        $client = new Client('https://server.example', transport: $transport);
        $client->registerWorker('worker', 'orders', [], []);
        return $client;
    }

    /** @return array<string, mixed> */
    private static function registration(): array
    {
        return ['registered' => true, 'registration_token' => str_repeat('a', 32), 'server_capabilities' => ['task_delivery' => [
            'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
            'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER,
            'version' => TaskDelivery::VERSION, 'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2,
            'unaccepted_offer_reserves_task' => false,
        ]]];
    }

    /** @return array<string, mixed> */
    private static function offer(string $kind): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => $kind, 'task_id' => 'selected-task',
            'token' => 'selected-packet', 'expires_at_ms' => 1000,
        ]];
    }

    /** @return array<string, mixed> */
    private static function claimed(string $kind): array
    {
        $field = match ($kind) { 'query' => 'query_task_id', 'update_validation' => 'update_validation_task_id', default => 'task_id' };
        return ['task' => [$field => 'selected-task', 'lease_owner' => 'worker', 'task_kind' => $kind]];
    }
}
