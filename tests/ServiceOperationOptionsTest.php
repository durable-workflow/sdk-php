<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ServiceOperationOptionsTest extends TestCase
{
    #[DataProvider('namespaces')]
    public function testTypedAndArrayTargetsProduceTheSameDurableCommand(?string $namespace): void
    {
        $typed = new ServiceOperationOptions(targetNamespace: $namespace);
        $array = $namespace === null ? [] : ['namespace' => $namespace];
        $first = self::replay($typed);
        self::assertSame($array, $typed->toArray());
        self::assertSame($first->commands, self::replay($array)->commands);
        self::assertSame($first->commands, self::replay($typed)->commands);
        self::assertSame('completed', $first->commands[0]['wait_for']);
        self::assertSame('sync', $first->commands[0]['mode_override']);
        self::assertSame($namespace, $first->commands[0]['namespace'] ?? null);
        self::assertArrayNotHasKey('caller_namespace', $first->commands[0]);
    }

    public static function namespaces(): iterable
    {
        yield 'current namespace default' => [null];
        yield 'explicit current namespace' => ['orders'];
        yield 'another target namespace' => ['billing'];
    }

    public function testExistingPositionalAndNamedConstructorArgumentsKeepTheirMeaning(): void
    {
        $positional = new ServiceOperationOptions('async', 'accepted', 30, 'key', null, null, null,
            'target', 'target-run', 'connection', 'queue', 'business', ['label' => 1], ['memo' => 2],
            ['search' => 3], 'return_existing_active');
        $named = new ServiceOperationOptions(modeOverride: 'async', waitFor: 'accepted', waitTimeoutSeconds: 30,
            idempotencyKey: 'key', targetWorkflowId: 'target', targetRunId: 'target-run', connection: 'connection',
            taskQueue: 'queue', businessKey: 'business', labels: ['label' => 1], memo: ['memo' => 2],
            searchAttributes: ['search' => 3], duplicateStartPolicy: 'return_existing_active');
        self::assertSame($named->toArray(), $positional->toArray());
        self::assertArrayNotHasKey('namespace', $positional->toArray());
        self::assertSame('return_existing_active', $positional->duplicateStartPolicy);
        self::assertSame([], (new ServiceOperationOptions())->toArray());
    }

    #[DataProvider('emptyNamespaces')]
    public function testEmptyTypedTargetIsRejectedBeforeAuthoring(string $namespace): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('targetNamespace');
        new ServiceOperationOptions(targetNamespace: $namespace);
    }

    public static function emptyNamespaces(): iterable
    {
        yield 'empty' => [''];
        yield 'blank' => [" \t\n"];
    }

    public function testTypedTargetReplaysRecordedCrossNamespaceAdmissionAndResult(): void
    {
        $codec = new AvroPayloadCodec();
        $typed = new ServiceOperationOptions(targetNamespace: 'billing');
        $command = self::replay($typed)->commands[0];
        $intent = ['sequence' => 1, 'service_call_id' => 'call-1', 'namespace' => 'billing',
            'caller_namespace' => 'orders', 'target_namespace' => 'billing',
            'endpoint_name' => 'billing', 'service_name' => 'invoices', 'operation_name' => 'create',
            'operation_mode' => 'sync', 'wait_for' => 'completed', 'idempotency_key' => $command['idempotency_key'],
            'request_payload' => $command['request_payload'],
            'service_call' => ['id' => 'call-1', 'namespace' => 'billing']];
        $started = ['event_type' => 'ServiceCallStarted', 'payload' => $intent];
        $completed = ['event_type' => 'ServiceCallCompleted', 'payload' => [
            ...$intent, 'response_payload' => $codec->envelope(['max' => PHP_INT_MAX, 'text' => '雪☃', 'null' => null])]];
        self::assertSame([], self::replay($typed, [$started])->commands);
        foreach (range(1, 2) as $_) {
            $result = self::replay($typed, [$started, $completed]);
            self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
            self::assertSame(['max' => PHP_INT_MAX, 'text' => '雪☃', 'null' => null], $codec->decodeEnvelope($result->commands[0]['result']));
        }
        $this->expectException(NonDeterministicWorkflow::class);
        self::replay(new ServiceOperationOptions(targetNamespace: 'other'), [$started, $completed]);
    }

    #[DataProvider('callerAuthorityFields')]
    public function testTargetSelectionDoesNotGrantCallerAuthority(string $field): void
    {
        $result = self::replay(new ServiceOperationOptions(...['targetNamespace' => 'billing', $field => 'forged']));
        self::assertInstanceOf(InvalidArgumentException::class, $result->terminalFailure);
        self::assertStringContainsString('Caller authority is assigned by Server.', $result->terminalFailure->getMessage());
        self::assertSame([], $result->commands);
    }

    public static function callerAuthorityFields(): iterable
    {
        foreach (['callerNamespace', 'callerWorkflowId', 'callerRunId'] as $field) {
            yield $field => [$field];
        }
    }

    public function testDirectClientRefusesAnIgnoredWorkflowTargetSelector(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', namespace: 'orders', transport: $transport);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('target namespace');
        try {
            $client->startServiceOperation('billing', 'invoices', 'create', [], new ServiceOperationOptions(targetNamespace: 'billing'));
        } finally {
            self::assertSame([], $transport->requests);
        }
    }

    private static function replay(array|ServiceOperationOptions $options, array $history = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context) use ($options): mixed {
            return $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42], $options)->responsePayload;
        }, $history, [], 'callers', ['workflow_id' => 'caller', 'run_id' => 'caller-run', 'namespace' => 'orders']);
    }
}
