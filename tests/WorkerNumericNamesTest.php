<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerNumericNamesTest extends TestCase
{
    public static function names(): iterable
    {
        yield 'zero' => ['0'];
        yield 'integer-shaped' => ['17'];
        yield 'leading-zero' => ['017'];
        yield 'ordinary' => ['ordinary'];
    }

    public static function commandNames(): iterable
    {
        foreach (['query', 'signal', 'update'] as $kind) {
            foreach (self::names() as $label => [$name]) {
                yield $kind.'-'.$label => [$kind, $name];
            }
        }
    }

    #[DataProvider('names')]
    public function testManagedWorkflowAndActivityNamesRemainStrings(string $name): void
    {
        $transport = $this->transport();
        $worker = $this->worker($transport)
            ->registerWorkflow($name, static fn (WorkflowContext $context): string => 'result')
            ->registerActivity($name, static fn (ActivityContext $context): string => 'result');

        $worker->validate();
        self::assertSame([$name], $worker->contracts()['workflows']);
        self::assertSame([$name], $worker->contracts()['activities']);
        $worker->run(0);

        $wire = json_decode(json_encode($transport->requests[0]['body'], JSON_THROW_ON_ERROR), false, 512, JSON_THROW_ON_ERROR);
        self::assertSame([$name], $wire->supported_workflow_types);
        self::assertSame([$name], $wire->supported_activity_types);
        self::assertInstanceOf(\stdClass::class, $wire->workflow_command_contracts);
        self::assertInstanceOf(\stdClass::class, $wire->workflow_definition_fingerprints);
        self::assertMatchesRegularExpression('/^sha256:[a-f0-9]{64}$/D', $wire->workflow_definition_fingerprints->{$name});
        self::assertSame([], $wire->workflow_command_contracts->{$name}->queries);
    }

    #[DataProvider('commandNames')]
    public function testCommandDeclarationNamesRemainStrings(string $kind, string $name): void
    {
        $worker = $this->worker()->registerWorkflow('ordinary', static fn (WorkflowContext $context): string => 'result');
        $query = static fn (QueryContext $context, string $value): string => $value;
        match ($kind) {
            'query' => $worker->registerQuery('ordinary', $name, $query),
            'signal' => $worker->declareSignal('ordinary', $name, static fn (string $value): string => $value),
            'update' => $worker->registerUpdate('ordinary', $name, $query),
        };

        $worker->validate();
        $contract = $worker->contracts()['workflow_commands']['ordinary'];
        self::assertSame([$name], $contract[$kind === 'query' ? 'queries' : $kind.'s']);
        self::assertSame($name, $contract[$kind.'_contracts'][0]['name']);
        self::assertSame('value', $contract[$kind.'_contracts'][0]['parameters'][0]['name']);
    }

    public function testDistinctNamesDoNotCollapseIntoOneHandler(): void
    {
        $transport = $this->transport();
        $worker = $this->worker($transport);
        $handlers = [];
        foreach (['0', '17', '017', 'ordinary'] as $name) {
            $handler = static fn (ActivityContext $context): string => $name;
            $handlers[$name] = $handler;
            $worker->registerWorkflow($name, static fn (WorkflowContext $context): string => $name);
            $worker->registerActivity($name, $handler);
        }

        $worker->run(0);
        self::assertSame(['0', '17', '017', 'ordinary'], $worker->contracts()['workflows']);
        self::assertSame(['0', '17', '017', 'ordinary'], $worker->contracts()['activities']);
        foreach (['0', '17', '017', 'ordinary'] as $name) {
            self::assertSame($handlers[$name], $worker->registeredHandler('activity', $name));
        }
    }

    public function testAttributedNamesReachManagedRegistration(): void
    {
        $transport = $this->transport();
        $worker = $this->worker($transport)->register(NumericAttributedWorkflow::class);

        $worker->run(0);
        self::assertSame(['0'], $worker->contracts()['workflows']);
        self::assertSame(['0', '17', '017'], $worker->contracts()['activities']);
        foreach (['queries', 'signals', 'updates'] as $kind) {
            self::assertSame(['0'], $worker->contracts()['workflow_commands']['0'][$kind]);
        }
    }

    public function testDuplicateAttributedNumericNamesStillRefuseAtomically(): void
    {
        $worker = $this->worker()->register(NumericAttributedWorkflow::class);
        $before = $worker->contracts();
        try {
            $worker->register(NumericAttributedWorkflow::class);
            self::fail('Duplicate attributed workflow was accepted.');
        } catch (InvalidWorkerDefinition) {
            self::assertSame($before, $worker->contracts());
        }
    }

    public function testDuplicateAndMalformedExplicitNamesStillRefuse(): void
    {
        $workflow = static fn (WorkflowContext $context): string => 'result';
        $worker = $this->worker()->registerWorkflow('0', $workflow);
        $before = $worker->contracts();
        foreach (['0', '', ' 0', '0 '] as $name) {
            try {
                $worker->registerWorkflow($name, $workflow);
                self::fail('Invalid or duplicate workflow name was accepted.');
            } catch (\InvalidArgumentException) {
                self::assertSame($before, $worker->contracts());
            }
        }
    }

    public static function mapNames(): iterable
    {
        yield 'zero-map' => [['0']];
        yield 'contiguous-map' => [['0', '1']];
        yield 'sparse-map' => [['17']];
        yield 'leading-zero-map' => [['017']];
    }

    #[DataProvider('mapNames')]
    public function testDirectRegistrationEncodesKeyedMetadataAsObjects(array $names): void
    {
        $transport = new FakeTransport([['registered' => true]]);
        $client = new Client('https://server.example', transport: $transport);
        $contracts = [];
        $fingerprints = [];
        foreach ($names as $name) {
            $contracts[$name] = ['queries' => [], 'signals' => [], 'updates' => []];
            $fingerprints[$name] = 'sha256:'.str_repeat('a', 64);
        }
        $client->registerWorker('numeric-client', 'numeric', $names, [],
            workflowCommandContracts: $contracts, workflowDefinitionFingerprints: $fingerprints);

        $wire = json_decode(json_encode($transport->requests[0]['body'], JSON_THROW_ON_ERROR), false, 512, JSON_THROW_ON_ERROR);
        self::assertInstanceOf(\stdClass::class, $wire->workflow_command_contracts);
        self::assertInstanceOf(\stdClass::class, $wire->workflow_definition_fingerprints);
        foreach ($names as $name) {
            self::assertSame([], $wire->workflow_command_contracts->{$name}->queries);
            self::assertSame($fingerprints[$name], $wire->workflow_definition_fingerprints->{$name});
        }
    }

    private function transport(): FakeTransport
    {
        return new FakeTransport([
            ['registered' => true],
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
        ]);
    }

    private function worker(?FakeTransport $transport = null): Worker
    {
        return Worker::create(new Client('https://server.example', transport: $transport ?? new FakeTransport()), 'numeric');
    }
}

final class NumericAttributedWorkflow
{
    #[Workflow('0')]
    public function run(WorkflowContext $context): string
    {
        return 'result';
    }

    #[Activity('0')]
    public function zero(ActivityContext $context): string
    {
        return 'zero';
    }

    #[Activity('17')]
    public function integerName(ActivityContext $context): string
    {
        return 'integer';
    }

    #[Activity('017')]
    public function leadingZero(ActivityContext $context): string
    {
        return 'leading-zero';
    }

    #[Query('0')]
    public function query(QueryContext $context): string
    {
        return 'query';
    }

    #[Signal('0')]
    public function signal(string $value): void {}

    #[Update('0')]
    public function update(QueryContext $context, string $value): string
    {
        return $value;
    }
}
