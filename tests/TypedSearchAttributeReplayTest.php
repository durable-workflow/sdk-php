<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\RedriveFixtureRegistry;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TypedSearchAttributeReplayTest extends TestCase
{
    public function testChangingOnlyADeclaredTypeRefusesReplay(): void
    {
        try {
            $this->replay(['Stage' => 'keyword'], ['Stage' => 'string']);
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('search_attribute_type_mismatch', $error->reason);
            self::assertSame(1, $error->sequence);
            self::assertStringNotContainsString('private-stage-value', $error->getMessage());

            return;
        }
        self::fail('An unchanged value must not hide a changed declared type.');
    }

    public function testAnExplicitTypeDeclarationIsSentWithTheCommand(): void
    {
        $command = WorkflowCommand::upsertSearchAttributes(['Stage' => 'greeting'], ['Stage' => 'keyword']);

        self::assertSame(['Stage' => 'keyword'], $command->toWire(new AvroPayloadCodec(), 'test')['attribute_types'] ?? null);
    }

    public function testIdenticalTypeIdentityReplays(): void
    {
        self::assertSame(['complete_workflow'], $this->replay(['Stage' => 'keyword'], ['Stage' => 'keyword']));
    }

    public function testLegacyHistoryWithoutTypeMetadataKeepsValueOnlyCompatibility(): void
    {
        self::assertSame(['complete_workflow'], $this->replay(['Stage' => 'keyword'], null, hasTypeField: false));
    }

    public function testExistingOneArgumentCallsKeepTheirWireShape(): void
    {
        self::assertSame(
            ['type' => 'upsert_search_attributes', 'attributes' => ['Stage' => 'greeting']],
            WorkflowCommand::upsertSearchAttributes(['Stage' => 'greeting'])->toWire(new AvroPayloadCodec(), 'test'),
        );
    }

    public function testAllCanonicalTypesAndMapOrderingRoundTrip(): void
    {
        $attributes = ['Text' => 'hello', 'Stage' => 'ready', 'Tags' => [' first ', 'second'],
            'Count' => 42, 'Ratio' => 1.25, 'Ready' => true, 'At' => '2026-11-04T12:00:00.123456+02:00'];
        $types = ['Text' => 'string', 'Stage' => 'keyword', 'Tags' => 'keyword_list',
            'Count' => 'int', 'Ratio' => 'float', 'Ready' => 'bool', 'At' => 'datetime'];
        self::assertSame(['complete_workflow'], $this->replay($types, array_reverse($types, true), $attributes));
        $wire = WorkflowCommand::upsertSearchAttributes($attributes, $types)->toWire(new AvroPayloadCodec(), 'test');
        ksort($types, SORT_STRING);
        self::assertSame($types, $wire['attribute_types']);
        self::assertSame($attributes, $wire['attributes']);
    }

    public function testDeletionKeepsItsExplicitOrEmptyTypeIdentity(): void
    {
        self::assertSame(['complete_workflow'], $this->replay([], [], ['Stage' => null]));
        self::assertSame(['complete_workflow'], $this->replay(['Stage' => 'keyword'], ['Stage' => 'keyword'], ['Stage' => null]));
        $wire = WorkflowCommand::upsertSearchAttributes(['Stage' => null], [])->toWire(new AvroPayloadCodec(), 'test');
        self::assertArrayNotHasKey('attribute_types', $wire);
        self::assertSame(['Stage' => null], $wire['attributes']);
    }

    /** @return iterable<string, array{mixed}> */
    public static function malformedRecordedTypes(): iterable
    {
        yield 'explicit null' => [null];
        yield 'scalar' => ['keyword'];
        yield 'list' => [['keyword']];
        yield 'non-string type' => [['Stage' => 7]];
        yield 'unsupported type' => [['Stage' => 'unsupported-private-type']];
        yield 'noncanonical alias' => [['Stage' => 'Keyword']];
        yield 'orphan declaration' => [['Stage' => 'keyword', 'Other' => 'keyword']];
        yield 'missing declaration' => [[]];
        yield 'incompatible value' => [['Stage' => 'bool']];
    }

    #[DataProvider('malformedRecordedTypes')]
    public function testMalformedRecordedTypesFailClosed(mixed $types): void
    {
        try {
            $this->replay(['Stage' => 'keyword'], $types);
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('search_attribute_types_malformed', $error->reason);
            self::assertSame(1, $error->sequence);
            self::assertStringNotContainsString('private', $error->getMessage());

            return;
        }
        self::fail('Malformed recorded type identity must be refused.');
    }

    /** @return iterable<string, array{array<array-key, mixed>, array<array-key, mixed>}> */
    public static function invalidDeclarations(): iterable
    {
        yield 'empty attributes' => [[], []];
        yield 'missing explicit types' => [['Stage' => 'ready'], []];
        yield 'extra type' => [['Stage' => 'ready'], ['Stage' => 'keyword', 'Other' => 'keyword']];
        yield 'unknown type' => [['Stage' => 'ready'], ['Stage' => 'unsupported-private-type']];
        yield 'numeric key' => [[1 => 'ready'], [1 => 'keyword']];
        yield 'invalid key' => [['bad key' => 'ready'], ['bad key' => 'keyword']];
        yield 'int is not bool' => [['Ready' => 1], ['Ready' => 'bool']];
        yield 'bool is not int' => [['Count' => true], ['Count' => 'int']];
        yield 'float is not int' => [['Count' => 1.25], ['Count' => 'int']];
        yield 'nonfinite float' => [['Ratio' => INF], ['Ratio' => 'float']];
        yield 'numeric string is not float' => [['Ratio' => '1.25'], ['Ratio' => 'float']];
        yield 'empty string needs null' => [['Stage' => ' '], ['Stage' => 'keyword']];
        yield 'map is not list' => [['Tags' => ['tag' => 'x']], ['Tags' => 'keyword_list']];
        yield 'list elements must be strings' => [['Tags' => [1]], ['Tags' => 'keyword_list']];
        yield 'datetime timezone required' => [['At' => '2026-11-04T12:00:00'], ['At' => 'datetime']];
        yield 'invalid calendar date' => [['At' => '2026-02-31T12:00:00Z'], ['At' => 'datetime']];
        yield 'invalid timezone' => [['At' => '2026-11-04T12:00:00+25:00'], ['At' => 'datetime']];
        yield 'keyword limit' => [['Stage' => str_repeat('x', 256)], ['Stage' => 'keyword']];
        yield 'string limit' => [['Text' => str_repeat('x', 2049)], ['Text' => 'string']];
        yield 'keyword list limit' => [['Tags' => [str_repeat('x', 256)]], ['Tags' => 'keyword_list']];
    }

    #[DataProvider('invalidDeclarations')]
    public function testInvalidAuthoredDeclarationsAreRejected(array $attributes, array $types): void
    {
        $this->expectException(InvalidArgumentException::class);
        WorkflowCommand::upsertSearchAttributes($attributes, $types);
    }

    public function testRedriveFixturePinsTypeChangesAtTheReusedStep(): void
    {
        $path = RedriveFixtureRegistry::path('redrive-reused-search-attribute-type-changed');
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, ['MemoUpserted', 'SearchAttributesUpserted']));
        $tasks = ReplayRegressionFixture::executeFile($path);
        self::assertSame([['type' => 'replay_error', 'reason' => 'search_attribute_type_mismatch', 'sequence' => 2]], $tasks[0]['command_sequence']);
    }

    /**
     * @param array<string, string> $authoredTypes
     * @param array<string, mixed> $attributes
     * @return list<string>
     */
    private function replay(array $authoredTypes, mixed $recordedTypes,
        array $attributes = ['Stage' => 'private-stage-value'], bool $hasTypeField = true): array
    {
        $payload = ['sequence' => 1, 'attributes' => $attributes, 'merged' => $attributes];
        if ($hasTypeField) {
            $payload['attribute_types'] = $recordedTypes;
        }
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use ($authoredTypes, $attributes): string {
                $context->upsertSearchAttributes($attributes, $authoredTypes);

                return 'done';
            },
            [['event_type' => 'SearchAttributesUpserted', 'payload' => $payload]],
            [],
            'typed-search-replay',
        );

        return array_column($result->commands, 'type');
    }
}
