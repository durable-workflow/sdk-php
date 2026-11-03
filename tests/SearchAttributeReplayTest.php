<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use JsonSerializable;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A recorded SearchAttributesUpserted binds replay by value, the way a
 * recorded memo does: workflow code that upserts different values no longer
 * matches its history. This matters most for redrive, where the successor
 * reuses the failed run's upsert instead of running it again.
 *
 * This is value-only binding. Recorded attribute_types are not compared,
 * because this SDK's upsert command cannot declare types.
 */
final class SearchAttributeReplayTest extends TestCase
{
    public function testRedriveReusedSearchAttributesRejectAChangedValue(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-search-attributes-changed.json';
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(3, $fixture['history'][0]['payload']['resume_step_sequence']);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, ['MemoUpserted', 'SearchAttributesUpserted']));
        $reused = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] === 'SearchAttributesUpserted'));
        self::assertCount(1, $reused);
        self::assertSame(['Stage' => 'greeting'], $reused[0]['payload']['attributes']);
        self::assertSame($fixture['history'][0]['payload']['continued_from_run_id'], $reused[0]['payload']['reused_from_run_id']);
        self::assertIsString($reused[0]['payload']['reused_recorded_at']);

        $tasks = ReplayRegressionFixture::executeFile($path);

        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];
        self::assertCount(1, $commands);
        self::assertSame('replay_error', $commands[0]['type']);
        self::assertSame('search_attribute_value_mismatch', $commands[0]['reason']);
        self::assertSame(2, $commands[0]['sequence']);
    }

    /** @param array<string, mixed> $attributes */
    #[DataProvider('changedCalls')]
    public function testChangedSearchAttributeValuesAreRejected(array $attributes): void
    {
        $exception = $this->refusal($attributes, ['Stage' => 'greeting', 'Count' => 2]);

        self::assertSame('search_attribute_value_mismatch', $exception->reason);
        self::assertSame(1, $exception->sequence);
        // Diagnostics stay payload-free.
        self::assertSame('the recorded search attributes', $exception->expected);
        foreach (['greeting', 'approved', 'eu'] as $value) {
            self::assertStringNotContainsString($value, $exception->getMessage().$exception->actual);
        }
    }

    /** @return array<string, array{array<string, mixed>}> */
    public static function changedCalls(): array
    {
        return [
            'changed value' => [['Stage' => 'approved', 'Count' => 2]],
            'changed PHP value type' => [['Stage' => 'greeting', 'Count' => '2']],
            'added key' => [['Stage' => 'greeting', 'Count' => 2, 'Region' => 'eu']],
            'removed key' => [['Stage' => 'greeting']],
            'deleted key' => [['Stage' => 'greeting', 'Count' => null]],
        ];
    }

    public function testCallMatchesTheValuesCoreNormalizedIntoHistory(): void
    {
        $result = $this->replay(
            ['Tags' => [' urgent ', 'urgent'], 'Stage' => " greeting\n", 'Gone' => '', 'Ratio' => 2.0, 'Count' => 9_007_199_254_740_993, 'Flag' => false],
            ['Count' => 9_007_199_254_740_993, 'Flag' => false, 'Gone' => null, 'Ratio' => 2.0, 'Stage' => 'greeting', 'Tags' => ['urgent', 'urgent']],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * Values outside Core's portable set refuse replay with a typed reason
     * instead of being coerced into a matching JSON form.
     *
     * @param array<array-key, mixed> $authored
     * @param array<array-key, mixed> $recorded
     */
    #[DataProvider('unportableAttributes')]
    public function testUnportableSearchAttributesRefuseReplay(array $authored, array $recorded, string $origin): void
    {
        $exception = $this->refusal($authored, $recorded);

        self::assertSame('search_attributes_unportable', $exception->reason);
        self::assertSame(1, $exception->sequence);
        self::assertStringContainsString("The {$origin} search attributes", $exception->getMessage());
    }

    /** @return array<string, array{array<array-key, mixed>, array<array-key, mixed>, string}> */
    public static function unportableAttributes(): array
    {
        $greeting = new class implements JsonSerializable {
            public function jsonSerialize(): string
            {
                return 'greeting';
            }
        };

        return [
            'numeric key' => [[1 => 'x', 'Stage' => 'greeting'], [1 => 'x', 'Stage' => 'greeting'], 'recorded'],
            'authored numeric key' => [[1 => 'x', 'Stage' => 'greeting'], ['1x' => 'x', 'Stage' => 'greeting'], 'authored'],
            'key with a space' => [['Stage name' => 'x'], ['Stage' => 'x'], 'authored'],
            'object with the same JSON form' => [['Stage' => $greeting], ['Stage' => 'greeting'], 'authored'],
            'object with the same shape as a map' => [['Stage' => (object) ['a' => 'x']], ['Stage' => ['x']], 'authored'],
            'associative array' => [['Stage' => ['a' => 'x']], ['Stage' => ['a' => 'x']], 'recorded'],
            'nested list' => [['Tags' => [['x']]], ['Tags' => [['x']]], 'recorded'],
            'non-string list members' => [['Tags' => [1, 2]], ['Tags' => [1, 2]], 'recorded'],
            'authored non-string list member' => [['Tags' => ['x', true]], ['Tags' => ['x', '1']], 'authored'],
            'not a number' => [['Ratio' => NAN], ['Ratio' => 1.0], 'authored'],
            'infinity' => [['Ratio' => INF], ['Ratio' => 1.0], 'authored'],
            'negative infinity' => [['Ratio' => -INF], ['Ratio' => 1.0], 'authored'],
        ];
    }

    public function testRedriveReusedUpsertWithoutAttributesFailsClosed(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-search-attributes-missing.json';
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, ['MemoUpserted', 'SearchAttributesUpserted']));
        self::assertArrayNotHasKey('attributes', $fixture['history'][2]['payload']);

        $tasks = ReplayRegressionFixture::executeFile($path);

        self::assertSame('replay_error', $tasks[0]['command_sequence'][0]['type']);
        self::assertSame('search_attribute_update_missing', $tasks[0]['command_sequence'][0]['reason']);
        self::assertSame(2, $tasks[0]['command_sequence'][0]['sequence']);
    }

    public function testRecordedUpsertWithoutAttributesFailsClosed(): void
    {
        // One reason across SDKs: sdk-rust reports search_attribute_update_missing.
        $exception = $this->refusal(['Stage' => 'greeting'], null);

        self::assertSame('search_attribute_update_missing', $exception->reason);
    }

    public function testFloatIdentityDoesNotDependOnSerializePrecision(): void
    {
        $previous = ini_get('serialize_precision');
        ini_set('serialize_precision', '3');
        try {
            $exception = $this->refusal(['Ratio' => 1.2344], ['Ratio' => 1.2345]);
        } finally {
            ini_set('serialize_precision', (string) $previous);
        }

        self::assertSame('search_attribute_value_mismatch', $exception->reason);
        self::assertStringNotContainsString('1.23', $exception->getMessage());
    }

    public function testSignedZeroMatchesZero(): void
    {
        $result = $this->replay(['Ratio' => -0.0], ['Ratio' => 0.0]);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    public function testIntAndFloatAreDifferentValues(): void
    {
        self::assertSame('search_attribute_value_mismatch', $this->refusal(['Count' => 2], ['Count' => 2.0])->reason);
        self::assertSame('search_attribute_value_mismatch', $this->refusal(['Count' => 2.0], ['Count' => 2])->reason);
    }

    public function testValuesAtCoresLengthBoundsMatch(): void
    {
        $text = str_repeat('é', 2048);
        $keyword = str_repeat('é', 255);
        $result = $this->replay(['Text' => " {$text} ", 'Tags' => [" {$keyword} "]], ['Text' => $text, 'Tags' => [$keyword]]);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * @param array<string, mixed> $authored
     * @param array<string, mixed> $recorded
     */
    #[DataProvider('overLengthValues')]
    public function testValuesOverCoresLengthBoundsRefuseReplay(array $authored, array $recorded, string $origin): void
    {
        $exception = $this->refusal($authored, $recorded);

        self::assertSame('search_attributes_unportable', $exception->reason);
        self::assertStringContainsString("The {$origin} search attributes", $exception->getMessage());
        self::assertStringNotContainsString('é', $exception->getMessage());
    }

    /** @return array<string, array{array<string, mixed>, array<string, mixed>, string}> */
    public static function overLengthValues(): array
    {
        $text = str_repeat('é', 2049);
        $keyword = str_repeat('é', 256);

        return [
            'recorded string over 2048' => [['Text' => $text], ['Text' => $text], 'recorded'],
            'authored string over 2048' => [['Text' => $text], ['Text' => 'x'], 'authored'],
            'recorded list item over 255' => [['Tags' => [$keyword]], ['Tags' => [$keyword]], 'recorded'],
            'authored list item over 255' => [['Tags' => [$keyword]], ['Tags' => ['x']], 'authored'],
        ];
    }

    /**
     * Value-only: the recorded effective type is not compared. Here Server
     * recorded a namespace-registered "string" type for a value it would infer
     * as "keyword", and replay still matches on the value. Typed reuse needs a
     * command that declares types, which this SDK does not have.
     */
    public function testRecordedAttributeTypesAreNotCompared(): void
    {
        $result = $this->replay(['Stage' => 'greeting'], ['Stage' => 'greeting'], ['Stage' => 'string']);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    /**
     * @param array<array-key, mixed> $attributes
     * @param array<array-key, mixed>|null $recorded
     */
    private function refusal(array $attributes, ?array $recorded): NonDeterministicWorkflow
    {
        try {
            $this->replay($attributes, $recorded);
        } catch (NonDeterministicWorkflow $exception) {
            return $exception;
        }
        self::fail('Replay must refuse this search-attribute upsert.');
    }

    /**
     * @param array<array-key, mixed> $attributes
     * @param array<array-key, mixed>|null $recorded
     * @param array<string, string>|null $types
     */
    private function replay(array $attributes, ?array $recorded, ?array $types = null): ReplayResult
    {
        $payload = ['sequence' => 1, 'merged' => $recorded ?? []];
        if ($recorded !== null) {
            $payload['attributes'] = $recorded;
        }
        if ($types !== null) {
            $payload['attribute_types'] = $types;
        }

        return (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use ($attributes): string {
                $context->upsertSearchAttributes($attributes);

                return 'done';
            },
            [['event_type' => 'SearchAttributesUpserted', 'payload' => $payload]],
            [],
            'search-attribute-replay',
        );
    }
}
