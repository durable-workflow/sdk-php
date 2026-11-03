<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A recorded SearchAttributesUpserted binds replay the way a recorded memo
 * does: workflow code that upserts different values no longer matches its
 * history. This matters most for redrive, where the successor reuses the
 * failed run's upsert instead of running it again.
 */
final class SearchAttributeReplayTest extends TestCase
{
    public function testRedriveReusedSearchAttributesRejectAChangedValue(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-search-attributes-changed.json';
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        $reused = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] === 'SearchAttributesUpserted'));
        self::assertCount(1, $reused);
        self::assertSame(['Stage' => 'greeting'], $reused[0]['payload']['attributes']);
        self::assertSame('01JBREDRIVESOURCERUN000000', $reused[0]['payload']['reused_from_run_id']);
        self::assertIsString($reused[0]['payload']['reused_recorded_at']);

        $commands = ReplayRegressionFixture::executeFile($path);

        self::assertCount(1, $commands);
        self::assertSame('replay_error', $commands[0]['type']);
        self::assertSame('workflow_nondeterministic', $commands[0]['reason']);
        self::assertSame(2, $commands[0]['sequence']);
    }

    /** @param array<string, mixed> $attributes */
    #[DataProvider('changedCalls')]
    public function testChangedSearchAttributeCallIsRejected(array $attributes): void
    {
        try {
            $this->replay($attributes, ['Stage' => 'greeting', 'Count' => 2]);
            self::fail('A changed search-attribute upsert must not match its recorded history.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('workflow_nondeterministic', $exception->reason);
            self::assertSame(1, $exception->sequence);
            self::assertSame('{"Count":2,"Stage":"greeting"}', $exception->expected);
        }
    }

    /** @return array<string, array{array<string, mixed>}> */
    public static function changedCalls(): array
    {
        return [
            'changed value' => [['Stage' => 'approved', 'Count' => 2]],
            'changed type' => [['Stage' => 'greeting', 'Count' => '2']],
            'added key' => [['Stage' => 'greeting', 'Count' => 2, 'Region' => 'eu']],
            'removed key' => [['Stage' => 'greeting']],
            'deleted key' => [['Stage' => 'greeting', 'Count' => null]],
        ];
    }

    public function testCallMatchesTheValuesCoreNormalizedIntoHistory(): void
    {
        $result = $this->replay(
            ['Tags' => [' urgent ', 'urgent'], 'Stage' => " greeting\n", 'Gone' => '', 'Ratio' => 2.0, 'Count' => 9_007_199_254_740_993],
            ['Count' => 9_007_199_254_740_993, 'Gone' => null, 'Ratio' => 2.0, 'Stage' => 'greeting', 'Tags' => ['urgent', 'urgent']],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
    }

    public function testRecordedUpsertWithoutAttributesFailsClosed(): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('missing its recorded attributes');

        $this->replay(['Stage' => 'greeting'], null);
    }

    /**
     * @param array<string, mixed> $attributes
     * @param array<string, mixed>|null $recorded
     */
    private function replay(array $attributes, ?array $recorded): ReplayResult
    {
        $payload = ['sequence' => 1, 'merged' => $recorded ?? []];
        if ($recorded !== null) {
            $payload['attributes'] = $recorded;
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
