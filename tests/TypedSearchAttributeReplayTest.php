<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowCommand;
use DurableWorkflow\Worker\WorkflowContext;
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
        self::assertSame(['complete_workflow'], $this->replay(['Stage' => 'keyword'], null));
    }

    /**
     * @param array<string, string> $authoredTypes
     * @param array<string, string>|null $recordedTypes
     * @return list<string>
     */
    private function replay(array $authoredTypes, ?array $recordedTypes): array
    {
        $payload = ['sequence' => 1, 'attributes' => ['Stage' => 'private-stage-value'], 'merged' => ['Stage' => 'private-stage-value']];
        if ($recordedTypes !== null) {
            $payload['attribute_types'] = $recordedTypes;
        }
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context) use ($authoredTypes): string {
                $context->upsertSearchAttributes(['Stage' => 'private-stage-value'], $authoredTypes);

                return 'done';
            },
            [['event_type' => 'SearchAttributesUpserted', 'payload' => $payload]],
            [],
            'typed-search-replay',
        );

        return array_column($result->commands, 'type');
    }
}
