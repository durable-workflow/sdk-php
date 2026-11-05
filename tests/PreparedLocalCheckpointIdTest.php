<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\PreparedLocalCheckpointId;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;

/** The prepared-local checkpoint id does not depend on serialize_precision (#288, #282). */
final class PreparedLocalCheckpointIdTest extends TestCase
{
    public function testTheIdIsUnchangedUnderTheDefaultPrecision(): void
    {
        foreach (self::commandSets() as $commands) {
            self::assertSame(
                hash('sha256', json_encode(['task-1', 'worker-1', 4, 7, $commands], JSON_THROW_ON_ERROR)),
                PreparedLocalCheckpointId::for('task-1', 'worker-1', 4, 7, $commands),
            );
        }
    }

    #[RunInSeparateProcess]
    public function testTheIdIsTheSameUnderAReducedSerializePrecision(): void
    {
        $default = array_map(static fn (array $commands): string => PreparedLocalCheckpointId::for('task-1', 'worker-1', 4, 7, $commands), self::commandSets());

        ini_set('serialize_precision', '3');
        $reduced = array_map(static fn (array $commands): string => PreparedLocalCheckpointId::for('task-1', 'worker-1', 4, 7, $commands), self::commandSets());

        self::assertSame($default, $reduced);
        self::assertNotSame(
            PreparedLocalCheckpointId::for('task-1', 'worker-1', 4, 7, [self::upsert(1.2345)]),
            PreparedLocalCheckpointId::for('task-1', 'worker-1', 4, 7, [self::upsert(1.2344)]),
            'close floats still give different ids',
        );
        self::assertSame('3', ini_get('serialize_precision'));
    }

    /** @return list<list<array<string, mixed>>> */
    private static function commandSets(): array
    {
        return [
            [self::upsert(1.2345)],
            [self::upsert(0.1 + 0.2), ['type' => 'record_side_effect', 'result' => 'opaque-avro']],
            [['type' => 'record_version_marker', 'change_id' => 'v1', 'version' => 2]],
        ];
    }

    /** @return array<string, mixed> */
    private static function upsert(float $ratio): array
    {
        return ['type' => 'upsert_search_attributes', 'attributes' => ['Ratio' => $ratio, 'Whole' => 2.0]];
    }
}
