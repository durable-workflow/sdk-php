<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ReplayRegressionCorpusTest extends TestCase
{
    public function testEveryPolicySelectedFixtureUsesTheOfficialReplayer(): void
    {
        [$selectorCount, $paths] = self::replayFixturePaths();
        self::assertGreaterThan(0, $selectorCount);

        foreach ($paths as $path) {
            ReplayRegressionFixture::executeFile($path);
        }

        self::addToAssertionCount(1);
    }

    public function testReplayFixtureFormatsExecuteThroughTheOfficialReplayer(): void
    {
        $codec = new AvroPayloadCodec();
        $commands = ReplayRegressionFixture::execute([
            'fixture_schema' => 'durable-workflow.replay-regression/v1',
            'id' => 'history-format-contract',
            'protocol_version' => '1.0',
            'bindings' => ['php'],
            'workflow' => [
                'type' => 'golden.single-activity',
                'input' => ['Ada'],
            ],
            'history' => [[
                'event_type' => 'ActivityCompleted',
                'payload' => ['result' => $codec->envelope('hello Ada')],
            ]],
            'expected' => [
                'command_sequence' => [[
                    'type' => 'complete_workflow',
                    'result' => 'hello Ada',
                ]],
            ],
        ]);

        self::assertSame('complete_workflow', $commands[0]['type']);

        $commands = ReplayRegressionFixture::execute([
            'fixture_schema' => 'durable-workflow.replay-regression/v1',
            'id' => 'command-sequence-format-contract',
            'protocol_version' => '1.0',
            'bindings' => ['php'],
            'workflow' => [
                'type' => 'golden.single-activity',
                'input' => ['Ada'],
            ],
            'command_sequence' => [[
                'type' => 'schedule_activity',
                'activity_type' => 'golden.greet',
                'arguments' => ['Ada'],
            ]],
            'expected' => ['type' => 'schedule_activity'],
        ]);

        self::assertSame('schedule_activity', $commands[0]['type']);
    }

    /** @return array{int, list<string>} */
    private static function replayFixturePaths(): array
    {
        $root = dirname(__DIR__);
        $policy = json_decode(
            (string) file_get_contents($root.'/regression-corpus-policy.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        if (!is_array($policy)) {
            throw new RuntimeException('Regression corpus policy must be a JSON object.');
        }
        $fixtures = $policy['categories']['replay']['fixtures'] ?? null;
        if (!is_array($fixtures)) {
            throw new RuntimeException('Regression corpus policy must declare replay fixtures.');
        }

        $selectorCount = 0;
        $paths = [];
        foreach ($fixtures as $fixture) {
            if (!is_array($fixture)) {
                throw new RuntimeException('Replay fixture selector must be an object.');
            }
            if (($fixture['format'] ?? null) !== 'replay-regression-v1') {
                throw new RuntimeException('Replay fixture selector has no official PHP consumer.');
            }
            $glob = $fixture['glob'] ?? null;
            if (!is_string($glob) || $glob === '') {
                throw new RuntimeException('Replay regression fixture selector must have a glob.');
            }
            if (preg_match(
                '/\A(?:[A-Za-z0-9._-]+\/)*(?:[A-Za-z0-9._-]+|\*)\.json\z/D',
                $glob,
            ) !== 1) {
                throw new RuntimeException(
                    "Replay fixture selector {$glob} is not portable to the official PHP consumer.",
                );
            }
            ++$selectorCount;
            foreach (glob($root.'/'.$glob) ?: [] as $path) {
                if (isset($paths[$path])) {
                    throw new RuntimeException("Replay fixture {$path} is selected more than once.");
                }
                $paths[$path] = true;
            }
        }

        $selected = array_keys($paths);
        sort($selected);

        return [$selectorCount, $selected];
    }
}
