<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
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

    public function testReplayFixturesExerciseCancellationAndWorkerUpdateSemantics(): void
    {
        $commands = ReplayRegressionFixture::execute([
            'fixture_schema' => 'durable-workflow.replay-regression/v1',
            'id' => 'cancellation-semantics',
            'protocol_version' => '1.0',
            'bindings' => ['php'],
            'workflow' => [
                'type' => 'golden.cancellation',
                'input' => [],
            ],
            'command_sequence' => [[
                'type' => 'fail_workflow',
                'message' => 'Workflow cancellation was requested.',
                'exception_type' => 'DurableWorkflow\Exception\WorkflowCancelled',
            ]],
            'expected' => ['type' => 'fail_workflow'],
        ]);

        self::assertSame('fail_workflow', $commands[0]['type']);

        $codec = new AvroPayloadCodec();
        $commands = ReplayRegressionFixture::execute([
            'fixture_schema' => 'durable-workflow.replay-regression/v1',
            'id' => 'worker-update-semantics',
            'protocol_version' => '1.0',
            'bindings' => ['php'],
            'workflow' => [
                'type' => 'golden.worker-update',
                'input' => [],
            ],
            'history' => [[
                'event_type' => 'UpdateAccepted',
                'payload' => [
                    'update_id' => 'update-1',
                    'update_name' => 'golden.update',
                    'arguments' => $codec->envelope('hello Ada'),
                ],
            ]],
            'command_sequence' => [[
                'type' => 'complete_update',
                'update_id' => 'update-1',
                'result' => ['updated' => 'hello Ada'],
            ]],
            'expected' => ['type' => 'complete_update'],
        ]);

        self::assertSame('complete_update', $commands[0]['type']);
    }

    public function testConditionWaitRestartFixtureReplaysThroughFreshWorkerExecutions(): void
    {
        $commands = ReplayRegressionFixture::executeFile(
            __DIR__.'/fixtures/condition-waits/worker-restart.json',
        );

        self::assertCount(2, $commands);
        self::assertSame('open_condition_wait', $commands[0]['type']);
        self::assertSame($commands[0]['command_sequence'], $commands[1]['command_sequence']);
    }

    public function testConditionWaitMismatchFixtureRejectsChangedPredicateFingerprint(): void
    {
        $fixture = json_decode(
            (string) file_get_contents(__DIR__.'/fixtures/condition-waits/mismatched-replay.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        self::assertIsArray($fixture);
        $history = $fixture['history'] ?? null;
        self::assertIsArray($history);
        $workflow = static function (WorkflowContext $context): bool {
            return $context->waitCondition(
                static fn (): bool => false,
                key: 'approval-mismatch',
                timeout: 30,
            );
        };

        try {
            (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'php-workers');
            self::fail('The mismatched replay fixture must be rejected.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame($fixture['expected']['exception_type'] ?? null, $exception::class);
            self::assertSame($fixture['expected']['sequence'] ?? null, $exception->sequence);
        }
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
