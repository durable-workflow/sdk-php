<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowVersioningTest extends TestCase
{
    public function testPackageMetadataAdvertisesTheLanguageNeutralVersioningContract(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(dirname(__DIR__).'/composer.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        $metadata = $manifest['extra']['durable-workflow'] ?? [];

        self::assertTrue($metadata['version-markers'] ?? false);
        self::assertSame('record_version_marker', $metadata['version-marker-command'] ?? null);
        self::assertSame('VersionMarkerRecorded', $metadata['version-marker-history-event'] ?? null);
        self::assertSame(['patched', 'deprecatePatch'], $metadata['version-marker-helpers'] ?? null);
    }

    public function testNewRunsRecordOneStableDecisionPerChangeId(): void
    {
        $result = $this->replay(static function (WorkflowContext $context): array {
            $selected = $context->getVersion('checkout-routing', 1, 2);
            $raisedMaximum = $context->getVersion('checkout-routing', 2, 4);
            $patched = $context->patched('invoice-search');
            $context->deprecatePatch('invoice-search');

            return compact('selected', 'raisedMaximum', 'patched');
        });

        self::assertSame([
            [
                'type' => 'record_version_marker',
                'change_id' => 'checkout-routing',
                'version' => 2,
                'min_supported' => 1,
                'max_supported' => 2,
            ],
            [
                'type' => 'record_version_marker',
                'change_id' => 'invoice-search',
                'version' => 1,
                'min_supported' => -1,
                'max_supported' => 1,
            ],
            [
                'type' => 'complete_workflow',
                'result' => $result->commands[2]['result'],
            ],
        ], $result->commands);
        self::assertSame([
            'selected' => 2,
            'raisedMaximum' => 2,
            'patched' => true,
        ], $this->codec()->decodeEnvelope($result->commands[2]['result']));
    }

    public function testRecordedDecisionSurvivesUpgradeRedeliveryRestartAndCompletedReplay(): void
    {
        $sideEffectCalls = 0;
        $history = [
            $this->marker(1, 'checkout-routing', 2, 1, 2),
            [
                'event_type' => 'SideEffectRecorded',
                'payload' => [
                    'sequence' => 2,
                    'result' => $this->codec()->envelope('committed'),
                ],
            ],
            ['event_type' => 'WorkflowCompleted', 'payload' => []],
        ];
        $workflow = static function (WorkflowContext $context) use (&$sideEffectCalls): array {
            $version = $context->getVersion('checkout-routing', 2, 5);
            $captured = $context->sideEffect(static function () use (&$sideEffectCalls): string {
                ++$sideEffectCalls;

                return 'must-not-run';
            });

            return compact('version', 'captured');
        };

        $firstDelivery = $this->replay($workflow, $history)->commands;
        $redelivery = $this->replay($workflow, $history)->commands;
        $coldWorker = (new Replayer($this->codec()))->replay(
            $workflow,
            $history,
            [],
            'php-workers',
            ['workflow_id' => 'checkout-1', 'run_id' => 'run-1'],
        )->commands;

        self::assertSame($firstDelivery, $redelivery);
        self::assertSame($firstDelivery, $coldWorker);
        self::assertSame(['complete_workflow'], array_column($firstDelivery, 'type'));
        self::assertSame(
            ['version' => 2, 'captured' => 'committed'],
            $this->codec()->decodeEnvelope($firstDelivery[0]['result']),
        );
        self::assertSame(0, $sideEffectCalls);
    }

    public function testPatchIntroducedBeforeRecordedWorkUsesTheLegacyDecisionWithoutAddingAMarker(): void
    {
        $history = [[
            'event_type' => 'ActivityCompleted',
            'payload' => [
                'sequence' => 1,
                'activity_type' => 'legacy.checkout',
                'result' => $this->codec()->envelope('legacy-result'),
            ],
        ]];

        $result = $this->replay(static function (WorkflowContext $context): array {
            $patched = $context->patched('checkout-routing');
            $activity = $context->activity('legacy.checkout');

            return compact('patched', 'activity');
        }, $history);

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(
            ['patched' => false, 'activity' => 'legacy-result'],
            $this->codec()->decodeEnvelope($result->commands[0]['result']),
        );
    }

    public function testCompletedHistoryWithoutAMarkerUsesTheLegacyDecisionOnColdReplay(): void
    {
        $result = $this->replay(
            static fn (WorkflowContext $context): int => $context->getVersion('terminal-format', -1, 3),
            [['event_type' => 'WorkflowCompleted', 'payload' => []]],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(-1, $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testLegacyHistoryFailsWhenTheCurrentRangeDropsTheLegacyDecision(): void
    {
        try {
            $this->replay(
                static fn (WorkflowContext $context): int => $context->getVersion('checkout-routing', 1, 3),
                [[
                    'event_type' => 'ActivityCompleted',
                    'payload' => [
                        'sequence' => 1,
                        'activity_type' => 'legacy.checkout',
                        'result' => $this->codec()->envelope('legacy-result'),
                    ],
                ]],
            );
            self::fail('Legacy history must be refused after its decision leaves the supported range.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('version_marker_incompatible_range', $exception->reason);
            self::assertSame(1, $exception->sequence);
            self::assertStringContainsString('checkout-routing', $exception->getMessage());
        }
    }

    public function testIndependentChangesAndWorkflowFibersDoNotShareDecisions(): void
    {
        $history = [
            $this->marker(1, 'catalog-layout', 1, 1, 1),
            $this->marker(2, 'invoice-search', 3, 2, 3),
        ];
        $replayed = $this->replay(
            static fn (WorkflowContext $context): array => [
                $context->getVersion('catalog-layout', 1, 4),
                $context->getVersion('invoice-search', 3, 5),
            ],
            $history,
        );

        self::assertSame(
            [1, 3],
            $this->codec()->decodeEnvelope($replayed->commands[0]['result']),
        );

        $replayer = new Replayer($this->codec());
        $interleaved = $replayer->replay(
            static function (WorkflowContext $context) use ($replayer): array {
                $outer = $context->getVersion('isolated-change', 1, 1);
                $nested = $replayer->replay(
                    static fn (WorkflowContext $nestedContext): int => $nestedContext->getVersion(
                        'isolated-change',
                        1,
                        4,
                    ),
                    [],
                    [],
                    'nested-workers',
                );

                return [$outer, $nested->commands[0]['version']];
            },
            [],
            [],
            'php-workers',
        );

        self::assertSame(1, $interleaved->commands[0]['version']);
        self::assertSame(
            [1, 4],
            $this->codec()->decodeEnvelope($interleaved->commands[1]['result']),
        );
    }

    #[DataProvider('invalidCallProvider')]
    public function testInvalidCallsFailWithTypedDiagnosticsBeforeLaterSideEffects(
        callable $workflow,
        string $reason,
        string $changeId,
    ): void {
        $sideEffectCalls = 0;

        try {
            $this->replay(static function (WorkflowContext $context) use ($workflow, &$sideEffectCalls): void {
                $workflow($context);
                $context->sideEffect(static function () use (&$sideEffectCalls): string {
                    ++$sideEffectCalls;

                    return 'must-not-run';
                });
            });
            self::fail('The invalid version call must fail replay.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame($reason, $exception->reason);
            self::assertStringContainsString($changeId, $exception->getMessage());
        }

        self::assertSame(0, $sideEffectCalls);
    }

    /** @return iterable<string, array{callable(WorkflowContext): void, string, string}> */
    public static function invalidCallProvider(): iterable
    {
        yield 'empty change ID' => [
            static fn (WorkflowContext $context) => $context->getVersion('', 1, 2),
            'version_change_id_invalid',
            'change ID',
        ];
        yield 'blank change ID' => [
            static fn (WorkflowContext $context) => $context->patched('   '),
            'version_change_id_invalid',
            'change ID',
        ];
        yield 'reversed range' => [
            static fn (WorkflowContext $context) => $context->getVersion('checkout-routing', 3, 2),
            'version_range_invalid',
            'checkout-routing',
        ];
        yield 'range outside protocol integer' => [
            static fn (WorkflowContext $context) => $context->getVersion(
                'checkout-routing',
                -2_147_483_649,
                2,
            ),
            'version_range_invalid',
            'checkout-routing',
        ];
    }

    public function testDuplicateCallsRejectIncompatibleBoundsAndHelperSwitches(): void
    {
        $workflows = [
            'incompatible bounds' => [
                static function (WorkflowContext $context): void {
                    $context->getVersion('checkout-routing', 1, 2);
                    $context->getVersion('checkout-routing', 3, 4);
                },
                'version_marker_incompatible_range',
            ],
            'helper family switch' => [
                static function (WorkflowContext $context): void {
                    $context->patched('checkout-routing');
                    $context->getVersion('checkout-routing', -1, 1);
                },
                'version_marker_kind_mismatch',
            ],
        ];

        foreach ($workflows as $name => [$workflow, $reason]) {
            $sideEffectCalls = 0;
            $guardedWorkflow = static function (WorkflowContext $context) use (
                $workflow,
                &$sideEffectCalls,
            ): void {
                $workflow($context);
                $context->sideEffect(static function () use (&$sideEffectCalls): string {
                    ++$sideEffectCalls;

                    return 'must-not-run';
                });
            };
            try {
                $this->replay($guardedWorkflow);
                self::fail("{$name} must fail replay.");
            } catch (NonDeterministicWorkflow $exception) {
                self::assertSame($reason, $exception->reason, $name);
                self::assertStringContainsString('checkout-routing', $exception->getMessage(), $name);
            }
            self::assertSame(0, $sideEffectCalls, $name);
        }
    }

    public function testReplayRejectsARecordedVersionOutsideTheCurrentRangeBeforeSideEffects(): void
    {
        $sideEffectCalls = 0;
        $workflow = static function (WorkflowContext $context) use (&$sideEffectCalls): void {
            $context->getVersion('checkout-routing', 2, 4);
            $context->sideEffect(static function () use (&$sideEffectCalls): string {
                ++$sideEffectCalls;

                return 'must-not-run';
            });
        };

        try {
            $this->replay($workflow, [$this->marker(1, 'checkout-routing', 1, 1, 2)]);
            self::fail('The unsupported recorded version must fail replay.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('version_marker_incompatible_range', $exception->reason);
            self::assertSame(1, $exception->sequence);
            self::assertStringContainsString('checkout-routing', $exception->getMessage());
        }

        self::assertSame(0, $sideEffectCalls);
    }

    #[DataProvider('malformedHistoryProvider')]
    public function testMalformedOrDuplicateMarkersFailBeforeWorkflowCode(
        array $history,
        string $reason,
    ): void {
        $workflowStarted = false;

        try {
            $this->replay(
                static function (WorkflowContext $context) use (&$workflowStarted): int {
                    $workflowStarted = true;

                    return $context->getVersion('checkout-routing', 1, 4);
                },
                $history,
            );
            self::fail('Malformed version-marker history must fail replay.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame($reason, $exception->reason);
        }

        self::assertFalse($workflowStarted);
    }

    /** @return iterable<string, array{list<array<string, mixed>>, string}> */
    public static function malformedHistoryProvider(): iterable
    {
        $marker = static fn (array $payload): array => [
            'event_type' => 'VersionMarkerRecorded',
            'payload' => $payload,
        ];
        $valid = [
            'sequence' => 1,
            'change_id' => 'checkout-routing',
            'version' => 2,
            'min_supported' => 1,
            'max_supported' => 2,
        ];

        yield 'missing sequence' => [[$marker(array_diff_key($valid, ['sequence' => true]))], 'version_marker_sequence_invalid'];
        yield 'empty change ID' => [[$marker(array_merge($valid, ['change_id' => '']))], 'version_marker_field_missing'];
        yield 'non-integer version' => [[$marker(array_merge($valid, ['version' => '2']))], 'version_marker_field_missing'];
        yield 'missing minimum' => [[$marker(array_diff_key($valid, ['min_supported' => true]))], 'version_marker_field_missing'];
        yield 'reversed recorded range' => [[
            $marker(array_merge($valid, ['min_supported' => 3])),
        ], 'version_marker_history_range_invalid'];
        yield 'version outside recorded range' => [[
            $marker(array_merge($valid, ['version' => 4])),
        ], 'version_marker_history_range_invalid'];
        yield 'two marker events at one sequence' => [[
            $marker($valid),
            $marker(array_merge($valid, ['change_id' => 'other-change'])),
        ], 'duplicate_version_marker_record'];
        yield 'one change ID at two sequences' => [[
            $marker($valid),
            $marker(array_merge($valid, ['sequence' => 2])),
        ], 'duplicate_version_marker'];
        yield 'marker collides with another durable command' => [[
            $marker($valid),
            ['event_type' => 'SideEffectRecorded', 'payload' => ['sequence' => 1]],
        ], 'durable_command_sequence_collision'];
    }

    /**
     * @param callable(WorkflowContext): mixed $workflow
     * @param list<array<string, mixed>> $history
     */
    private function replay(callable $workflow, array $history = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer($this->codec()))->replay(
            $workflow,
            $history,
            [],
            'php-workers',
            ['workflow_id' => 'checkout-1', 'run_id' => 'run-1'],
        );
    }

    /** @return array{event_type: string, payload: array<string, mixed>} */
    private function marker(
        int $sequence,
        string $changeId,
        int $version,
        int $minSupported,
        int $maxSupported,
    ): array {
        return [
            'event_type' => 'VersionMarkerRecorded',
            'payload' => [
                'sequence' => $sequence,
                'change_id' => $changeId,
                'version' => $version,
                'min_supported' => $minSupported,
                'max_supported' => $maxSupported,
            ],
        ];
    }

    private function codec(): AvroPayloadCodec
    {
        return new AvroPayloadCodec();
    }
}
