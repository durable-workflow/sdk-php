<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\SagaCompensationFailed;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class SagaTest extends TestCase
{
    public function testCompensationsRunInDeterministicReverseRegistrationOrder(): void
    {
        $history = [
            $this->completed(1, 'trip.reserve-flight', 'flight-1'),
            $this->completed(2, 'trip.reserve-hotel', 'hotel-1'),
            $this->failed(3, 'trip.charge', 'card declined'),
        ];

        $firstCompensation = $this->replay(self::tripSaga(), $history)->commands;
        self::assertSame('trip.cancel-hotel', $firstCompensation[0]['activity_type'] ?? null);
        self::assertSame(['hotel-1'], $this->arguments($firstCompensation[0]));

        $secondCompensation = $this->replay(self::tripSaga(), [
            ...$history,
            $this->completed(4, 'trip.cancel-hotel'),
        ])->commands;
        self::assertSame('trip.cancel-flight', $secondCompensation[0]['activity_type'] ?? null);
        self::assertSame(['flight-1'], $this->arguments($secondCompensation[0]));
    }

    public function testColdWorkerRestartResumesAfterTheCompletedCompensation(): void
    {
        $history = [
            $this->completed(1, 'trip.reserve-flight', 'flight-1'),
            $this->completed(2, 'trip.reserve-hotel', 'hotel-1'),
            $this->failed(3, 'trip.charge', 'card declined'),
            $this->completed(4, 'trip.cancel-hotel'),
        ];

        $firstWorker = $this->replay(self::tripSaga(), $history)->commands;
        $restartedWorker = (new Replayer($this->codec()))->replay(
            self::tripSaga(),
            $history,
            [],
            'trip-workers',
        )->commands;

        self::assertSame($firstWorker, $restartedWorker);
        self::assertCount(1, $restartedWorker);
        self::assertSame('trip.cancel-flight', $restartedWorker[0]['activity_type'] ?? null);
    }

    public function testDuplicateTerminalDeliveryDoesNotRepeatACompletedCompensation(): void
    {
        $history = [
            $this->completed(1, 'trip.reserve-flight', 'flight-1'),
            $this->completed(2, 'trip.reserve-hotel', 'hotel-1'),
            $this->failed(3, 'trip.charge', 'card declined'),
            $this->completed(4, 'trip.cancel-hotel'),
            $this->completed(4, 'trip.cancel-hotel', 'duplicate-delivery'),
        ];

        $firstDelivery = $this->replay(self::tripSaga(), $history)->commands;
        $redelivery = $this->replay(self::tripSaga(), $history)->commands;

        self::assertSame($firstDelivery, $redelivery);
        self::assertSame(['trip.cancel-flight'], array_column($redelivery, 'activity_type'));
    }

    public function testOriginalActivityFailurePropagatesAfterSuccessfulCompensation(): void
    {
        try {
            $this->replay(self::tripSaga(), [
                $this->completed(1, 'trip.reserve-flight', 'flight-1'),
                $this->completed(2, 'trip.reserve-hotel', 'hotel-1'),
                $this->failed(3, 'trip.charge', 'card declined'),
                $this->completed(4, 'trip.cancel-hotel'),
                $this->completed(5, 'trip.cancel-flight'),
            ]);
            self::fail('A successfully compensated saga must preserve its forward failure.');
        } catch (ActivityFailed $failure) {
            self::assertSame('trip.charge', $failure->activityType);
            self::assertSame('CardDeclined', $failure->failureType);
            self::assertSame('card declined', $failure->getMessage());
        }
    }

    public function testCompensationFailureIdentifiesBothActivitySteps(): void
    {
        try {
            $this->replay(self::tripSaga(), [
                $this->completed(1, 'trip.reserve-flight', 'flight-1'),
                $this->completed(2, 'trip.reserve-hotel', 'hotel-1'),
                $this->failed(3, 'trip.charge', 'card declined'),
                $this->failed(4, 'trip.cancel-hotel', 'hotel cancellation rejected', 'CancellationRejected'),
            ]);
            self::fail('A terminal compensation failure must fail the saga.');
        } catch (SagaCompensationFailed $failure) {
            self::assertSame('trip.charge', $failure->forwardStep);
            self::assertInstanceOf(ActivityFailed::class, $failure->forwardFailure);
            self::assertSame('trip.charge', $failure->forwardFailure->activityType);
            self::assertSame('trip.cancel-hotel', $failure->compensationActivityType);
            self::assertSame(2, $failure->compensationRegistrationOrder);
            self::assertSame('trip.cancel-hotel', $failure->compensationFailure->activityType);
            self::assertSame('CancellationRejected', $failure->compensationFailure->failureType);
            self::assertSame([
                'forward_step' => 'trip.charge',
                'forward_exception_type' => ActivityFailed::class,
                'forward_message' => 'card declined',
                'compensation_activity_type' => 'trip.cancel-hotel',
                'compensation_registration_order' => 2,
                'compensation_exception_type' => 'CancellationRejected',
                'compensation_message' => 'hotel cancellation rejected',
            ], $failure->diagnosticContext());
        }
    }

    public function testCancellationCompensatesCompletedForwardStepsThenPropagates(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->saga()->run(static function (Saga $saga) use ($context): string {
                $reservation = $context->activity('trip.reserve-flight');
                $saga->addCompensation('trip.cancel-flight', [$reservation]);
                $context->throwIfCancellationRequested();

                return 'booked';
            });
        };
        $history = [$this->completed(1, 'trip.reserve-flight', 'flight-1')];

        $commands = $this->replay($workflow, $history, ['cancel_requested' => true])->commands;
        self::assertSame('trip.cancel-flight', $commands[0]['activity_type'] ?? null);

        try {
            $this->replay($workflow, [
                ...$history,
                $this->completed(2, 'trip.cancel-flight'),
            ], ['cancel_requested' => true]);
            self::fail('Workflow cancellation must remain the terminal saga outcome.');
        } catch (WorkflowCancelled $failure) {
            self::assertSame('Workflow cancellation was requested.', $failure->getMessage());
        }
    }

    public function testNestedSagaFailureCompensatesInnerThenOuterScopes(): void
    {
        $workflow = static function (WorkflowContext $context): mixed {
            return $context->saga()->run(static function (Saga $outer) use ($context): mixed {
                $outerId = $context->activity('trip.reserve-outer');
                $outer->addCompensation('trip.cancel-outer', [$outerId]);

                return $context->saga()->run(static function (Saga $inner) use ($context): mixed {
                    $innerId = $context->activity('trip.reserve-inner');
                    $inner->addCompensation('trip.cancel-inner', [$innerId]);

                    return $context->activity('trip.fail-inner');
                });
            });
        };
        $history = [
            $this->completed(1, 'trip.reserve-outer', 'outer-1'),
            $this->completed(2, 'trip.reserve-inner', 'inner-1'),
            $this->failed(3, 'trip.fail-inner', 'inner failed'),
        ];

        $inner = $this->replay($workflow, $history)->commands;
        self::assertSame('trip.cancel-inner', $inner[0]['activity_type'] ?? null);

        $outer = $this->replay($workflow, [
            ...$history,
            $this->completed(4, 'trip.cancel-inner'),
        ])->commands;
        self::assertSame('trip.cancel-outer', $outer[0]['activity_type'] ?? null);
    }

    public function testExplicitCompensationCanReturnAnEmbeddedStyleCompletedResult(): void
    {
        $workflow = static function (WorkflowContext $context): array {
            $saga = $context->saga();
            try {
                $reservation = $context->activity('trip.reserve-flight');
                $saga->addCompensation('trip.cancel-flight', [$reservation]);
                $context->activity('trip.charge');

                return ['status' => 'booked'];
            } catch (ActivityFailed $failure) {
                $saga->compensate($failure);

                return ['status' => 'compensated', 'failed_forward_step' => $failure->activityType];
            }
        };
        $result = $this->replay($workflow, [
            $this->completed(1, 'trip.reserve-flight', 'flight-1'),
            $this->failed(2, 'trip.charge', 'card declined'),
            $this->completed(3, 'trip.cancel-flight'),
        ])->commands;

        self::assertSame('complete_workflow', $result[0]['type'] ?? null);
        self::assertSame([
            'status' => 'compensated',
            'failed_forward_step' => 'trip.charge',
        ], $this->codec()->decodeEnvelope($result[0]['result'] ?? []));
    }

    /** @return callable(WorkflowContext): mixed */
    private static function tripSaga(): callable
    {
        return static function (WorkflowContext $context): mixed {
            return $context->saga()->run(static function (Saga $saga) use ($context): string {
                $flight = $context->activity('trip.reserve-flight');
                $saga->addCompensation('trip.cancel-flight', [$flight]);

                $hotel = $context->activity('trip.reserve-hotel');
                $saga->addCompensation('trip.cancel-hotel', [$hotel]);

                $context->activity('trip.charge');

                return 'booked';
            });
        };
    }

    /**
     * @param callable(WorkflowContext): mixed $workflow
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    private function replay(callable $workflow, array $history, array $task = []): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer($this->codec()))->replay($workflow, $history, [], 'trip-workers', $task);
    }

    /** @return array<string, mixed> */
    private function completed(int $sequence, string $activityType, mixed $result = null): array
    {
        return [
            'event_type' => 'ActivityCompleted',
            'payload' => [
                'sequence' => $sequence,
                'activity_type' => $activityType,
                'result' => $this->codec()->envelope($result),
            ],
        ];
    }

    /** @return array<string, mixed> */
    private function failed(
        int $sequence,
        string $activityType,
        string $message,
        string $failureType = 'CardDeclined',
    ): array {
        return [
            'event_type' => 'ActivityFailed',
            'payload' => [
                'sequence' => $sequence,
                'activity_type' => $activityType,
                'message' => $message,
                'exception_type' => $failureType,
                'non_retryable' => true,
            ],
        ];
    }

    /** @param array<string, mixed> $command */
    private function arguments(array $command): mixed
    {
        return $this->codec()->decodeEnvelope($command['arguments'] ?? []);
    }

    private function codec(): AvroPayloadCodec
    {
        return new AvroPayloadCodec();
    }
}
