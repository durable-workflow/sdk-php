<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollOppositeSlots;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollOppositeSlots.php';

final class CompletionPollConsumerOppositeSlotsTest extends TestCase
{
    public function testAValidCompletionWithOmittedOppositeCarrierIsTheRequiredOutcome(): void
    {
        CompletionPollOppositeSlots::omitted(['recorded' => true], 'workflow_poll');
        $this->addToAssertionCount(1);
    }

    #[DataProvider('notDeclined')]
    public function testAnEmptyClaimOrInvalidParentIsNotAnOppositeSlotDecline(array $answer): void
    {
        $this->expectExceptionMessage('completion_opposite_carrier_not_declined');
        CompletionPollOppositeSlots::omitted($answer, 'workflow_poll');
    }

    public static function notDeclined(): iterable
    {
        yield 'empty probe' => [['recorded' => true, 'workflow_poll' => ['leased' => false]]];
        yield 'extra lease' => [['recorded' => true, 'workflow_poll' => ['leased' => true]]];
        yield 'parent not recorded' => [['recorded' => false]];
    }
}
