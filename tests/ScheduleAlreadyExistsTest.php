<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ScheduleAlreadyExists;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Model\ScheduleAction;
use DurableWorkflow\Model\ScheduleSpec;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ScheduleAlreadyExistsTest extends TestCase
{
    /** @param array<string, mixed> $body */
    private static function createWith(array $body, int $status = 409): void
    {
        $transport = new FakeTransport([
            TransportException::fromResponse($status, $body, json_encode($body, JSON_THROW_ON_ERROR)),
        ]);
        $client = new Client('https://server.example', transport: $transport);
        $client->createSchedule(
            new ScheduleSpec(cronExpressions: ['0 0 * * *']),
            new ScheduleAction('reports.nightly', 'reports'),
            scheduleId: 'nightly',
        );
    }

    public function testTakenScheduleIdRaisesTheTypedExceptionAndKeepsTheServerAnswer(): void
    {
        // Recorded native answer for a create with the id of a deleted schedule (workflow#876).
        $body = [
            'message' => 'A schedule with this id already exists.',
            'reason' => 'schedule_already_exists',
            'schedule_id' => 'nightly',
        ];

        try {
            self::createWith($body);
            self::fail('createSchedule should refuse a taken schedule id.');
        } catch (ScheduleAlreadyExists $error) {
            self::assertInstanceOf(ServerException::class, $error);
            self::assertSame('nightly', $error->scheduleId);
            self::assertSame(409, $error->status);
            self::assertSame('schedule_already_exists', $error->reason);
            self::assertSame('nightly', $error->details['schedule_id'] ?? null);
            self::assertInstanceOf(ServerException::class, $error->getPrevious());
        }
    }

    public function testRequestedIdIsUsedWhenTheAnswerOmitsIt(): void
    {
        try {
            self::createWith(['message' => 'Taken.', 'reason' => 'schedule_already_exists']);
            self::fail('createSchedule should refuse a taken schedule id.');
        } catch (ScheduleAlreadyExists $error) {
            self::assertSame('nightly', $error->scheduleId);
        }
    }

    public function testOtherConflictsStayPlainServerExceptions(): void
    {
        try {
            self::createWith(['message' => 'Conflict.', 'reason' => 'namespace_draining']);
            self::fail('createSchedule should surface the conflict.');
        } catch (ServerException $error) {
            self::assertNotInstanceOf(ScheduleAlreadyExists::class, $error);
            self::assertSame(409, $error->status);
            self::assertSame('namespace_draining', $error->reason);
        }
    }
}
