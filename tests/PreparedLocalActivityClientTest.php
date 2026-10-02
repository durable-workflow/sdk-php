<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityClientTest extends TestCase
{
    public function test_operations_use_original_worker_authority_and_encode_identifiers(): void
    {
        $transport = new FakeTransport([['active' => false]]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'tenant',
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20');
        self::assertSame(['active' => false], $client->preparedLocalActivityOperation(
            'task/one', 'original-owner', 4, 'control', ['renew_lease' => false], 'backend/attempt',
        ));
        $request = $transport->requests[0];
        self::assertSame('https://server.example/api/worker/workflow-tasks/task%2Fone/local-activities/backend%2Fattempt/control', $request['uri']);
        self::assertSame(['lease_owner' => 'original-owner', 'workflow_task_attempt' => 4, 'renew_lease' => false], $request['body']);
        self::assertSame('Bearer worker', $request['headers']['Authorization']);
        self::assertSame('tenant', $request['headers']['X-Namespace']);
        self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
    }

    public function test_default_protocol_refuses_candidate_operations_before_sending(): void
    {
        $transport = new FakeTransport([]);
        try {
            (new Client('https://server.example', transport: $transport))->preparedLocalActivityOperation('task', 'owner', 1, 'prepare');
            self::fail('Default protocol must refuse candidate admission.');
        } catch (LogicException $error) {
            self::assertStringContainsString('1.20', $error->getMessage());
        }
        self::assertSame([], $transport->requests);
    }

    public function test_operation_bodies_cannot_replace_issued_claim_authority(): void
    {
        $transport = new FakeTransport([]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        foreach ([['lease_owner' => 'replacement'], ['workflow_task_attempt' => 2]] as $body) {
            try {
                $client->preparedLocalActivityOperation('task', 'original', 1, 'prepare', $body);
                self::fail('Operation body replaced original authority.');
            } catch (InvalidArgumentException) {
                self::assertSame([], $transport->requests);
            }
        }
    }
}
