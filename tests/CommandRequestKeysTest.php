<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\SignalFailed;
use DurableWorkflow\Exception\UpdateFailed;
use DurableWorkflow\Model\ClusterInfo;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

/**
 * A signal or update `request_id` as a durable command key (native Server 3.9.0, server#934),
 * over the real PSR-18 transport against a mocked HTTP Server.
 */
final class CommandRequestKeysTest extends TestCase
{
    public function testClusterInfoReportsTheCapability(): void
    {
        self::assertSame('command_request_keys', ClusterInfo::COMMAND_REQUEST_KEYS);
        self::assertTrue(ClusterInfo::fromArray(['capabilities' => ['command_request_keys' => true]])
            ->supportsCommandRequestKeys());
        self::assertFalse(ClusterInfo::fromArray(['capabilities' => ['command_request_keys' => 'true']])
            ->supportsCommandRequestKeys());
        self::assertFalse(ClusterInfo::fromArray(['capabilities' => []])->supportsCommandRequestKeys());
        self::assertFalse(ClusterInfo::fromArray([])->supportsCommandRequestKeys());
    }

    public function testASignalRetriedWithChangedArgumentsIsATypedConflictSentOnce(): void
    {
        [$client, $requests] = $this->server(native: true);

        $first = $client->signalWorkflow('order-1', 'approve', ['Ada'], requestId: 'approve-1');
        try {
            $client->signalWorkflow('order-1', 'approve', ['Grace'], requestId: 'approve-1');
            self::fail('A changed signal under a retained key must be refused.');
        } catch (SignalFailed $exception) {
            self::assertSame(409, $exception->status);
            self::assertTrue($exception->isRequestIdConflict());
            self::assertSame('request_id_conflict', $exception->reason());
            self::assertSame('approve-1', $exception->requestId());
            self::assertSame(['arguments'], $exception->conflict());
            self::assertSame($first['command_id'], $exception->retainedCommandId());
            self::assertSame('rejected_request_id_conflict', $exception->details()['outcome']);
            self::assertFalse($exception->isTransientConnectionFailure());
            self::assertFalse($exception->isTransientUpstreamFailure());
        }

        // One request per call: the 409 is never resent automatically.
        self::assertCount(2, $requests->value);
    }

    public function testAnUpdateRetriedWithAnotherRunTargetIsATypedConflictSentOnce(): void
    {
        [$client, $requests] = $this->server(native: true);

        $first = $client->updateWorkflowReceipt('order-1', 'rename', ['Ada'], 'accepted', requestId: 'rename-1');
        try {
            $client->updateWorkflowReceipt('order-1', 'rename', ['Ada'], 'accepted', requestId: 'rename-1', runId: 'run-1');
            self::fail('A retargeted update under a retained key must be refused.');
        } catch (UpdateFailed $exception) {
            self::assertTrue($exception->isRequestIdConflict());
            self::assertSame('rename-1', $exception->requestId());
            self::assertSame(['target_or_arguments'], $exception->conflict());
            self::assertSame($first['command_id'], $exception->retainedCommandId());
        }

        self::assertCount(2, $requests->value);
    }

    public function testAnIdenticalRetryReturnsTheOriginalReceipt(): void
    {
        [$client, $requests] = $this->server(native: true);

        $signal = $client->signalWorkflow('order-1', 'approve', ['Ada'], requestId: 'approve-1');
        $retry = $client->signalWorkflow('order-1', 'approve', ['Ada'], requestId: 'approve-1');
        self::assertSame($signal['command_id'], $retry['command_id']);
        self::assertTrue($retry['retained']);
        self::assertTrue($retry['intent_verified']);
        self::assertSame('approve-1', $retry['request_id']);

        $update = $client->updateWorkflowReceipt('order-1', 'rename', ['Ada'], 'accepted', requestId: 'rename-1');
        $again = $client->updateWorkflowReceipt('order-1', 'rename', ['Ada'], 'accepted', requestId: 'rename-1');
        self::assertSame($update['update_id'], $again['update_id']);
        self::assertSame('rename-1', $again['request_id']);

        $bodies = array_map(static fn (RequestInterface $request): array
            => json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR), $requests->value);
        self::assertSame($bodies[0], $bodies[1]);
        self::assertSame($bodies[2], $bodies[3]);
    }

    public function testAnOlderServerWithoutTheCapabilityIsUnchanged(): void
    {
        [$client, $requests] = $this->server(native: false);

        self::assertFalse($client->clusterInfo()->supportsCommandRequestKeys());
        // Before 3.9.0 a repeated signal key recorded a second signal, and an update retried
        // with changed arguments returned the original receipt. Both come back as answered.
        $first = $client->signalWorkflow('order-1', 'approve', ['Ada'], requestId: 'approve-1');
        $second = $client->signalWorkflow('order-1', 'approve', ['Grace'], requestId: 'approve-1');
        self::assertNotSame($first['command_id'], $second['command_id']);
        self::assertArrayNotHasKey('retained', $second);

        $update = $client->updateWorkflowReceipt('order-1', 'rename', ['Ada'], 'accepted', requestId: 'rename-1');
        $changed = $client->updateWorkflowReceipt('order-1', 'rename', ['Grace'], 'accepted', requestId: 'rename-1');
        self::assertSame($update['update_id'], $changed['update_id']);
        self::assertCount(4, $requests->value);
    }

    /**
     * A Server that keys signals and updates by request_id like native 3.9.0, or like an older
     * Server when `$native` is false. `$requests` records the signal and update requests.
     *
     * @return array{Client, object{value: list<RequestInterface>}}
     */
    private function server(bool $native): array
    {
        $requests = new class { /** @var list<RequestInterface> */ public array $value = []; };
        $retained = [];
        $sequence = 0;
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use ($native, $requests, &$retained, &$sequence) {
            $path = $request->getUri()->getPath();
            if ($path === '/api/cluster/info') {
                $capabilities = $native ? ['command_request_keys' => true] : [];

                return Create::promiseFor(new Response(200, [], json_encode(['capabilities' => $capabilities], JSON_THROW_ON_ERROR)));
            }
            self::assertSame(1, preg_match('~^/api/workflows/order-1(?:/runs/([^/]+))?/(signal|update)/([^/]+)$~', $path, $match));
            [, $run, $type, $name] = $match;
            $requests->value[] = $request;
            $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
            $key = $type.':'.$body['request_id'];
            $intent = [$name, $body['input'], $run];
            $original = $retained[$key] ?? null;

            if ($original !== null && $native && $original['intent'] !== $intent) {
                return Create::promiseFor(new Response(409, [], json_encode([
                    'workflow_id' => 'order-1',
                    'reason' => 'request_id_conflict',
                    'outcome' => 'rejected_request_id_conflict',
                    'command_status' => 'rejected',
                    'rejection_reason' => 'request_id_conflict',
                    'message' => "The request_id was already used for a different {$type} command on workflow [order-1]. Use a new request_id for a different command.",
                    'intent_verified' => true,
                    'retained' => true,
                    'retained_command_id' => $original['receipt']['command_id'],
                    'retained_outcome' => $original['receipt']['outcome'],
                    'conflict' => [$original['intent'][0] !== $name ? 'name' : ($run !== '' ? 'target_or_arguments' : 'arguments')],
                ], JSON_THROW_ON_ERROR)));
            }
            if ($original !== null && ($native || $type === 'update')) {
                $receipt = $original['receipt'] + ($type === 'signal' ? ['retained' => true, 'intent_verified' => true] : []);

                return Create::promiseFor(new Response(200, [], json_encode($receipt, JSON_THROW_ON_ERROR)));
            }

            ++$sequence;
            $receipt = ['workflow_id' => 'order-1', 'command_id' => 'cmd-'.$sequence, 'command_status' => 'accepted',
                'outcome' => $type === 'signal' ? 'signal_received' : 'update_accepted']
                + ($type === 'update' ? ['update_id' => 'upd-'.$sequence, 'update_status' => 'accepted'] : []);
            $retained[$key] ??= ['intent' => $intent, 'receipt' => $receipt];

            return Create::promiseFor(new Response(202, [], json_encode($receipt, JSON_THROW_ON_ERROR)));
        }]);

        return [new Client('https://runtime.test', transport: new Psr18Transport($http), controlToken: 'fixture-client'), $requests];
    }
}
