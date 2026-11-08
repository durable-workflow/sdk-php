<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\ChildLifecycleTransport;
use DurableWorkflow\Transport\BoundedTransport;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/Support/ChildLifecycleTransport.php';

/**
 * #185: the lifecycle fixture's stop-after-start worker must not carry a
 * workflow poll on its committed-start completion, or a carried-poll Server
 * leases the fresh parent turn to that exiting worker.
 */
final class ChildLifecycleTransportTest extends TestCase
{
    private const START = 'http://127.0.0.1:8080/api/worker/workflow-tasks/01TASK/complete';

    private string $receipt;

    protected function setUp(): void
    {
        $this->receipt = (string) tempnam(sys_get_temp_dir(), 'child-lifecycle-transport-');
        unlink($this->receipt);
    }

    protected function tearDown(): void
    {
        @unlink($this->receipt);
    }

    public function testAStopAfterStartWorkerSendsAndRecordsTheStartCompletionWithoutACarriedWorkflowPoll(): void
    {
        $inner = new RecordingInnerTransport(['recorded' => true]);
        $started = 0;
        $transport = new ChildLifecycleTransport($this->receipt, static function () use (&$started): void { ++$started; }, true, $inner);

        $transport->sendBounded('POST', self::START, [], self::startBody(), 5);

        self::assertArrayNotHasKey('workflow_poll', $inner->bodies[0]);
        self::assertSame(self::startBody()['commands'], $inner->bodies[0]['commands']);
        $receipt = json_decode((string) file_get_contents($this->receipt), true, flags: JSON_THROW_ON_ERROR);
        self::assertSame('01TASK', $receipt['task_id']);
        self::assertSame($inner->bodies[0], $receipt['body'], 'the receipt is the body actually sent');
        self::assertSame(1, $started);
    }

    /** The carried-lease control and the predecessor fixture keep the carrier. */
    public function testKeepingTheCarrierSendsTheWorkflowPollUnchanged(): void
    {
        $inner = new RecordingInnerTransport(['recorded' => true, 'workflow_poll' => ['leased' => true]]);
        $transport = new ChildLifecycleTransport($this->receipt, static function (): void {}, false, $inner);

        $transport->send('POST', self::START, [], self::startBody());

        self::assertSame(self::startBody(), $inner->bodies[0]);
        $receipt = json_decode((string) file_get_contents($this->receipt), true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(['leased' => true], $receipt['response']['workflow_poll']);
    }

    public function testOtherRequestsAreSentUnchangedAndNotRecorded(): void
    {
        $inner = new RecordingInnerTransport(['recorded' => true]);
        $transport = new ChildLifecycleTransport($this->receipt, static function (): void {}, true, $inner);
        $later = self::startBody();
        $later['commands'][0]['await'] = 'completed';

        $transport->send('POST', self::START, [], $later);
        $transport->send('POST', 'http://127.0.0.1:8080/api/worker/activity-tasks/01A/complete', [], self::startBody());

        self::assertSame([$later, self::startBody()], $inner->bodies);
        self::assertFileDoesNotExist($this->receipt);
    }

    /**
     * Causal model of the cell 02 failure on a carried-poll Server: the
     * predecessor fixture (carrier kept) lets the exiting worker take the fresh
     * parent turn, so the replacement's delivery-control poll finds nothing; the
     * candidate (carrier withheld) leaves the turn for the replacement.
     */
    public function testWithholdingTheCarrierLeavesTheFreshParentTurnForTheReplacement(): void
    {
        foreach ([false => null, true => 'start-turn'] as $withhold => $expected) {
            $native = new CarriedPollNativeModel();
            $transport = new ChildLifecycleTransport($this->receipt, static function (): void {}, (bool) $withhold, $native);
            $transport->sendBounded('POST', self::START, [], self::startBody(), 5);
            self::assertSame($expected, $native->deliveryControlPoll(), $withhold ? 'candidate' : 'predecessor');
            @unlink($this->receipt);
        }
    }

    /** @return array<string, mixed> */
    private static function startBody(): array
    {
        return [
            'lease_owner' => 'parent-original', 'workflow_task_attempt' => 1,
            'commands' => [['type' => 'start_child_workflow', 'await' => 'started', 'workflow_type' => 'qualification.lifecycle.child']],
            'workflow_poll' => ['worker_id' => 'parent-original', 'task_queue' => 'parent', 'poll_request_id' => 'carried-1', 'timeout_seconds' => 0],
        ];
    }
}

final class RecordingInnerTransport implements BoundedTransport
{
    /** @var list<array<string, mixed>|null> */
    public array $bodies = [];

    /** @param array<string, mixed> $response */
    public function __construct(private readonly array $response)
    {
    }

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->bodies[] = $body;

        return $this->response;
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }
}

/** The committed start creates a ready parent turn; a carried workflow poll on the same completion leases it. */
final class CarriedPollNativeModel implements BoundedTransport
{
    private ?string $readyTurn = null;

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->readyTurn = 'start-turn';
        if (isset($body['workflow_poll'])) {
            $this->readyTurn = null;

            return ['recorded' => true, 'workflow_poll' => ['leased' => true]];
        }

        return ['recorded' => true];
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }

    public function deliveryControlPoll(): ?string
    {
        return $this->readyTurn;
    }
}
